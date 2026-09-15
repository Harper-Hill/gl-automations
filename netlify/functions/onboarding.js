'use strict';
const https = require('https');
const { createSign } = require('crypto');

const SHEET_ID = process.env.GL_SHEET_ID;
const SHEET_NAME = 'Onboarding';

// The ID of the "Staff" folder itself (Harper Hill Grounds Ltd > Staff),
// under which "Employees/<name>" gets created. Set this as a Netlify env
// var once Drive access is granted — until then, every Drive-touching
// step below no-ops quietly so starter add/tick/archive/remove keeps
// working on the sheet alone.
const STAFF_DRIVE_ROOT_ID = process.env.GL_STAFF_DRIVE_ID;

// Manual manager lookup: the "Manager" name typed on the Starters form,
// lower-cased, → their real email. Add a line here whenever someone new
// becomes a manager. Anyone in MANAGEMENT_EMAILS gets access to EVERY
// employee's folder on top of whatever Shared Drive membership already
// gives them — currently just Peter.
const MANAGER_EMAILS = {
  'peter barusevicus': 'info@harperhill.co.uk',
  'peter': 'info@harperhill.co.uk'
};
const MANAGEMENT_EMAILS = ['info@harperhill.co.uk'];

// Column letters for Onboarding:
// A=ID, B=Created, C=Name, D=Role, E=StartDate, F=Manager, G=Depot,
// H=Archived, I=ChecksJson, J=Email, K=DriveFolderId, L=DriveFolderUrl,
// M=EvidenceJson, N=NextOfKinJson, O=NextOfKinFileId,
// P=HmrcStatus, Q=HmrcFileId, R=BankStatus, S=BankFileId
const HEADERS = [
  'id', 'created', 'name', 'role', 'startDate', 'manager', 'depot', 'archived', 'checksJson',
  'email', 'driveFolderId', 'driveFolderUrl', 'evidenceJson', 'nextOfKinJson', 'nextOfKinFileId',
  'hmrcStatus', 'hmrcFileId', 'bankStatus', 'bankFileId'
];
const RANGE_ALL = SHEET_NAME + '!A2:S';
const RANGE_ROW = (i) => SHEET_NAME + '!A' + i + ':S' + i;

// ── SA + GOOGLE TOKEN ──────────────────────────────────────────
// One JWT covering both scopes this function needs — Sheets for the
// checklist data, Drive for the per-employee folder/evidence/forms.

async function fetchSA() {
  const { getStore } = require('@netlify/blobs');
  const store = getStore({
    name: 'service-account',
    siteID: process.env.NETLIFY_SITE_ID,
    token: process.env.NETLIFY_ACCESS_TOKEN
  });
  const raw = await store.get('sa_json');
  if (!raw) throw new Error('SA JSON not found in Netlify Blobs');
  return JSON.parse(raw);
}

function req(o, b) {
  return new Promise((resolve, reject) => {
    const r = https.request(o, rr => {
      let d = '';
      rr.on('data', c => d += c);
      rr.on('end', () => {
        try { resolve({ status: rr.statusCode, body: JSON.parse(d) }); }
        catch (e) { resolve({ status: rr.statusCode, body: d }); }
      });
    });
    r.on('error', reject);
    if (b) r.write(typeof b === 'string' ? b : JSON.stringify(b));
    r.end();
  });
}

// Separate from req() above because uploads send a raw Buffer body (a
// multipart/related payload, or raw file bytes) rather than JSON.
function reqRaw(o, buf) {
  return new Promise((resolve, reject) => {
    const r = https.request(o, rr => {
      const chunks = [];
      rr.on('data', c => chunks.push(c));
      rr.on('end', () => {
        const d = Buffer.concat(chunks).toString('utf8');
        try { resolve({ status: rr.statusCode, body: JSON.parse(d) }); }
        catch (e) { resolve({ status: rr.statusCode, body: d }); }
      });
    });
    r.on('error', reject);
    if (buf) r.write(buf);
    r.end();
  });
}

function b64u(s) {
  return Buffer.from(s).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

async function gToken(sa) {
  const now = Math.floor(Date.now() / 1000);
  const claim = {
    iss: sa.client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets https://www.googleapis.com/auth/drive',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now
  };
  const h = b64u(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const p = b64u(JSON.stringify(claim));
  const sig = createSign('RSA-SHA256').update(h + '.' + p).sign(sa.private_key, 'base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
  const body = 'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=' + h + '.' + p + '.' + sig;
  const r = await req({
    hostname: 'oauth2.googleapis.com',
    path: '/token',
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
  }, body);
  if (!r.body || !r.body.access_token) throw new Error('gToken: ' + JSON.stringify(r.body));
  return r.body.access_token;
}

// ── SHEETS API HELPERS ─────────────────────────────────────────

async function sheetsGet(token, range) {
  const r = await req({
    hostname: 'sheets.googleapis.com',
    path: '/v4/spreadsheets/' + SHEET_ID + '/values/' + encodeURIComponent(range),
    method: 'GET',
    headers: { Authorization: 'Bearer ' + token }
  });
  if (r.status >= 400) throw new Error('sheetsGet ' + r.status + ': ' + JSON.stringify(r.body));
  return r.body;
}

async function sheetsAppend(token, range, values) {
  const r = await req({
    hostname: 'sheets.googleapis.com',
    path: '/v4/spreadsheets/' + SHEET_ID + '/values/' + encodeURIComponent(range)
      + ':append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS',
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/json'
    }
  }, { values });
  if (r.status >= 400) throw new Error('sheetsAppend ' + r.status + ': ' + JSON.stringify(r.body));
  return r.body;
}

async function sheetsUpdate(token, range, values) {
  const r = await req({
    hostname: 'sheets.googleapis.com',
    path: '/v4/spreadsheets/' + SHEET_ID + '/values/' + encodeURIComponent(range)
      + '?valueInputOption=USER_ENTERED',
    method: 'PUT',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/json'
    }
  }, { values });
  if (r.status >= 400) throw new Error('sheetsUpdate ' + r.status + ': ' + JSON.stringify(r.body));
  return r.body;
}

async function sheetsBatchUpdate(token, requests) {
  const r = await req({
    hostname: 'sheets.googleapis.com',
    path: '/v4/spreadsheets/' + SHEET_ID + ':batchUpdate',
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'application/json'
    }
  }, { requests });
  if (r.status >= 400) throw new Error('sheetsBatchUpdate ' + r.status + ': ' + JSON.stringify(r.body));
  return r.body;
}

async function getSheetNumericId(token) {
  const r = await req({
    hostname: 'sheets.googleapis.com',
    path: '/v4/spreadsheets/' + SHEET_ID + '?fields=' + encodeURIComponent('sheets.properties'),
    method: 'GET',
    headers: { Authorization: 'Bearer ' + token }
  });
  if (r.status >= 400) throw new Error('getSheetNumericId ' + r.status + ': ' + JSON.stringify(r.body));
  const sheets = (r.body && r.body.sheets) || [];
  const match = sheets.find(s => s.properties && s.properties.title === SHEET_NAME);
  return match ? match.properties.sheetId : null;
}

// Creates the Onboarding tab (with header row) the first time it's needed.
async function ensureSheet(token) {
  const existingId = await getSheetNumericId(token);
  if (existingId !== null) return;
  await sheetsBatchUpdate(token, [{ addSheet: { properties: { title: SHEET_NAME } } }]);
  await sheetsUpdate(token, SHEET_NAME + '!A1:S1', [HEADERS]);
}

function safeParseJson(s) {
  if (!s) return null;
  try { return JSON.parse(s); } catch (e) { return null; }
}
function safeParseObj(s) {
  const v = safeParseJson(s);
  return (v && typeof v === 'object' && !Array.isArray(v)) ? v : {};
}
function safeParseArr(s) {
  const v = safeParseJson(s);
  return Array.isArray(v) ? v : [];
}

// ── DRIVE API HELPERS ───────────────────────────────────────────
// Everything here is a no-op (returns null) when GL_STAFF_DRIVE_ID isn't
// set yet, so the checklist keeps working on the sheet alone until Drive
// access is wired up.

async function driveJson(token, path, method, body) {
  const r = await req({
    hostname: 'www.googleapis.com',
    path,
    method,
    headers: Object.assign(
      { Authorization: 'Bearer ' + token },
      body !== undefined ? { 'Content-Type': 'application/json' } : {}
    )
  }, body);
  if (r.status >= 400) throw new Error('drive ' + method + ' ' + r.status + ': ' + JSON.stringify(r.body));
  return r.body;
}

// Finds a folder by name under parentId, creating it if it doesn't exist.
async function ensureFolder(token, name, parentId) {
  const safe = name.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
  const q = encodeURIComponent(`name='${safe}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`);
  const found = await driveJson(
    token,
    '/drive/v3/files?q=' + q + '&supportsAllDrives=true&includeItemsFromAllDrives=true&corpora=allDrives&fields=' + encodeURIComponent('files(id,name)'),
    'GET'
  );
  if (found.files && found.files.length) return found.files[0].id;
  const created = await driveJson(
    token,
    '/drive/v3/files?supportsAllDrives=true&fields=id',
    'POST',
    { name, mimeType: 'application/vnd.google-apps.folder', parents: [parentId] }
  );
  return created.id;
}

async function shareWithEmailSafe(token, fileId, email, role) {
  if (!email || !fileId) return;
  try {
    await driveJson(
      token,
      '/drive/v3/files/' + fileId + '/permissions?supportsAllDrives=true&sendNotificationEmail=false',
      'POST',
      { type: 'user', role: role || 'writer', emailAddress: email }
    );
  } catch (e) {
    // Don't let a bad/duplicate email address break starter creation —
    // just log it so it's visible in the function logs.
    console.error('shareWithEmailSafe failed for', email, ':', e.message);
  }
}

function lookupManagerEmail(managerName) {
  if (!managerName) return null;
  return MANAGER_EMAILS[String(managerName).trim().toLowerCase()] || null;
}

// Creates Employees/<starter name> under the configured "Staff" folder
// (GL_STAFF_DRIVE_ID — the Staff folder itself, auto-creating "Employees"
// the first time), and shares the new folder with the looked-up manager
// plus everyone in MANAGEMENT_EMAILS. Returns { id, url } or null if
// Drive isn't configured yet.
async function createStarterFolder(token, starterName, managerName) {
  if (!STAFF_DRIVE_ROOT_ID) return null;
  const employeesId = await ensureFolder(token, 'Employees', STAFF_DRIVE_ROOT_ID);
  const folderId = await ensureFolder(token, starterName, employeesId);

  const managerEmail = lookupManagerEmail(managerName);
  const shared = new Set();
  if (managerEmail) shared.add(managerEmail.toLowerCase());
  await shareWithEmailSafe(token, folderId, managerEmail, 'writer');
  for (const email of MANAGEMENT_EMAILS) {
    if (shared.has(email.toLowerCase())) continue;
    shared.add(email.toLowerCase());
    await shareWithEmailSafe(token, folderId, email, 'writer');
  }

  return { id: folderId, url: 'https://drive.google.com/drive/folders/' + folderId };
}

async function driveUploadFile(token, parentId, filename, mimeType, base64Data) {
  const boundary = 'gl_onboarding_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8);
  const metadata = JSON.stringify({ name: filename, parents: [parentId] });
  const preamble =
    '--' + boundary + '\r\n' +
    'Content-Type: application/json; charset=UTF-8\r\n\r\n' +
    metadata + '\r\n' +
    '--' + boundary + '\r\n' +
    'Content-Type: ' + mimeType + '\r\n' +
    'Content-Transfer-Encoding: base64\r\n\r\n';
  const body = Buffer.concat([
    Buffer.from(preamble, 'utf8'),
    Buffer.from(base64Data, 'utf8'),
    Buffer.from('\r\n--' + boundary + '--', 'utf8')
  ]);
  const r = await reqRaw({
    hostname: 'www.googleapis.com',
    path: '/upload/drive/v3/files?uploadType=multipart&supportsAllDrives=true&fields=id,webViewLink',
    method: 'POST',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': 'multipart/related; boundary=' + boundary,
      'Content-Length': Buffer.byteLength(body)
    }
  }, body);
  if (r.status >= 400) throw new Error('driveUploadFile ' + r.status + ': ' + JSON.stringify(r.body));
  return r.body;
}

async function driveUpdateMedia(token, fileId, mimeType, base64Data) {
  const buf = Buffer.from(base64Data, 'base64');
  const r = await reqRaw({
    hostname: 'www.googleapis.com',
    path: '/upload/drive/v3/files/' + fileId + '?uploadType=media&supportsAllDrives=true&fields=id,webViewLink',
    method: 'PATCH',
    headers: {
      Authorization: 'Bearer ' + token,
      'Content-Type': mimeType,
      'Content-Length': buf.length
    }
  }, buf);
  if (r.status >= 400) throw new Error('driveUpdateMedia ' + r.status + ': ' + JSON.stringify(r.body));
  return r.body;
}

// Uploads new content under an existing fileId if we have one (keeps a
// stable link — used for the singleton HMRC/bank/next-of-kin files),
// otherwise creates a new file in the folder.
async function driveUploadOrReplace(token, existingFileId, parentId, filename, mimeType, base64Data) {
  if (existingFileId) return driveUpdateMedia(token, existingFileId, mimeType, base64Data);
  return driveUploadFile(token, parentId, filename, mimeType, base64Data);
}

function toBase64Text(str) { return Buffer.from(str, 'utf8').toString('base64'); }

function formatNextOfKin(contacts) {
  let out = 'NEXT OF KIN / EMERGENCY CONTACTS\n' + '='.repeat(40) + '\n\n';
  (contacts || []).forEach((c, i) => {
    out += 'Contact ' + (i + 1) + '\n';
    out += '  Name:         ' + (c.name || '') + '\n';
    out += '  Relationship: ' + (c.relationship || '') + '\n';
    out += '  Phone:        ' + (c.phone || '') + '\n';
    out += '  Email:        ' + (c.email || '') + '\n\n';
  });
  out += 'Last updated: ' + new Date().toISOString() + '\n';
  return out;
}

function formatHmrc(a) {
  a = a || {};
  let out = 'HMRC STARTER CHECKLIST — CONFIDENTIAL\n' + '='.repeat(40) + '\n\n';
  out += 'Last name:        ' + (a.lastName || '') + '\n';
  out += 'First name(s):    ' + (a.firstName || '') + '\n';
  out += 'Sex:              ' + (a.sex || '') + '\n';
  out += 'Date of birth:    ' + (a.dob || '') + '\n';
  out += 'Address:          ' + (a.address || '') + '\n';
  out += 'Postcode:         ' + (a.postcode || '') + '\n';
  out += 'Country:          ' + (a.country || '') + '\n';
  out += 'NI number:        ' + (a.niNumber || '') + '\n';
  out += 'Start date:       ' + (a.startDate || '') + '\n\n';
  out += 'Another job?                       ' + (a.hasOtherJob || '') + '\n';
  out += 'State/workplace/private pension?   ' + (a.hasPension || '') + '\n';
  out += 'Other job/benefits since 6 April?  ' + (a.sinceApril || '') + '\n';
  out += 'Tax code statement:  ' + (a.statement || '') + '\n\n';
  out += 'Student/postgraduate loan?         ' + (a.hasLoan || '') + '\n';
  if (a.hasLoan === 'Yes') {
    out += 'Loan plan type:                     ' + (a.loanPlan || '') + '\n';
  }
  out += '\nDeclared name: ' + (a.declaredName || '') + '\n';
  out += 'Declared date: ' + (a.declaredDate || '') + '\n';
  out += '\nSaved: ' + new Date().toISOString() + '\n';
  return out;
}

function formatBank(b) {
  b = b || {};
  let out = 'BANK DETAILS FOR PAYROLL — CONFIDENTIAL\n' + '='.repeat(40) + '\n\n';
  out += 'Account name:    ' + (b.accountName || '') + '\n';
  out += 'Bank name:       ' + (b.bankName || '') + '\n';
  out += 'Sort code:       ' + (b.sortCode || '') + '\n';
  out += 'Account number:  ' + (b.accountNumber || '') + '\n';
  out += '\nSaved: ' + new Date().toISOString() + '\n';
  return out;
}

// ── AUTH HELPERS (identical to staff-notifications.js) ─────────

function getUser(event) {
  if (event.clientContext && event.clientContext.user) {
    return event.clientContext.user;
  }
  try {
    const auth = (event.headers &&
      (event.headers.authorization || event.headers.Authorization)) || '';
    const m = auth.match(/^Bearer\s+(.+)$/i);
    if (!m) return null;
    const payload = m[1].split('.')[1];
    if (!payload) return null;
    const json = Buffer.from(payload, 'base64').toString('utf8');
    const claims = JSON.parse(json);
    if (claims.exp && Date.now() / 1000 > claims.exp) return null;
    return {
      id: claims.sub,
      email: claims.email,
      app_metadata: claims.app_metadata || {},
      user_metadata: claims.user_metadata || {}
    };
  } catch (e) {
    console.error('getUser decode failed:', e);
    return null;
  }
}

function isDirector(user) {
  if (!user) return false;
  const roles = (user.app_metadata && user.app_metadata.roles) || [];
  return roles.indexOf('director') !== -1;
}

// ── ROW HELPERS ──────────────────────────────────────────────────

function rowToItem(r, idx) {
  return {
    rowIndex: idx + 2,
    id: r[0] || '',
    created: r[1] || '',
    name: r[2] || '',
    role: r[3] || 'Grounds Operative',
    startDate: r[4] || '',
    manager: r[5] || '',
    depot: r[6] || '',
    archived: r[7] === true || r[7] === 'TRUE',
    checks: safeParseObj(r[8]),
    email: r[9] || '',
    driveFolderId: r[10] || '',
    driveFolderUrl: r[11] || '',
    evidence: safeParseObj(r[12]),
    nextOfKin: safeParseArr(r[13]),
    nextOfKinFileId: r[14] || '',
    hmrcStatus: r[15] || '',
    hmrcFileId: r[16] || '',
    bankStatus: r[17] || '',
    bankFileId: r[18] || ''
  };
}

async function getRow(token, rowIndex, expectedId) {
  const existing = await sheetsGet(token, RANGE_ROW(rowIndex));
  const cur = (existing.values && existing.values[0]) || [];
  if ((cur[0] || '') !== expectedId) return null;
  return cur;
}

async function writeRow(token, rowIndex, cur) {
  await sheetsUpdate(token, RANGE_ROW(rowIndex), [cur]);
}

// ── REQUEST HANDLERS ───────────────────────────────────────────

async function handleGet(token, qs) {
  const includeArchived = qs && qs.includeArchived === '1';
  const data = await sheetsGet(token, RANGE_ALL);
  const rows = data.values || [];

  const items = rows.map(rowToItem).filter(i => i.id);
  const filtered = includeArchived ? items : items.filter(i => !i.archived);
  filtered.sort((a, b) => (new Date(b.created).getTime() || 0) - (new Date(a.created).getTime() || 0));

  return { items: filtered, total: items.length, shown: filtered.length };
}

async function handlePost(token, body, user, qs) {
  const op = (qs && qs.op) || 'add';

  if (op === 'add') {
    if (!user) return { error: 'Sign in required to add a starter', status: 401 };
    const name = String(body.name || '').trim();
    if (!name) return { error: 'Name required', status: 400 };

    const role = String(body.role || 'Grounds Operative').trim() || 'Grounds Operative';
    const startDate = body.startDate ? String(body.startDate).trim() : '';
    const manager = body.manager ? String(body.manager).trim() : '';
    const depot = body.depot ? String(body.depot).trim() : '';

    const id = 'OB-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
    const created = new Date().toISOString().replace('T', ' ').replace(/\..+$/, '');

    let folder = null;
    try {
      folder = await createStarterFolder(token, name, manager);
    } catch (e) {
      console.error('createStarterFolder failed:', e.message);
    }

    const row = [
      id, created, name, role, startDate, manager, depot, 'FALSE', JSON.stringify({}),
      '', (folder && folder.id) || '', (folder && folder.url) || '', JSON.stringify({}),
      JSON.stringify([]), '', '', '', '', ''
    ];
    await sheetsAppend(token, SHEET_NAME + '!A:S', [row]);
    return { ok: true, id, driveFolderUrl: folder && folder.url };
  }

  // Every op below acts on an existing starter row.
  if (!user) return { error: 'Sign in required', status: 401 };
  const rowIndex = parseInt(body.rowIndex, 10);
  if (!rowIndex || rowIndex < 2) return { error: 'Invalid rowIndex', status: 400 };
  const cur = await getRow(token, rowIndex, body.id);
  if (!cur) return { error: 'Row no longer matches — refresh and try again', status: 409 };

  if (op === 'upload') {
    const itemId = String(body.itemId || '').trim();
    if (!itemId) return { error: 'itemId required', status: 400 };
    if (!body.dataBase64) return { error: 'No file data received', status: 400 };
    if (!cur[10]) return { error: "This starter doesn't have a Drive folder yet — set GL_STAFF_DRIVE_ID and re-add them, or ask an admin to wire up Drive access.", status: 409 };

    const evidence = safeParseObj(cur[12]);
    const filename = (String(body.filename || itemId).replace(/[\\/]/g, '-')) || itemId;
    const mimeType = body.mimeType || 'application/octet-stream';
    const existingFileId = evidence[itemId] && evidence[itemId].fileId;

    const uploaded = await driveUploadOrReplace(token, existingFileId, cur[10], filename, mimeType, body.dataBase64);
    evidence[itemId] = { fileId: uploaded.id, name: filename, webViewLink: uploaded.webViewLink, uploadedAt: new Date().toISOString() };
    cur[12] = JSON.stringify(evidence);
    await writeRow(token, rowIndex, cur);
    return { ok: true, evidence: evidence[itemId] };
  }

  if (op === 'email') {
    const email = String(body.email || '').trim();
    cur[9] = email;
    await writeRow(token, rowIndex, cur);
    if (email && cur[10]) await shareWithEmailSafe(token, cur[10], email, 'writer');
    return { ok: true, email };
  }

  if (op === 'nextOfKin') {
    const contacts = Array.isArray(body.contacts) ? body.contacts.slice(0, 3).map(c => ({
      name: String(c.name || '').trim(),
      relationship: String(c.relationship || '').trim(),
      phone: String(c.phone || '').trim(),
      email: String(c.email || '').trim()
    })) : [];
    cur[13] = JSON.stringify(contacts);
    let fileId = cur[14] || null;
    if (cur[10]) {
      try {
        const f = await driveUploadOrReplace(token, fileId, cur[10], 'Next of Kin.txt', 'text/plain', toBase64Text(formatNextOfKin(contacts)));
        fileId = f.id;
        cur[14] = fileId;
      } catch (e) { console.error('nextOfKin drive write failed:', e.message); }
    }
    await writeRow(token, rowIndex, cur);
    return { ok: true, contacts };
  }

  if (op === 'hmrc') {
    if (!cur[10]) return { error: "This starter doesn't have a Drive folder yet — set GL_STAFF_DRIVE_ID first.", status: 409 };
    let fileId = cur[16] || null;
    const f = await driveUploadOrReplace(token, fileId, cur[10], 'HMRC Starter Checklist.txt', 'text/plain', toBase64Text(formatHmrc(body.answers)));
    fileId = f.id;
    cur[15] = 'saved';
    cur[16] = fileId;
    await writeRow(token, rowIndex, cur);
    return { ok: true, status: 'saved' };
  }

  if (op === 'bank') {
    if (!cur[10]) return { error: "This starter doesn't have a Drive folder yet — set GL_STAFF_DRIVE_ID first.", status: 409 };
    let fileId = cur[18] || null;
    const f = await driveUploadOrReplace(token, fileId, cur[10], 'Bank Details - CONFIDENTIAL.txt', 'text/plain', toBase64Text(formatBank(body.bank)));
    fileId = f.id;
    cur[17] = 'saved';
    cur[18] = fileId;
    await writeRow(token, rowIndex, cur);
    return { ok: true, status: 'saved' };
  }

  return { error: 'Unknown op: ' + op, status: 400 };
}

// One PATCH endpoint covers ticking a single checklist item, marking a
// starter's onboarding complete/not complete, and editing their
// name/role/startDate/manager/depot — same "read row, merge only what's
// provided, write it back" approach as staff-notifications.js.
async function handlePatch(token, body, user) {
  if (!user) return { error: 'Sign in required to update a starter', status: 401 };

  const rowIndex = parseInt(body.rowIndex, 10);
  if (!rowIndex || rowIndex < 2) return { error: 'Invalid rowIndex', status: 400 };
  const cur = await getRow(token, rowIndex, body.id);
  if (!cur) return { error: 'Row no longer matches — refresh and try again', status: 409 };

  let checks = safeParseObj(cur[8]);
  if (body.itemId !== undefined) {
    checks = Object.assign({}, checks);
    checks[String(body.itemId)] = !!body.value;
  }

  cur[2] = body.name !== undefined ? String(body.name) : (cur[2] || '');
  cur[3] = body.role !== undefined ? String(body.role) : (cur[3] || '');
  cur[4] = body.startDate !== undefined ? String(body.startDate) : (cur[4] || '');
  cur[5] = body.manager !== undefined ? String(body.manager) : (cur[5] || '');
  cur[6] = body.depot !== undefined ? String(body.depot) : (cur[6] || '');
  cur[7] = body.archived !== undefined ? (body.archived ? 'TRUE' : 'FALSE') : (cur[7] || 'FALSE');
  cur[8] = JSON.stringify(checks);

  await writeRow(token, rowIndex, cur);
  return { ok: true, checks: checks, archived: cur[7] === 'TRUE' };
}

// Onboarding rows are few, and "Remove" here is mostly for undoing a
// mis-added starter, so — unlike staff-notifications.js's soft-delete —
// this does a real row delete via batchUpdate. Still director-only, still
// defended by the same id-at-rowIndex check.
async function handleDelete(token, body, user) {
  if (!isDirector(user)) return { error: 'Director role required to remove a starter', status: 403 };

  const rowIndex = parseInt(body.rowIndex, 10);
  if (!rowIndex || rowIndex < 2) return { error: 'Invalid rowIndex', status: 400 };
  const cur = await getRow(token, rowIndex, body.id);
  if (!cur) return { error: 'Row no longer matches — refresh and try again', status: 409 };

  const sheetId = await getSheetNumericId(token);
  if (sheetId === null) return { ok: true };

  await sheetsBatchUpdate(token, [{
    deleteDimension: {
      range: { sheetId: sheetId, dimension: 'ROWS', startIndex: rowIndex - 1, endIndex: rowIndex }
    }
  }]);
  return { ok: true };
}

// ── ENTRY POINT ────────────────────────────────────────────────

exports.handler = async (event) => {
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Methods': 'GET, POST, PATCH, DELETE, OPTIONS'
  };

  if (event.httpMethod === 'OPTIONS') return { statusCode: 204, headers };

  try {
    const sa = await fetchSA();
    const token = await gToken(sa);
    const user = getUser(event);
    const qs = event.queryStringParameters || {};

    await ensureSheet(token);

    let result;
    if (event.httpMethod === 'GET') {
      result = await handleGet(token, qs);
    } else if (event.httpMethod === 'POST') {
      const body = JSON.parse(event.body || '{}');
      result = await handlePost(token, body, user, qs);
    } else if (event.httpMethod === 'PATCH') {
      const body = JSON.parse(event.body || '{}');
      result = await handlePatch(token, body, user);
    } else if (event.httpMethod === 'DELETE') {
      const body = JSON.parse(event.body || '{}');
      result = await handleDelete(token, body, user);
    } else {
      return { statusCode: 405, headers, body: JSON.stringify({ error: 'Method not allowed' }) };
    }

    if (result.error) {
      return { statusCode: result.status || 500, headers, body: JSON.stringify({ error: result.error }) };
    }
    return { statusCode: 200, headers, body: JSON.stringify(result) };
  } catch (err) {
    console.error('onboarding error:', err.message);
    return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
  }
};
