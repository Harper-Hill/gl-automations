'use strict';
const https = require('https');
const { createSign } = require('crypto');

const SHEET_ID = process.env.GL_SHEET_ID;
const SHEET_NAME = 'Onboarding';

// Column letters for Onboarding:
// A=ID, B=Created, C=Name, D=Role, E=StartDate, F=Manager, G=Depot,
// H=Archived, I=ChecksJson
const HEADERS = ['id', 'created', 'name', 'role', 'startDate', 'manager', 'depot', 'archived', 'checksJson'];

// ── SA + GOOGLE TOKEN ──────────────────────────────────────────

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

function b64u(s) {
  return Buffer.from(s).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

async function gToken(sa) {
  const now = Math.floor(Date.now() / 1000);
  const claim = {
    iss: sa.client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
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

// Creates the Onboarding tab (with header row) the first time it's needed —
// mirrors onboarding_getSheet_()'s auto-create behaviour from the earlier
// Apps-Script draft, just done via the Sheets API directly.
async function ensureSheet(token) {
  const existingId = await getSheetNumericId(token);
  if (existingId !== null) return;
  await sheetsBatchUpdate(token, [{ addSheet: { properties: { title: SHEET_NAME } } }]);
  await sheetsUpdate(token, SHEET_NAME + '!A1:I1', [HEADERS]);
}

function safeParseJson(s) {
  if (!s) return {};
  try {
    const parsed = JSON.parse(s);
    return (parsed && typeof parsed === 'object') ? parsed : {};
  } catch (e) {
    return {};
  }
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

// ── REQUEST HANDLERS ───────────────────────────────────────────

async function handleGet(token, qs) {
  const includeArchived = qs && qs.includeArchived === '1';
  const data = await sheetsGet(token, SHEET_NAME + '!A2:I');
  const rows = data.values || [];

  const items = rows
    .map((r, idx) => ({
      rowIndex: idx + 2,
      id: r[0] || '',
      created: r[1] || '',
      name: r[2] || '',
      role: r[3] || 'Grounds Operative',
      startDate: r[4] || '',
      manager: r[5] || '',
      depot: r[6] || '',
      archived: r[7] === true || r[7] === 'TRUE',
      checks: safeParseJson(r[8]),
      _createdMs: r[1] ? new Date(r[1]).getTime() : 0
    }))
    .filter(i => i.id);

  const filtered = includeArchived ? items : items.filter(i => !i.archived);
  filtered.sort((a, b) => b._createdMs - a._createdMs);
  filtered.forEach(i => delete i._createdMs);

  return { items: filtered, total: items.length, shown: filtered.length };
}

async function handlePost(token, body, user) {
  if (!user) return { error: 'Sign in required to add a starter', status: 401 };

  const name = String(body.name || '').trim();
  if (!name) return { error: 'Name required', status: 400 };

  const role = String(body.role || 'Grounds Operative').trim() || 'Grounds Operative';
  const startDate = body.startDate ? String(body.startDate).trim() : '';
  const manager = body.manager ? String(body.manager).trim() : '';
  const depot = body.depot ? String(body.depot).trim() : '';

  const id = 'OB-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
  const created = new Date().toISOString().replace('T', ' ').replace(/\..+$/, '');

  const row = [id, created, name, role, startDate, manager, depot, 'FALSE', JSON.stringify({})];
  await sheetsAppend(token, SHEET_NAME + '!A:I', [row]);
  return { ok: true, id };
}

// One PATCH endpoint covers three things the frontend needs: ticking a single
// checklist item, marking a starter's onboarding complete/not complete, and
// editing their name/role/startDate/manager/depot — same
// "read row, merge only what's provided, write it back" approach as
// staff-notifications.js's handlePatch.
async function handlePatch(token, body, user) {
  if (!user) return { error: 'Sign in required to update a starter', status: 401 };

  const rowIndex = parseInt(body.rowIndex, 10);
  if (!rowIndex || rowIndex < 2) return { error: 'Invalid rowIndex', status: 400 };

  const idCheck = await sheetsGet(token, SHEET_NAME + '!A' + rowIndex);
  const existingId = idCheck.values && idCheck.values[0] && idCheck.values[0][0];
  if (existingId !== body.id) {
    return { error: 'Row no longer matches — refresh and try again', status: 409 };
  }

  const existing = await sheetsGet(token, SHEET_NAME + '!A' + rowIndex + ':I' + rowIndex);
  const cur = (existing.values && existing.values[0]) || [];

  let checks = safeParseJson(cur[8]);
  if (body.itemId !== undefined) {
    checks = Object.assign({}, checks);
    checks[String(body.itemId)] = !!body.value;
  }

  const merged = [
    cur[0] || '',
    cur[1] || '',
    body.name !== undefined ? String(body.name) : (cur[2] || ''),
    body.role !== undefined ? String(body.role) : (cur[3] || ''),
    body.startDate !== undefined ? String(body.startDate) : (cur[4] || ''),
    body.manager !== undefined ? String(body.manager) : (cur[5] || ''),
    body.depot !== undefined ? String(body.depot) : (cur[6] || ''),
    body.archived !== undefined ? (body.archived ? 'TRUE' : 'FALSE') : (cur[7] || 'FALSE'),
    JSON.stringify(checks)
  ];

  await sheetsUpdate(token, SHEET_NAME + '!A' + rowIndex + ':I' + rowIndex, [merged]);
  return { ok: true, checks: checks, archived: merged[7] === 'TRUE' };
}

// Unlike staff-notifications.js's soft-delete (a handful of notifications a
// week, so row-shift risk from a hard delete isn't worth it), Onboarding
// rows are few and "Remove" here is mostly for undoing a mis-added starter —
// so this does a real row delete via batchUpdate, still gated to directors
// and still defended by the same id-at-rowIndex check.
async function handleDelete(token, body, user) {
  if (!isDirector(user)) return { error: 'Director role required to remove a starter', status: 403 };

  const rowIndex = parseInt(body.rowIndex, 10);
  if (!rowIndex || rowIndex < 2) return { error: 'Invalid rowIndex', status: 400 };

  const idCheck = await sheetsGet(token, SHEET_NAME + '!A' + rowIndex);
  const existingId = idCheck.values && idCheck.values[0] && idCheck.values[0][0];
  if (existingId !== body.id) {
    return { error: 'Row no longer matches — refresh and try again', status: 409 };
  }

  const sheetId = await getSheetNumericId(token);
  if (sheetId === null) return { ok: true }; // sheet already gone — fine

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

    await ensureSheet(token);

    let result;
    if (event.httpMethod === 'GET') {
      result = await handleGet(token, event.queryStringParameters || {});
    } else if (event.httpMethod === 'POST') {
      const body = JSON.parse(event.body || '{}');
      result = await handlePost(token, body, user);
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
