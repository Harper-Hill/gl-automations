'use strict';
const https = require('https');
const { createSign } = require('crypto');

const SHEET_ID = process.env.GL_SHEET_ID;
const TAB_NOTIFICATIONS = 'StaffNotifications';
const TAB_BRIEFINGS = 'DailyBriefings';

// Column letters for StaffNotifications:
// A=ID, B=Created, C=Source, D=Content, E=Audience, F=Expires,
// G=Priority, H=DeliveredTo, I=Status

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

// ── AUTH HELPERS ───────────────────────────────────────────────

// Decode Netlify Identity JWT (no signature verification — Netlify already
// verifies it before passing it through the context.clientContext.user object;
// we just trust that). The function-level `event.clientContext.user` is set
// automatically when an Authorization: Bearer header is present.
function getUser(event) {
  if (event.clientContext && event.clientContext.user) {
    return event.clientContext.user;
  }
  return null;
}

function isDirector(user) {
  if (!user) return false;
  const roles = (user.app_metadata && user.app_metadata.roles) || [];
  return roles.indexOf('director') !== -1;
}

// ── REQUEST HANDLERS ───────────────────────────────────────────

async function handleGet(token, qs) {
  const showAll = qs && qs.showAll === '1';

  // Read full notifications + briefings
  const [notifData, briefData] = await Promise.all([
    sheetsGet(token, TAB_NOTIFICATIONS + '!A2:I'),
    sheetsGet(token, TAB_BRIEFINGS + '!A2:H')
  ]);

  const notifRows = notifData.values || [];
  const briefRows = briefData.values || [];

  // Build a map of notification ID → { delivered: [staff], read: [staff] }
  // by joining DailyBriefings.NotificationIDs (col F) and CompletedAt (col G)
  const auditMap = {};
  briefRows.forEach(r => {
    const userName = r[2] || '';
    const notifIdsCsv = r[5] || '';
    const completedAt = r[6] || '';
    notifIdsCsv.split(',').map(s => s.trim()).filter(Boolean).forEach(id => {
      if (!auditMap[id]) auditMap[id] = { delivered: [], read: [] };
      auditMap[id].delivered.push(userName);
      if (completedAt) auditMap[id].read.push(userName);
    });
  });

  const cutoff = Date.now() - 30 * 24 * 60 * 60 * 1000; // 30 days

  const items = notifRows.map((r, idx) => {
    const created = r[1] ? new Date(r[1]).getTime() : 0;
    const audit = auditMap[r[0]] || { delivered: [], read: [] };
    return {
      rowIndex: idx + 2, // sheet row number (1-indexed, +1 for header)
      id: r[0] || '',
      created: r[1] || '',
      source: r[2] || '',
      content: r[3] || '',
      audience: r[4] || 'all',
      expires: r[5] || '',
      priority: r[6] || 'normal',
      deliveredToCol: r[7] || '',
      status: r[8] || '',
      delivered: audit.delivered,
      read: audit.read,
      _createdMs: created
    };
  });

  const filtered = showAll ? items : items.filter(i => i._createdMs >= cutoff);

  // Sort newest first
  filtered.sort((a, b) => b._createdMs - a._createdMs);

  filtered.forEach(i => delete i._createdMs);

  return { items: filtered, total: items.length, shown: filtered.length };
}

async function handlePost(token, body, user) {
  if (!user) return { error: 'Sign in required to add notifications', status: 401 };

  const content = String(body.content || '').trim();
  if (!content) return { error: 'Content required', status: 400 };

  const audience = String(body.audience || 'all').trim() || 'all';
  const priority = (body.priority === 'high') ? 'high' : 'normal';
  const expires = body.expires ? String(body.expires).trim() : '';

  const id = 'UI-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
  const created = new Date().toISOString().replace('T', ' ').replace(/\..+$/, '');

  const row = [
    id,             // A: ID
    created,        // B: Created
    'manual',       // C: Source
    content,        // D: Content
    audience,       // E: Audience
    expires,        // F: Expires
    priority,       // G: Priority
    '',             // H: DeliveredTo
    ''              // I: Status
  ];

  await sheetsAppend(token, TAB_NOTIFICATIONS + '!A:I', [row]);
  return { ok: true, id };
}

async function handlePatch(token, body, user) {
  if (!isDirector(user)) return { error: 'Director role required to edit', status: 403 };

  const rowIndex = parseInt(body.rowIndex, 10);
  if (!rowIndex || rowIndex < 2) return { error: 'Invalid rowIndex', status: 400 };

  // Verify ID matches what we expect (defence against stale rowIndex if rows shifted)
  const idCheck = await sheetsGet(token, TAB_NOTIFICATIONS + '!A' + rowIndex);
  const existingId = idCheck.values && idCheck.values[0] && idCheck.values[0][0];
  if (existingId !== body.id) {
    return { error: 'Row no longer matches — refresh and try again', status: 409 };
  }

  // We allow editing Content, Audience, Expires, Priority, Status (D, E, F, G, I).
  // Read existing row first so we only change what's provided.
  const existing = await sheetsGet(token, TAB_NOTIFICATIONS + '!A' + rowIndex + ':I' + rowIndex);
  const cur = (existing.values && existing.values[0]) || [];
  const merged = [
    cur[0] || '', // A: ID — preserved
    cur[1] || '', // B: Created — preserved
    cur[2] || '', // C: Source — preserved
    body.content !== undefined ? String(body.content) : (cur[3] || ''),
    body.audience !== undefined ? String(body.audience) : (cur[4] || ''),
    body.expires !== undefined ? String(body.expires) : (cur[5] || ''),
    body.priority !== undefined ? String(body.priority) : (cur[6] || ''),
    cur[7] || '', // H: DeliveredTo — preserved
    body.status !== undefined ? String(body.status) : (cur[8] || '')
  ];

  await sheetsUpdate(token, TAB_NOTIFICATIONS + '!A' + rowIndex + ':I' + rowIndex, [merged]);
  return { ok: true };
}

// "Delete" is implemented as setting Status=expired (soft delete).
// Hard delete would shift row numbers and break in-flight rowIndex references;
// soft delete is safer for a multi-user form.
async function handleDelete(token, body, user) {
  if (!isDirector(user)) return { error: 'Director role required to delete', status: 403 };

  const rowIndex = parseInt(body.rowIndex, 10);
  if (!rowIndex || rowIndex < 2) return { error: 'Invalid rowIndex', status: 400 };

  const idCheck = await sheetsGet(token, TAB_NOTIFICATIONS + '!A' + rowIndex);
  const existingId = idCheck.values && idCheck.values[0] && idCheck.values[0][0];
  if (existingId !== body.id) {
    return { error: 'Row no longer matches — refresh and try again', status: 409 };
  }

  await sheetsUpdate(token, TAB_NOTIFICATIONS + '!I' + rowIndex, [['expired']]);
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
    console.error('staff-notifications error:', err.message);
    return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
  }
};
