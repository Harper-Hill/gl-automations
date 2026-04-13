// ================================================================
// netlify/functions/jobber-invoice-webhook.js
// Receives INVOICE_CREATE / INVOICE_UPDATE from Jobber
// → fetches invoice details → appends/updates row in Google Sheet
// Uses Sheets API (not xlsx) so formatting and formulas are preserved
// ================================================================

'use strict';

const crypto = require('crypto');
const https  = require('https');

async function fetchServiceAccount() {
  function get(url) {
    return new Promise((resolve, reject) => {
      https.get(url, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) return get(res.headers.location).then(resolve).catch(reject);
        let d = ''; res.on('data', c => d += c);
        res.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(new Error('SA parse: ' + d.substring(0,100))); } });
      }).on('error', reject);
    });
  }
  return JSON.parse(Buffer.from((process.env.GOOGLE_SA_B64_1||"")+(process.env.GOOGLE_SA_B64_2||""),"base64").toString("utf8"));
}

const CFG = {
  CLIENT_ID:      process.env.JOBBER_GL_CLIENT_ID,
  CLIENT_SECRET:  process.env.JOBBER_GL_CLIENT_SECRET,
  REFRESH_TOKEN:  process.env.JOBBER_REFRESH_TOKEN,
  SHEET_ID:       process.env.GL_SHEET_ID,
  SHEET_TAB:      process.env.GL_SHEET_TAB || 'Income',
  SHEET_GID:      process.env.GL_SHEET_GID,
  FRS_RATE:       0.12,
  API_VERSION:    '2026-03-10',
};

const COL = {
  POSTED_DATE:  0,
  TAX_DATE:     1,
  COUNTERPARTY: 2,
  DESCRIPTION:  3,
  ISSUES_VIA:   4,
  INVOICE_NO:   5,
  TYPE:         7,
  TOTAL:        8,
  VAT:          9,
  EX_VAT:       10,
  FRS:          11,
  NOTES_JOB:    13,
  TOTAL_YTD:    14,
  EX_VAT_LIB:   15,
  NOTES:        16,
  DOM_COMM:     17,
  COMM_PCT:     18,
  DATE_PAID:    19,
};

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') return { statusCode: 405, body: 'Method Not Allowed' };

  const sig = event.headers['x-jobber-hmac-sha256'];
  if (!verifyHMAC(event.body, sig)) {
    console.error('Invalid HMAC signature');
    return { statusCode: 401, body: 'Unauthorized' };
  }

  let payload;
  try { payload = JSON.parse(event.body); }
  catch (e) { return { statusCode: 400, body: 'Bad JSON' }; }

  const webhookEvent = payload.data && payload.data.webHookEvent;
  const topic        = webhookEvent && webhookEvent.topic;
  const invoiceId    = webhookEvent && webhookEvent.itemId;
  console.log(`Webhook: topic=${topic}, invoiceId=${invoiceId}`);

  if (!topic || !topic.startsWith('INVOICE') || !invoiceId) {
    return { statusCode: 200, body: 'OK - ignored' };
  }

  try {
    const googleToken = await getGoogleToken();
    const jobberToken = await getJobberToken(googleToken);
    const invoice     = await fetchInvoice(jobberToken, invoiceId);
    if (!invoice) { console.log('Invoice not found'); return { statusCode: 200, body: 'OK - not found' }; }
    console.log('Fetched invoice:', invoice.invoiceNumber);

    const rows        = await readSheet(googleToken);
    console.log('Sheet rows:', rows.length);

    const invoiceMap = {};
    for (let i = 1; i < rows.length; i++) {
      const n = String(rows[i][COL.INVOICE_NO] || '').trim();
      if (n) invoiceMap[n] = i + 1;
    }

    const key    = String(invoice.invoiceNumber);
    const mapped = mapInvoice(invoice);

    if (invoiceMap.hasOwnProperty(key)) {
      const sheetRow = invoiceMap[key];
      await reconcile(googleToken, sheetRow, rows[sheetRow - 1], mapped);
    } else {
      const newSheetRow  = rows.length + 1;
      const newRowValues = buildRow(mapped, newSheetRow);
      await appendRow(googleToken, newRowValues);
      console.log(`Invoice ${key}: appended at row ${newSheetRow}`);
      await sortSheet(googleToken);
      console.log('Sheet sorted');
    }

    return { statusCode: 200, body: 'OK' };
  } catch (err) {
    console.error('Handler error:', err.message);
    return { statusCode: 500, body: 'Internal Server Error' };
  }
};

function mapInvoice(inv) {
  const incTotal = parseFloat((inv.amounts && inv.amounts.total)     || 0);
  const tip      = parseFloat((inv.amounts && inv.amounts.tipsTotal) || 0);
  // Use Jobber's total (inc VAT); fall back to subtotal*1.2 if not present
  const subtotal = parseFloat((inv.amounts && inv.amounts.subtotal)  || 0);
  const total    = incTotal > 0 ? round2(incTotal) : round2(subtotal * 1.2 + tip);
  const postedDate = inv.createdAt  ? toDate(inv.createdAt)  : '';
  const taxDate    = inv.issuedDate ? toDate(inv.issuedDate) : postedDate;
  let datePaid = 'Not Yet Paid';
  if (inv.receivedDate) datePaid = toDate(inv.receivedDate);
  const frs = datePaid !== 'Not Yet Paid' ? round2(total * CFG.FRS_RATE) : '';
  const client = inv.client || {};
  const counterparty = (client.name || `${client.firstName||''} ${client.lastName||''}`.trim() || client.companyName || '').trim();
  const lineNodes   = (inv.lineItems && inv.lineItems.nodes) || [];
  const description = lineNodes.map(li => (li.name || li.description || '').trim()).filter(Boolean).join(', ') || inv.subject || `Invoice #${inv.invoiceNumber}`;
  let invoiceType = '';
  (inv.customFields || []).forEach(cf => {
    if ((cf.label || '').toLowerCase().includes('invoice type')) invoiceType = cf.valueDropdown || cf.valueText || '';
  });
  return { postedDate, taxDate, counterparty, description, invoiceNo: String(inv.invoiceNumber), total, frs, tipNote: tip > 0 ? `Includes tip £${tip.toFixed(2)}` : '', domComm: invoiceType, datePaid };
}

function buildRow(m, r) {
  const prev = r - 1;
  const row = new Array(20).fill('');
  row[COL.POSTED_DATE]  = m.postedDate;
  row[COL.TAX_DATE]     = m.taxDate;
  row[COL.COUNTERPARTY] = m.counterparty;
  row[COL.DESCRIPTION]  = m.description;
  row[COL.ISSUES_VIA]   = 'Jobber Payments';
  row[COL.INVOICE_NO]   = m.invoiceNo;
  row[COL.TYPE]         = 'SALES';
  row[COL.TOTAL]        = m.total;
  row[COL.VAT]          = `=I${r}/6`;
  row[COL.EX_VAT]       = `=I${r}-J${r}`;
  row[COL.FRS]          = m.frs;
  row[COL.NOTES_JOB]    = m.tipNote;
  row[COL.TOTAL_YTD]    = `=O${prev}+I${r}`;
  row[COL.EX_VAT_LIB]   = `=P${prev}+K${r}`;
  row[COL.DOM_COMM]     = m.domComm;
  row[COL.COMM_PCT]     = `=IF(R${r}="Commercial",I${r}/O${r},0)`;
  row[COL.DATE_PAID]    = m.datePaid;
  return row;
}

async function reconcile(googleToken, sheetRow, existing, m) {
  const existingTotal   = parseFloat(existing[COL.TOTAL] || 0);
  const existingName    = String(existing[COL.COUNTERPARTY] || '').trim().toLowerCase();
  const existingDatePaid = String(existing[COL.DATE_PAID] || '').trim();
  // existingTotal may be NaN if the cell contains a formula — treat that as a match
  const totalOk = isNaN(existingTotal) || Math.abs(existingTotal - m.total) < 0.02;
  const nameOk  = existingName === m.counterparty.toLowerCase();

  console.log(`Reconcile row ${sheetRow}: total=${existingTotal} vs ${m.total}, datePaid="${existingDatePaid}" vs "${m.datePaid}"`);

  if (!totalOk || !nameOk) {
    await updateCell(googleToken, `${CFG.SHEET_TAB}!Q${sheetRow}`, `⚠️ REVIEW: Jobber Total=${m.total}, Name="${m.counterparty}" — sheet Total=${existingTotal}, Name="${existingName}"`);
    console.log(`Invoice ${m.invoiceNo}: flagged`);
    return;
  }

  let changed = false;

  // Update Date Paid + FRS whenever Jobber has a payment date and sheet doesn't match
  const sheetIsPaid = existingDatePaid !== '' && existingDatePaid !== 'Not Yet Paid';
  if (m.datePaid !== 'Not Yet Paid' && !sheetIsPaid) {
    await updateCell(googleToken, `${CFG.SHEET_TAB}!L${sheetRow}`, m.frs);
    await updateCell(googleToken, `${CFG.SHEET_TAB}!T${sheetRow}`, m.datePaid);
    console.log(`Invoice ${m.invoiceNo}: marked paid ${m.datePaid}, FRS=${m.frs}`);
    changed = true;
  }

  if (m.domComm && !String(existing[COL.DOM_COMM] || '').trim()) {
    await updateCell(googleToken, `${CFG.SHEET_TAB}!R${sheetRow}`, m.domComm);
    changed = true;
  }

  console.log(`Invoice ${m.invoiceNo}: ${changed ? 'updated' : 'ok (no changes needed)'}`);
}

async function readSheet(token) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.SHEET_TAB)}`;
  const resp = await sheetsGet(token, url);
  return resp.values || [];
}

async function appendRow(token, rowValues) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.SHEET_TAB)}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`;
  await sheetsRequest('POST', token, url, JSON.stringify({ values: [rowValues] }));
}

async function updateCell(token, range, value) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=USER_ENTERED`;
  await sheetsRequest('PUT', token, url, JSON.stringify({ range, values: [[value]] }));
}

async function sortSheet(token) {
  if (!CFG.SHEET_GID) { console.log('GL_SHEET_GID not set — skipping sort'); return; }
  const rows = await readSheet(token);
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}:batchUpdate`;
  const body = JSON.stringify({ requests: [{ sortRange: { range: { sheetId: parseInt(CFG.SHEET_GID), startRowIndex: 1, endRowIndex: rows.length, startColumnIndex: 0, endColumnIndex: 20 }, sortSpecs: [{ dimensionIndex: 0, sortOrder: 'ASCENDING' }] } }] });
  await sheetsRequest('POST', token, url, body);
}

function sheetsGet(token, url) {
  return new Promise((resolve, reject) => {
    const p = new URL(url);
    https.get({ hostname: p.hostname, path: p.pathname + p.search, headers: { Authorization: `Bearer ${token}` } }, (res) => {
      let d = ''; res.on('data', c => d += c); res.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(new Error('Parse: ' + d.substring(0,100))); } });
    }).on('error', reject);
  });
}

function sheetsRequest(method, token, url, body) {
  return new Promise((resolve, reject) => {
    const p = new URL(url);
    const req = https.request({ hostname: p.hostname, path: p.pathname + p.search, method, headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } }, (res) => {
      let d = ''; res.on('data', c => d += c); res.on('end', () => {
        if (res.statusCode >= 400) reject(new Error(`Sheets ${method} ${res.statusCode}: ${d.substring(0,200)}`));
        else { try { resolve(JSON.parse(d)); } catch { resolve({}); } }
      });
    });
    req.on('error', reject); req.write(body); req.end();
  });
}

async function getJobberToken(googleToken) {
  // Read refresh token from Config!B1 in the sheet — survives redeploys, works with native Google Sheets
  const refreshToken = await readSheetToken(googleToken);
  if (!refreshToken) throw new Error('No refresh token in Config!B1 — please add it');
  console.log('Got refresh token from Config sheet');

  const body = ['client_id='+encodeURIComponent(CFG.CLIENT_ID),'client_secret='+encodeURIComponent(CFG.CLIENT_SECRET),'grant_type=refresh_token','refresh_token='+encodeURIComponent(refreshToken)].join('&');
  const resp = await httpsPost('api.getjobber.com', '/api/oauth/token', body, { 'Content-Type': 'application/x-www-form-urlencoded' });
  let data; try { data = JSON.parse(resp); } catch(e) { throw new Error('Token not JSON: ' + resp.substring(0,100)); }
  if (!data.access_token) throw new Error('Token failed: ' + JSON.stringify(data));
  console.log('Jobber token OK');

  // Write rotated refresh token back to Config!B1
  if (data.refresh_token && data.refresh_token !== refreshToken) {
    console.log('Rotating refresh token in Config sheet');
    await writeSheetToken(googleToken, data.refresh_token).catch(e => console.error('Token rotation write failed:', e.message));
  }

  return data.access_token;
}

async function readSheetToken(googleToken) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/Config!B1`;
  const resp = await sheetsGet(googleToken, url);
  const val  = resp.values && resp.values[0] && resp.values[0][0];
  console.log('Config!B1 read:', val ? 'got token' : 'empty — ' + JSON.stringify(resp).substring(0,100));
  return val ? val.trim() : null;
}

async function writeSheetToken(googleToken, token) {
  const url = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent('Config!B1')}?valueInputOption=RAW`;
  await sheetsRequest('PUT', googleToken, url, JSON.stringify({ range: 'Config!B1', values: [[token]] }));
}

async function fetchInvoice(token, invoiceId) {
  const query = { query: `query { invoice(id: "${invoiceId}") { invoiceNumber subject createdAt issuedDate receivedDate amounts { subtotal total tipsTotal } client { name firstName lastName companyName } lineItems { nodes { name description } } customFields { ... on CustomFieldText { label valueText } ... on CustomFieldDropdown { label valueDropdown } } } }` };
  const resp = await httpsPost('api.getjobber.com', '/api/graphql', JSON.stringify(query), { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}`, 'X-JOBBER-GRAPHQL-VERSION': CFG.API_VERSION });
  let data; try { data = JSON.parse(resp); } catch(e) { throw new Error('Jobber API not JSON'); }
  if (data.errors) console.error('GraphQL errors:', JSON.stringify(data.errors).substring(0,200));
  return data.data && data.data.invoice;
}

async function getGoogleToken() {
  const sa = await fetchServiceAccount();
  const now = Math.floor(Date.now() / 1000);
  const claim = { iss: sa.client_email, scope: 'https://www.googleapis.com/auth/spreadsheets', aud: 'https://oauth2.googleapis.com/token', exp: now + 3600, iat: now };
  const jwt = buildJWT(sa.private_key, claim);
  const body = `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`;
  const resp = await httpsPost('oauth2.googleapis.com', '/token', body, { 'Content-Type': 'application/x-www-form-urlencoded' });
  const data = JSON.parse(resp);
  if (!data.access_token) throw new Error('Google auth failed');
  return data.access_token;
}

async function updateNetlifyEnv(key, value) {
  const siteId = process.env.NETLIFY_SITE_ID;
  const pat    = process.env.NETLIFY_ACCESS_TOKEN;
  if (!siteId || !pat) return;
  const body = JSON.stringify([{ value, context: 'all' }]);
  return new Promise((resolve) => {
    const req = https.request({ hostname: 'api.netlify.com', path: `/api/v1/sites/${siteId}/env/${key}`, method: 'PATCH', headers: { Authorization: 'Bearer ' + pat, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) } }, (res) => { res.resume(); res.on('end', resolve); });
    req.on('error', resolve); req.write(body); req.end();
  });
}

function verifyHMAC(body, signature) {
  if (!signature || !CFG.CLIENT_SECRET) return false;
  const expected = crypto.createHmac('sha256', CFG.CLIENT_SECRET).update(body, 'utf8').digest('base64');
  try { return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected)); } catch { return false; }
}

function buildJWT(privateKey, claims) {
  const header = b64u(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64u(JSON.stringify(claims));
  const signing = `${header}.${payload}`;
  const sign = crypto.createSign('RSA-SHA256'); sign.update(signing);
  return `${signing}.${sign.sign(privateKey, 'base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'')}`;
}
function b64u(s) { return Buffer.from(s).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }
function toDate(iso) { if (!iso) return ''; const d = new Date(iso); if (isNaN(d)) return ''; return `${pad(d.getDate())}/${pad(d.getMonth()+1)}/${d.getFullYear()}`; }
function pad(n) { return String(n).padStart(2,'0'); }
function round2(n) { return Math.round(n * 100) / 100; }
function httpsPost(hostname, path, body, headers) {
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method: 'POST', headers: { 'Content-Length': Buffer.byteLength(body), ...headers } }, (res) => { let d = ''; res.on('data', c => d += c); res.on('end', () => resolve(d)); });
    req.on('error', reject); req.write(body); req.end();
  });
}
