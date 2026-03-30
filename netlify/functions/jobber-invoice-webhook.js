// ================================================================
// netlify/functions/jobber-invoice-webhook.js
// Receives INVOICE_CREATE / INVOICE_UPDATE from Jobber
// → fetches invoice details → appends/updates GL_2026.xlsx in Drive
// ================================================================

'use strict';

const crypto = require('crypto');
const https  = require('https');
const XLSX   = require('xlsx');

// ── Config from Netlify env vars ──────────────────────────────
const CFG = {
  CLIENT_ID:      process.env.JOBBER_GL_CLIENT_ID,
  CLIENT_SECRET:  process.env.JOBBER_GL_CLIENT_SECRET,
  WEBHOOK_SECRET: process.env.JOBBER_GL_CLIENT_SECRET, // Jobber uses Client Secret for HMAC
  REFRESH_TOKEN:  process.env.JOBBER_REFRESH_TOKEN,
  DRIVE_FILE_ID:  process.env.GL_DRIVE_FILE_ID,
  INCOME_SHEET:   'Income',
  FRS_RATE:       0.12,
  JOBBER_API:     'https://api.getjobber.com/api/graphql',
  API_VERSION:    '2026-03-10',
};

// Income tab column indices (0-based, A=0)
const COL = {
  POSTED_DATE:  0,  // A — createdAt
  TAX_DATE:     1,  // B — issuedDate
  COUNTERPARTY: 2,  // C — client name
  DESCRIPTION:  3,  // D — line items concatenated
  ISSUES_VIA:   4,  // E — "Jobber Payments"
  INVOICE_NO:   5,  // F — invoiceNumber
  WORKING_COL:  6,  // G — blank
  TYPE:         7,  // H — "SALES"
  TOTAL:        8,  // I — subtotal + tip
  VAT:          9,  // J — formula
  EX_VAT:       10, // K — formula
  FRS:          11, // L — 12% if paid
  VAT_RETURN:   12, // M — blank (manual)
  NOTES_JOB:    13, // N — tip note
  TOTAL_YTD:    14, // O — formula
  EX_VAT_LIB:   15, // P — formula
  NOTES:        16, // Q — review flags
  DOM_COMM:     17, // R — Invoice Type
  COMM_PCT:     18, // S — formula
  DATE_PAID:    19, // T — receivedDate
};

const FORMULA_COLS = [COL.VAT, COL.EX_VAT, COL.TOTAL_YTD, COL.EX_VAT_LIB, COL.COMM_PCT];


// ================================================================
// HANDLER
// ================================================================
exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  // 1. Verify HMAC signature
  const sig = event.headers['x-jobber-hmac-sha256'];
  if (!verifyHMAC(event.body, sig)) {
    console.error('Invalid HMAC signature');
    return { statusCode: 401, body: 'Unauthorized' };
  }

  // 2. Parse payload
  let payload;
  try { payload = JSON.parse(event.body); }
  catch (e) { return { statusCode: 400, body: 'Bad JSON' }; }

  const webhookEvent = payload.data && payload.data.webHookEvent;
  const topic        = webhookEvent && webhookEvent.topic;
  const invoiceId    = webhookEvent && webhookEvent.itemId;

  console.log(`Webhook: topic=${topic}, invoiceId=${invoiceId}`);

  if (!topic || !topic.startsWith('INVOICE') || !invoiceId) {
    console.log('Ignoring non-invoice event');
    return { statusCode: 200, body: 'OK - ignored' };
  }

  try {
    // 3. Get fresh Jobber access token via refresh
    const accessToken = await getJobberToken();

    // 4. Fetch invoice from Jobber
    const invoice = await fetchInvoice(accessToken, invoiceId);
    if (!invoice) {
      console.error('Invoice not found:', invoiceId);
      return { statusCode: 200, body: 'OK - invoice not found' };
    }
    console.log('Fetched invoice:', invoice.invoiceNumber);

    // 5. Get Google Drive access token
    const driveToken = await getGoogleToken();

    // 6. Download xlsx
    const rawBytes = await downloadDriveFile(driveToken, CFG.DRIVE_FILE_ID);
    const uint8    = new Uint8Array(rawBytes.length);
    for (let i = 0; i < rawBytes.length; i++) uint8[i] = rawBytes[i] & 0xFF;
    console.log('Downloaded xlsx:', uint8.length, 'bytes');

    // 7. Parse workbook
    const workbook = XLSX.read(uint8, { type: 'array', cellDates: false });
    const ws = workbook.Sheets[CFG.INCOME_SHEET];
    if (!ws) throw new Error(`Sheet "${CFG.INCOME_SHEET}" not found`);

    const aoa = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '', raw: true });
    console.log('Income tab rows:', aoa.length);

    // 8. Build invoice map
    const map = {};
    for (let r = 1; r < aoa.length; r++) {
      const n = String(aoa[r][COL.INVOICE_NO] || '').trim();
      if (n) map[n] = r;
    }

    // 9. Map and reconcile
    const mapped = mapInvoice(invoice);
    const key    = String(invoice.invoiceNumber);
    let action   = 'none';

    if (map.hasOwnProperty(key)) {
      action = reconcileRow(aoa, map[key], mapped);
    } else {
      const lastRow = findLastDataRow(aoa);
      appendRow(aoa, mapped, lastRow);
      action = 'appended';
    }

    console.log(`Invoice ${key}: ${action}`);
    if (action === 'none' || action === 'ok') {
      return { statusCode: 200, body: 'OK - no changes' };
    }

    // 10. Write back to xlsx
    const newWs = XLSX.utils.aoa_to_sheet(aoa, { cellDates: false });
    if (ws['!cols'])   newWs['!cols']   = ws['!cols'];
    if (ws['!rows'])   newWs['!rows']   = ws['!rows'];
    if (ws['!merges']) newWs['!merges'] = ws['!merges'];
    workbook.Sheets[CFG.INCOME_SHEET] = newWs;

    const outArray = XLSX.write(workbook, { type: 'array', bookType: 'xlsx' });
    const outBuf   = Buffer.from(outArray);
    await uploadDriveFile(driveToken, CFG.DRIVE_FILE_ID, outBuf);
    console.log('GL_2026.xlsx updated successfully');

    return { statusCode: 200, body: `OK - ${action}` };

  } catch (err) {
    console.error('Handler error:', err.message);
    return { statusCode: 500, body: 'Internal Server Error' };
  }
};


// ================================================================
// INVOICE MAPPING
// ================================================================
function mapInvoice(inv) {
  const subtotal  = parseFloat((inv.amounts && inv.amounts.subtotal)   || 0);
  const tip       = parseFloat((inv.amounts && inv.amounts.tipsTotal)  || 0);
  const total     = Math.round((subtotal + tip) * 100) / 100;
  const exVat     = Math.round((total / 1.2) * 100) / 100;
  const vat       = Math.round((total - exVat) * 100) / 100;

  const postedDate = inv.createdAt  ? toDate(inv.createdAt)  : '';
  const taxDate    = inv.issuedDate ? toDate(inv.issuedDate) : postedDate;

  let datePaid = 'Not Yet Paid';
  if (inv.receivedDate) datePaid = toDate(inv.receivedDate);

  const frs = datePaid !== 'Not Yet Paid'
    ? Math.round(total * CFG.FRS_RATE * 100) / 100
    : '';

  const client = inv.client || {};
  const counterparty = (
    client.name ||
    `${client.firstName || ''} ${client.lastName || ''}`.trim() ||
    client.companyName || ''
  ).trim();

  const lineNodes  = (inv.lineItems && inv.lineItems.nodes) || [];
  const description = lineNodes
    .map(li => (li.name || li.description || '').trim())
    .filter(Boolean)
    .join(', ') || inv.subject || `Invoice #${inv.invoiceNumber}`;

  let invoiceType = '';
  (inv.customFields || []).forEach(cf => {
    if ((cf.label || '').toLowerCase().includes('invoice type')) {
      invoiceType = cf.valueDropdown || cf.valueText || '';
    }
  });

  const tipNote = tip > 0 ? `Includes tip £${tip.toFixed(2)}` : '';

  return {
    postedDate, taxDate, counterparty, description,
    invoiceNo: String(inv.invoiceNumber),
    total, vat, exVat, frs, tipNote,
    domComm: invoiceType, datePaid,
  };
}

function appendRow(aoa, m, lastDataRow) {
  const newRow    = new Array(20).fill('');
  const newRowIdx = aoa.length + 1; // 1-based Excel row

  newRow[COL.POSTED_DATE]  = m.postedDate;
  newRow[COL.TAX_DATE]     = m.taxDate;
  newRow[COL.COUNTERPARTY] = m.counterparty;
  newRow[COL.DESCRIPTION]  = m.description;
  newRow[COL.ISSUES_VIA]   = 'Jobber Payments';
  newRow[COL.INVOICE_NO]   = m.invoiceNo;
  newRow[COL.TYPE]         = 'SALES';
  newRow[COL.TOTAL]        = m.total;
  newRow[COL.FRS]          = m.frs;
  newRow[COL.NOTES_JOB]    = m.tipNote;
  newRow[COL.DOM_COMM]     = m.domComm;
  newRow[COL.DATE_PAID]    = m.datePaid;

  // Copy formula pattern from last data row
  FORMULA_COLS.forEach(col => {
    const src = aoa[lastDataRow] && aoa[lastDataRow][col];
    if (typeof src === 'string' && src.startsWith('=')) {
      newRow[col] = shiftFormula(src, lastDataRow + 1, newRowIdx);
    }
  });

  aoa.push(newRow);
}

function reconcileRow(aoa, rowIdx, m) {
  const row           = aoa[rowIdx];
  const existingTotal = parseFloat(row[COL.TOTAL] || 0);
  const existingName  = String(row[COL.COUNTERPARTY] || '').trim().toLowerCase();
  const totalOk       = Math.abs(existingTotal - m.total) < 0.02;
  const nameOk        = existingName === m.counterparty.toLowerCase();

  if (!totalOk || !nameOk) {
    if (!String(row[COL.NOTES] || '').includes('⚠️ REVIEW')) {
      aoa[rowIdx][COL.NOTES] =
        `⚠️ REVIEW: Jobber Total=${m.total}, Name="${m.counterparty}" — sheet Total=${existingTotal}, Name="${existingName}"`;
    }
    return 'flagged';
  }

  let changed = false;
  if (m.datePaid !== 'Not Yet Paid' &&
      String(row[COL.DATE_PAID] || '').trim() === 'Not Yet Paid') {
    aoa[rowIdx][COL.DATE_PAID] = m.datePaid;
    aoa[rowIdx][COL.FRS]       = m.frs;
    changed = true;
  }
  if (m.domComm && !String(row[COL.DOM_COMM] || '').trim()) {
    aoa[rowIdx][COL.DOM_COMM] = m.domComm;
    changed = true;
  }
  return changed ? 'updated' : 'ok';
}


// ================================================================
// JOBBER API
// ================================================================
async function getJobberToken() {
  if (!CFG.REFRESH_TOKEN) throw new Error('JOBBER_REFRESH_TOKEN env var not set');

  const body = [
    'client_id='     + encodeURIComponent(CFG.CLIENT_ID),
    'client_secret=' + encodeURIComponent(CFG.CLIENT_SECRET),
    'grant_type=refresh_token',
    'refresh_token=' + encodeURIComponent(CFG.REFRESH_TOKEN),
  ].join('&');

  const resp = await httpsPost('api.getjobber.com', '/api/oauth/token', body, {
    'Content-Type': 'application/x-www-form-urlencoded',
  });

  let data;
  try { data = JSON.parse(resp); }
  catch (e) { throw new Error('Token refresh response not JSON: ' + resp.substring(0, 100)); }

  if (!data.access_token) throw new Error('Token refresh failed: ' + JSON.stringify(data));
  console.log('Jobber token refreshed OK');
  return data.access_token;
}

async function fetchInvoice(token, invoiceId) {
  const query = {
    query: `query {
      invoice(id: "${invoiceId}") {
        invoiceNumber subject createdAt issuedDate receivedDate
        amounts { subtotal tipsTotal }
        client { name firstName lastName companyName }
        lineItems { nodes { name description } }
        customFields {
          ... on CustomFieldText { label valueText }
          ... on CustomFieldDropdown { label valueDropdown }
        }
      }
    }`
  };

  const resp = await httpsPost(
    'api.getjobber.com',
    '/api/graphql',
    JSON.stringify(query),
    {
      'Content-Type':             'application/json',
      'Authorization':            `Bearer ${token}`,
      'X-JOBBER-GRAPHQL-VERSION': CFG.API_VERSION,
    }
  );

  let data;
  try { data = JSON.parse(resp); }
  catch (e) { throw new Error('Jobber API response not JSON'); }

  if (data.errors) console.error('Jobber GraphQL errors:', JSON.stringify(data.errors).substring(0, 200));
  return data.data && data.data.invoice;
}


// ================================================================
// GOOGLE DRIVE
// ================================================================
async function getGoogleToken() {
  const sa  = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
  const now = Math.floor(Date.now() / 1000);
  const claim = {
    iss:   sa.client_email,
    scope: 'https://www.googleapis.com/auth/drive',
    aud:   'https://oauth2.googleapis.com/token',
    exp:   now + 3600,
    iat:   now,
  };
  const jwt  = buildJWT(sa.private_key, claim);
  const body = `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`;
  const resp = await httpsPost('oauth2.googleapis.com', '/token', body, {
    'Content-Type': 'application/x-www-form-urlencoded',
  });
  const data = JSON.parse(resp);
  if (!data.access_token) throw new Error('Google auth failed: ' + resp.substring(0, 100));
  return data.access_token;
}

async function downloadDriveFile(token, fileId) {
  return new Promise((resolve, reject) => {
    https.get({
      hostname: 'www.googleapis.com',
      path:     `/drive/v3/files/${fileId}?alt=media`,
      headers:  { Authorization: `Bearer ${token}` },
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    }).on('error', reject);
  });
}

async function uploadDriveFile(token, fileId, buffer) {
  const mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'www.googleapis.com',
      path:     `/upload/drive/v3/files/${fileId}?uploadType=media`,
      method:   'PATCH',
      headers:  {
        Authorization:    `Bearer ${token}`,
        'Content-Type':   mime,
        'Content-Length': buffer.length,
      },
    }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        if (res.statusCode >= 400) reject(new Error(`Drive upload ${res.statusCode}: ${d}`));
        else resolve(d);
      });
    });
    req.on('error', reject);
    req.write(buffer);
    req.end();
  });
}


// ================================================================
// SIGNATURE VERIFICATION
// ================================================================
function verifyHMAC(body, signature) {
  if (!signature || !CFG.WEBHOOK_SECRET) return false;
  const expected = crypto
    .createHmac('sha256', CFG.WEBHOOK_SECRET)
    .update(body, 'utf8')
    .digest('base64');
  try {
    return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected));
  } catch { return false; }
}


// ================================================================
// JWT (for Google service account)
// ================================================================
function buildJWT(privateKey, claims) {
  const header  = b64u(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64u(JSON.stringify(claims));
  const signing = `${header}.${payload}`;
  const sign    = crypto.createSign('RSA-SHA256');
  sign.update(signing);
  const sig = sign.sign(privateKey, 'base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
  return `${signing}.${sig}`;
}
function b64u(str) {
  return Buffer.from(str).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}


// ================================================================
// HELPERS
// ================================================================
function toDate(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (isNaN(d)) return '';
  return `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}`;
}

function findLastDataRow(aoa) {
  for (let r = aoa.length - 1; r >= 1; r--) {
    if (String(aoa[r][COL.INVOICE_NO] || '').trim()) return r;
  }
  return 1;
}

function shiftFormula(formula, srcRow, newRow) {
  const delta = newRow - srcRow;
  return formula.replace(/([A-Z]+)(\d+)/g, (_, col, row) => col + (parseInt(row) + delta));
}

function httpsPost(hostname, path, body, headers) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname, path, method: 'POST',
      headers: { 'Content-Length': Buffer.byteLength(body), ...headers },
    }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(d));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}
