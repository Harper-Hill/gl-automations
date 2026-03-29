// ================================================================
// netlify/functions/jobber-invoice-webhook.js
// Receives Jobber INVOICE_CREATE / INVOICE_UPDATE webhooks
// → fetches full invoice → updates GL_2026.xlsx in Google Drive
// ================================================================

'use strict';

const crypto  = require('crypto');
const https   = require('https');
const XLSX    = require('xlsx');

// ── CONFIG (from Netlify environment variables) ───────────────
const CFG = {
  JOBBER_CLIENT_ID:     process.env.JOBBER_GL_CLIENT_ID,
  JOBBER_CLIENT_SECRET: process.env.JOBBER_GL_CLIENT_SECRET,
  JOBBER_WEBHOOK_SECRET:process.env.JOBBER_GL_WEBHOOK_SECRET,
  DRIVE_FILE_ID:        process.env.GL_DRIVE_FILE_ID,
  INCOME_SHEET:         'Income',
  FRS_RATE:             0.12,
  JOBBER_API:           'https://api.getjobber.com/api/graphql',
  API_VERSION:          '2026-03-10',
};

// Income tab column indices (0-based, matches Apps Script)
const COL = {
  POSTED_DATE:  0,  // A — createdAt
  TAX_DATE:     1,  // B — issuedDate
  COUNTERPARTY: 2,  // C
  DESCRIPTION:  3,  // D — line items
  ISSUES_VIA:   4,  // E
  INVOICE_NO:   5,  // F
  WORKING_COL:  6,  // G
  TYPE:         7,  // H
  TOTAL:        8,  // I
  VAT:          9,  // J — formula copied
  EX_VAT:       10, // K — formula copied
  FRS:          11, // L — if paid
  VAT_RETURN:   12, // M — manual
  NOTES_JOB:    13, // N — tip note
  TOTAL_YTD:    14, // O — formula copied
  EX_VAT_LIB:   15, // P — formula copied
  NOTES:        16, // Q — review flags
  DOM_COMM:     17, // R — Invoice Type
  COMM_PCT:     18, // S — formula copied
  DATE_PAID:    19, // T
};

const FORMULA_COLS = [COL.VAT, COL.EX_VAT, COL.TOTAL_YTD, COL.EX_VAT_LIB, COL.COMM_PCT];


// ================================================================
// NETLIFY HANDLER
// ================================================================
exports.handler = async (event) => {
  // Only accept POST
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  // 1. Verify Jobber HMAC signature
  const signature = event.headers['x-jobber-hmac-sha256'] || event.headers['X-Jobber-Hmac-SHA256'];
  if (!verifySignature(event.body, signature)) {
    console.error('Invalid webhook signature');
    return { statusCode: 401, body: 'Unauthorized' };
  }

  // 2. Parse webhook payload
  let payload;
  try {
    payload = JSON.parse(event.body);
  } catch (e) {
    return { statusCode: 400, body: 'Invalid JSON' };
  }

  const webhookEvent = payload.data && payload.data.webHookEvent;
  const topic        = webhookEvent && webhookEvent.topic;
  const invoiceId    = webhookEvent && webhookEvent.itemId;

  console.log(`Received webhook: topic=${topic}, invoiceId=${invoiceId}`);

  // Only handle invoice events
  if (!topic || !topic.startsWith('INVOICE') || !invoiceId) {
    console.log('Ignoring non-invoice webhook');
    return { statusCode: 200, body: 'OK - ignored' };
  }

  try {
    // 3. Get valid Jobber token
    const token = await getValidJobberToken();

    // 4. Fetch full invoice from Jobber
    const invoice = await fetchInvoice(token, invoiceId);
    if (!invoice) {
      console.error('Invoice not found:', invoiceId);
      return { statusCode: 200, body: 'OK - invoice not found' };
    }

    // 5. Get Google Drive access token (service account)
    const driveToken = await getGoogleAccessToken();

    // 6. Download GL_2026.xlsx
    const xlsxBytes = await downloadFile(driveToken, CFG.DRIVE_FILE_ID);

    // 7. Parse workbook
    const workbook = XLSX.read(new Uint8Array(xlsxBytes), { type: 'array', cellDates: false });
    const ws = workbook.Sheets[CFG.INCOME_SHEET];
    if (!ws) throw new Error(`Sheet "${CFG.INCOME_SHEET}" not found`);

    const aoa = XLSX.utils.sheet_to_json(ws, { header: 1, defval: '', raw: true });

    // 8. Build invoice map
    const existingMap = {};
    for (let r = 1; r < aoa.length; r++) {
      const invNo = String(aoa[r][COL.INVOICE_NO] || '').trim();
      if (invNo) existingMap[invNo] = r;
    }

    // 9. Map invoice → row
    const mapped = mapInvoice(invoice);
    const key    = String(invoice.invoiceNumber);
    let action   = 'none';

    if (existingMap.hasOwnProperty(key)) {
      action = reconcileRow(aoa, existingMap[key], mapped);
    } else {
      const lastDataRow = findLastDataRow(aoa);
      appendRow(aoa, mapped, lastDataRow);
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

    const outputArray = XLSX.write(workbook, { type: 'array', bookType: 'xlsx' });
    await uploadFile(driveToken, CFG.DRIVE_FILE_ID, Buffer.from(outputArray));

    console.log(`GL_2026.xlsx updated — invoice ${key} ${action}`);
    return { statusCode: 200, body: `OK - ${action}` };

  } catch (err) {
    console.error('Webhook handler error:', err);
    return { statusCode: 500, body: 'Internal Server Error' };
  }
};


// ================================================================
// FIELD MAPPING
// ================================================================
function mapInvoice(inv) {
  const lineTotal = parseFloat((inv.amounts && inv.amounts.subtotal) || 0);
  const tip       = parseFloat((inv.amounts && inv.amounts.tipsTotal) || 0);
  const total     = Math.round((lineTotal + tip) * 100) / 100;
  const exVat     = Math.round((total / 1.2) * 100) / 100;
  const vat       = Math.round((total - exVat) * 100) / 100;

  const postedDate = inv.createdAt  ? toExcelDate(inv.createdAt)  : '';
  const taxDate    = inv.issuedDate ? toExcelDate(inv.issuedDate) : postedDate;

  let datePaid = 'Not Yet Paid';
  if (inv.receivedDate) datePaid = toExcelDate(inv.receivedDate);

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
  const cfNodes = inv.customFields || [];
  cfNodes.forEach(cf => {
    if ((cf.label || '').toLowerCase().includes('invoice type')) {
      invoiceType = cf.valueText || cf.valueDropdown || '';
    }
  });

  const tipNote = tip > 0 ? `Includes tip £${tip.toFixed(2)}` : '';

  return {
    postedDate, taxDate, counterparty, description,
    invoiceNo: String(inv.invoiceNumber),
    total, vat, exVat, frs, tipNote, domComm: invoiceType, datePaid,
  };
}


// ================================================================
// APPEND / RECONCILE
// ================================================================
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

  // Copy formulas from last data row, shifted to new row
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
        `⚠️ REVIEW: Jobber Total=${m.total}, Name="${m.counterparty}" ` +
        `— sheet Total=${existingTotal}, Name="${existingName}"`;
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

  const data = await jobberPost(token, query);
  return data && data.data && data.data.invoice;
}

async function jobberPost(token, body) {
  const resp = await httpPost('api.getjobber.com', '/api/graphql', body, {
    'Authorization':            `Bearer ${token}`,
    'X-JOBBER-GRAPHQL-VERSION': CFG.API_VERSION,
  });
  if (!resp.ok) {
    console.error('Jobber API error:', resp.status, resp.body.substring(0, 200));
    return null;
  }
  return JSON.parse(resp.body);
}


// ================================================================
// JOBBER OAUTH — tokens stored in Netlify Blobs
// ================================================================
// Tokens stored in Google Drive as tokens.json — avoids Netlify deploy snapshot issue
const TOKENS_FILE_NAME = 'gl-automations-tokens.json';

async function getValidJobberToken() {
  const driveToken = await getGoogleAccessToken();
  let tokens = await readTokensFromDrive(driveToken);

  if (!tokens || !tokens.refresh_token) {
    throw new Error('No tokens found in Drive. Run jobber-auth-init first.');
  }

  // Always refresh to get a fresh access token
  const body = new URLSearchParams({
    client_id:     CFG.JOBBER_CLIENT_ID,
    client_secret: CFG.JOBBER_CLIENT_SECRET,
    grant_type:    'refresh_token',
    refresh_token: tokens.refresh_token,
  });

  const resp = await httpPost('api.getjobber.com', '/api/oauth/token', body.toString(), {
    'Content-Type': 'application/x-www-form-urlencoded',
  }, true);

  const data = JSON.parse(resp.body);
  if (!data.access_token) throw new Error('Token refresh failed: ' + resp.body);

  // Save updated tokens back to Drive
  tokens.access_token  = data.access_token;
  tokens.expires_at    = Date.now() + (data.expires_in || 3600) * 1000;
  if (data.refresh_token) tokens.refresh_token = data.refresh_token;
  await writeTokensToDrive(driveToken, tokens);

  return data.access_token;
}

async function readTokensFromDrive(driveToken) {
  // Search for tokens file by name in Drive
  const searchResp = await new Promise((resolve, reject) => {
    const opts = {
      hostname: 'www.googleapis.com',
      path:     '/drive/v3/files?q=' + encodeURIComponent('name="' + TOKENS_FILE_NAME + '" and trashed=false') + '&fields=files(id)',
      headers:  { Authorization: 'Bearer ' + driveToken },
    };
    https.get(opts, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(JSON.parse(d)));
    }).on('error', reject);
  });

  console.log('Drive search result:', JSON.stringify(searchResp).substring(0, 200));
  const files = searchResp.files || [];
  if (files.length === 0) return null;

  const fileId = files[0].id;
  const content = await downloadFile(driveToken, fileId);
  return JSON.parse(content.toString('utf8'));
}

async function writeTokensToDrive(driveToken, tokens) {
  const json = JSON.stringify(tokens);
  const buf  = Buffer.from(json, 'utf8');

  // Check if file exists
  const searchResp = await new Promise((resolve, reject) => {
    const opts = {
      hostname: 'www.googleapis.com',
      path:     '/drive/v3/files?q=' + encodeURIComponent('name="' + TOKENS_FILE_NAME + '" and trashed=false') + '&fields=files(id)',
      headers:  { Authorization: 'Bearer ' + driveToken },
    };
    https.get(opts, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(JSON.parse(d)));
    }).on('error', reject);
  });

  const files = searchResp.files || [];

  if (files.length > 0) {
    // Update existing file
    await uploadFile(driveToken, files[0].id, buf);
  } else {
    // Create new file
    await new Promise((resolve, reject) => {
      const boundary = 'gl_boundary';
      const meta = JSON.stringify({ name: TOKENS_FILE_NAME, mimeType: 'application/json' });
      const body = Buffer.concat([
        Buffer.from('--' + boundary + '\r\nContent-Type: application/json\r\n\r\n'),
        Buffer.from(meta),
        Buffer.from('\r\n--' + boundary + '\r\nContent-Type: application/json\r\n\r\n'),
        buf,
        Buffer.from('\r\n--' + boundary + '--'),
      ]);
      const opts = {
        hostname: 'www.googleapis.com',
        path:     '/upload/drive/v3/files?uploadType=multipart',
        method:   'POST',
        headers:  {
          Authorization:   'Bearer ' + driveToken,
          'Content-Type':  'multipart/related; boundary=' + boundary,
          'Content-Length': body.length,
        },
      };
      const req = https.request(opts, (res) => {
        let d = '';
        res.on('data', c => d += c);
        res.on('end', () => resolve(d));
      });
      req.on('error', reject);
      req.write(body);
      req.end();
    });
  }
}

async function setNetlifyEnvVar(key, value) {
  const siteId = process.env.NETLIFY_SITE_ID;
  const token  = process.env.NETLIFY_ACCESS_TOKEN;
  if (!siteId || !token) return;
  const patchBody = JSON.stringify([{ value, context: 'all' }]);
  const postBody  = JSON.stringify([{ key, values: [{ value, context: 'all' }] }]);
  const patch = await netlifyApiCall('PATCH', '/api/v1/sites/' + siteId + '/env/' + key, patchBody, token);
  if (patch.status === 404) {
    await netlifyApiCall('POST', '/api/v1/sites/' + siteId + '/env', postBody, token);
  }
}

function netlifyApiCall(method, path, body, token) {
  return new Promise((resolve) => {
    const opts = {
      hostname: 'api.netlify.com', path, method,
      headers: {
        'Authorization':  'Bearer ' + token,
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };
    const req = https.request(opts, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve({ status: res.statusCode, body: d }));
    });
    req.on('error', () => resolve({ status: 500, body: '' }));
    req.write(body);
    req.end();
  });
}


// ================================================================
// GOOGLE DRIVE — service account JWT auth
// ================================================================
async function getGoogleAccessToken() {
  const sa = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);

  const now  = Math.floor(Date.now() / 1000);
  const claim = {
    iss: sa.client_email,
    scope: 'https://www.googleapis.com/auth/drive',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now,
  };

  const jwt  = buildJWT(sa.private_key, claim);
  const body = new URLSearchParams({
    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
    assertion:  jwt,
  });

  const resp = await httpPost('oauth2.googleapis.com', '/token',
    body.toString(), { 'Content-Type': 'application/x-www-form-urlencoded' },
    true
  );

  const data = JSON.parse(resp.body);
  if (!data.access_token) throw new Error('Google auth failed: ' + resp.body);
  return data.access_token;
}

async function downloadFile(token, fileId) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'www.googleapis.com',
      path:     `/drive/v3/files/${fileId}?alt=media`,
      headers:  { Authorization: `Bearer ${token}` },
    };
    https.get(options, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks)));
      res.on('error', reject);
    }).on('error', reject);
  });
}

async function uploadFile(token, fileId, buffer) {
  return new Promise((resolve, reject) => {
    const mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
    const options = {
      hostname: 'www.googleapis.com',
      path:     `/upload/drive/v3/files/${fileId}?uploadType=media`,
      method:   'PATCH',
      headers:  {
        Authorization:  `Bearer ${token}`,
        'Content-Type': mime,
        'Content-Length': buffer.length,
      },
    };
    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        if (res.statusCode >= 400) reject(new Error(`Drive upload ${res.statusCode}: ${body}`));
        else resolve(body);
      });
    });
    req.on('error', reject);
    req.write(buffer);
    req.end();
  });
}


// ================================================================
// JWT BUILDER (RS256) — no external dependency
// ================================================================
function buildJWT(privateKey, claims) {
  const header  = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = base64url(JSON.stringify(claims));
  const signing = `${header}.${payload}`;
  const sign    = crypto.createSign('RSA-SHA256');
  sign.update(signing);
  const sig = sign.sign(privateKey, 'base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
  return `${signing}.${sig}`;
}

function base64url(str) {
  return Buffer.from(str).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}


// ================================================================
// WEBHOOK HMAC VERIFICATION
// ================================================================
function verifySignature(body, signature) {
  if (!signature || !CFG.JOBBER_WEBHOOK_SECRET) return false;
  const expected = crypto
    .createHmac('sha256', CFG.JOBBER_WEBHOOK_SECRET)
    .update(body, 'utf8')
    .digest('base64');
  try {
    return crypto.timingSafeEqual(
      Buffer.from(signature),
      Buffer.from(expected)
    );
  } catch { return false; }
}


// ================================================================
// HELPERS
// ================================================================
function toExcelDate(iso) {
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

async function httpPost(hostname, path, body, extraHeaders = {}, rawBody = false) {
  const bodyStr  = rawBody ? body : JSON.stringify(body);
  const headers  = {
    'Content-Type':   rawBody ? (extraHeaders['Content-Type'] || 'application/x-www-form-urlencoded') : 'application/json',
    'Content-Length': Buffer.byteLength(bodyStr),
    ...extraHeaders,
  };
  if (!rawBody) delete headers['Content-Type']; // will be set below
  if (!rawBody) headers['Content-Type'] = 'application/json';

  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method: 'POST', headers }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ ok: res.statusCode < 400, status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}
