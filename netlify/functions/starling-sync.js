// ================================================================
// netlify/functions/starling-sync.js
// Fetches Starling OUT transactions since last sync
// Applies classification rules at write time
// New rows get light blue fill (needs manual review)
// ================================================================
'use strict';

async function fetchServiceAccount() {
  const { getStore } = require('@netlify/blobs');
  const store = getStore('service-account');
  const raw = await store.get('sa_json');
  if (!raw) throw new Error('SA JSON not found in Netlify Blobs');
  return JSON.parse(raw);
}

const https = require('https');

const CFG = {
  STARLING_TOKEN: process.env.STARLING_ACCESS_TOKEN,
  SHEET_ID:       process.env.GL_SHEET_ID,
  EXPENSES_TAB:   process.env.GL_EXPENSES_TAB || 'Expenses',
  EXPENSES_GID:   parseInt(process.env.GL_EXPENSES_GID, 10),
};

// ── Classification rules ──────────────────────────────────────────
// Applied top-to-bottom, first match wins.
// field: 'supplier' or 'description' (case-insensitive partial match)
// expCat: Expenditure Category (col H)
// taxType: Tax Type (col I) — 'Expense', 'CAPITAL', 'Disallowable', 'Money Transfer'
// vatType: VAT Type (col N) — 'No VAT', 'Reduced VAT', or '' (standard)

const RULES = [
  // ── Wages / Salaries ─────────────────────────────────────────
  { field: 'supplier', match: 'sam barton',          expCat: 'Direct Labour Costs (Salaries)',    taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'supplier', match: 'laure jean',          expCat: 'Direct Labour Costs (Salaries)',    taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'supplier', match: 'alex honnor',         expCat: 'Direct Labour Costs (Salaries)',    taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'supplier', match: 'nadia beele',         expCat: 'Direct Labour Costs (Salaries)',    taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'supplier', match: 'peter barus',         expCat: 'Directors Wages',                   taxType: 'Expense',        vatType: 'No VAT' },

  // ── HMRC / Tax ────────────────────────────────────────────────
  { field: 'supplier', match: 'hmrc',                expCat: 'Money Transfer',                    taxType: 'Money Transfer',  vatType: 'No VAT' },
  { field: 'supplier', match: 'dvla',                expCat: 'Vehicles (Operating) Fixed Costs',  taxType: 'Expense',        vatType: 'No VAT' },

  // ── Vehicles ──────────────────────────────────────────────────
  { field: 'supplier', match: 'fuel card',           expCat: 'Vehicles (Operating) Variable Costs', taxType: 'Expense',     vatType: '' },
  { field: 'supplier', match: 'nadia beele van',     expCat: 'Vehicles (None Van Costs)',          taxType: 'Expense',        vatType: '' },
  { field: 'description', match: 'fuel',             expCat: 'Vehicles (Operating) Variable Costs', taxType: 'Expense',     vatType: '' },
  { field: 'description', match: 'mileage',          expCat: 'Vehicles (Operating) Variable Costs', taxType: 'Expense',     vatType: 'No VAT' },

  // ── Van finance / loan ────────────────────────────────────────
  { field: 'supplier', match: 'van finance',         expCat: 'Loan Interest (Van)',               taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'description', match: 'van finance',      expCat: 'Loan Interest (Van)',               taxType: 'Expense',        vatType: 'No VAT' },

  // ── Capital One (credit card repayments) ─────────────────────
  { field: 'supplier', match: 'capital one',         expCat: 'Money Transfer',                    taxType: 'Money Transfer',  vatType: 'No VAT' },

  // ── Software / IT ─────────────────────────────────────────────
  { field: 'supplier', match: 'wix',                 expCat: 'Software (Operating)',              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'quickbooks',          expCat: 'Software (Operating)',              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'intuit',              expCat: 'Software (Operating)',              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: '123 reg',             expCat: 'Software (Operating)',              taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'supplier', match: 'amazon prime',        expCat: 'Software (Operating)',              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'pdftoexcel',          expCat: 'Software (Operating)',              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'sky mobile',          expCat: 'IT Expenses (Operating)',           taxType: 'Expense',        vatType: '' },
  { field: 'description', match: 'card subscription', expCat: 'Software (Operating)',             taxType: 'Expense',        vatType: '' },

  // ── Insurance ─────────────────────────────────────────────────
  { field: 'description', match: 'insurance',        expCat: 'Insurance',                         taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'supplier', match: 'insurance',           expCat: 'Insurance',                         taxType: 'Expense',        vatType: 'No VAT' },

  // ── Tools / Equipment / Supplies ──────────────────────────────
  { field: 'supplier', match: 'screwfix',            expCat: 'Other (Operating) (Small Tools)',   taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'diamond',             expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'ironmonger',          expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'newhow',              expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'fleetsmart',          expCat: 'Vehicles (Operating) Fixed Costs',  taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'paintnuts',           expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },

  // ── Amazon (general supplies = COGS) ─────────────────────────
  { field: 'supplier', match: 'amazon',              expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'ebay',                expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },

  // ── Property / Rent ───────────────────────────────────────────
  { field: 'supplier', match: 'rent',                expCat: 'Rent',                              taxType: 'Expense',        vatType: 'No VAT' },
  { field: 'supplier', match: 'hm land reg',         expCat: 'Other Property Costs',              taxType: 'Expense',        vatType: '' },

  // ── Marketing ─────────────────────────────────────────────────
  { field: 'supplier', match: 'wix.com',             expCat: 'Marketing & Advertising',           taxType: 'Expense',        vatType: '' },

  // ── Subcontractors / Melvyn Carr ─────────────────────────────
  { field: 'supplier', match: 'melvyn carr',         expCat: 'Subcontractor Payments (CIS)',      taxType: 'Expense',        vatType: 'No VAT' },

  // ── Shopify / Shopscpb ────────────────────────────────────────
  { field: 'supplier', match: 'shopscpb',            expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },
  { field: 'supplier', match: 'sp shops',            expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },

  // ── Doorfittings ─────────────────────────────────────────────
  { field: 'supplier', match: 'doorfittings',        expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },

  // ── Sheffield County Council ──────────────────────────────────
  { field: 'supplier', match: 'sheffield',           expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },

  // ── Sumup / Gecic ─────────────────────────────────────────────
  { field: 'supplier', match: 'sumup',               expCat: 'COGS',                              taxType: 'Expense',        vatType: '' },
];

function applyRules(supplier, description) {
  const s = (supplier || '').toLowerCase();
  const d = (description || '').toLowerCase();
  for (const rule of RULES) {
    const haystack = rule.field === 'supplier' ? s : d;
    if (haystack.includes(rule.match.toLowerCase())) {
      return { expCat: rule.expCat, taxType: rule.taxType, vatType: rule.vatType };
    }
  }
  return { expCat: '', taxType: '', vatType: '' };
}

// ── HTTP helpers ──────────────────────────────────────────────────
function httpsReq(options, body) {
  return new Promise((resolve, reject) => {
    const r = https.request(options, res => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(d) }); }
        catch { resolve({ status: res.statusCode, data: d }); }
      });
    });
    r.on('error', reject);
    if (body) r.write(JSON.stringify(body));
    r.end();
  });
}

function starling(path) {
  return httpsReq({
    hostname: 'api.starlingbank.com',
    path,
    method: 'GET',
    headers: { Authorization: `Bearer ${CFG.STARLING_TOKEN}`, Accept: 'application/json' },
  }).then(r => r.data);
}

function sheetsGet(token, range) {
  return httpsReq({
    hostname: 'sheets.googleapis.com',
    path: `/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(range)}`,
    method: 'GET',
    headers: { Authorization: `Bearer ${token}` },
  }).then(r => r.data);
}

function sheetsPut(token, range, values) {
  return httpsReq({
    hostname: 'sheets.googleapis.com',
    path: `/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=USER_ENTERED`,
    method: 'PUT',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  }, { range, majorDimension: 'ROWS', values });
}

function sheetsAppend(token, rows) {
  return httpsReq({
    hostname: 'sheets.googleapis.com',
    path: `/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.EXPENSES_TAB + '!A:T')}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`,
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  }, { values: rows });
}

function sheetsBatchUpdate(token, requests) {
  return httpsReq({
    hostname: 'sheets.googleapis.com',
    path: `/v4/spreadsheets/${CFG.SHEET_ID}:batchUpdate`,
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  }, { requests });
}

async function getGoogleToken(sa) {
  const now = Math.floor(Date.now() / 1000);
  const header = Buffer.from(JSON.stringify({ alg: 'RS256', typ: 'JWT' })).toString('base64url');
  const payload = Buffer.from(JSON.stringify({
    iss: sa.client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now,
  })).toString('base64url');
  const { createSign } = require('crypto');
  const sig = createSign('RSA-SHA256').update(`${header}.${payload}`).sign(sa.private_key, 'base64url');
  const jwt = `${header}.${payload}.${sig}`;
  const res = await httpsReq({
    hostname: 'oauth2.googleapis.com',
    path: '/token',
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
  }, `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`);
  // Note: body is URL-encoded string not JSON for this one
  return res.data.access_token;
}

// ── Row mapper ────────────────────────────────────────────────────
// Columns A–T (indices 0–19):
// 0  Posted Date  | 1  Tax Date    | 2  Supplier     | 3  Description
// 4  Payment Type | 5  Overhead?   | 6  Source        | 7  Expenditure Category
// 8  Tax Type     | 9  Cash Movement | 10 Total       | 11 VAT
// 12 Ex VAT       | 13 VAT Type    | 14–18 (formula cols, leave blank)
// 19 Transaction ID (dedup)

function mapTx(tx, source) {
  if (tx.direction !== 'OUT') return null;

  const date = tx.transactionTime
    ? new Date(tx.transactionTime).toLocaleDateString('en-GB')
    : '';
  const amount = tx.amount ? (tx.amount.minorUnits / 100).toFixed(2) : '0.00';
  const supplier = tx.counterPartyName || '';
  const description = tx.reference || tx.userNote || '';

  let paymentType = 'Bank Transfer';
  if (tx.source === 'MASTER_CARD')          paymentType = 'Card';
  else if (tx.source === 'DIRECT_DEBIT')    paymentType = 'Direct Debit';
  else if (tx.source === 'STANDING_ORDER')  paymentType = 'Standing Order';
  else if (tx.source === 'ONLINE_PAYMENT')  paymentType = 'Online Payment';

  const { expCat, taxType, vatType } = applyRules(supplier, description);

  const row = new Array(20).fill('');
  row[0]  = date;
  row[1]  = date;
  row[2]  = supplier;
  row[3]  = description;
  row[4]  = paymentType;
  row[5]  = '';          // Overhead? — manual
  row[6]  = source;
  row[7]  = expCat;      // Expenditure Category
  row[8]  = taxType;     // Tax Type
  row[9]  = '';          // Cash Movement — manual
  row[10] = amount;      // Total
  row[11] = '';          // VAT — manual
  row[12] = '';          // Ex VAT — manual
  row[13] = vatType;     // VAT Type
  row[19] = tx.feedItemUid;
  return row;
}

// ── Apply light blue fill to newly appended rows ─────────────────
async function formatNewRows(token, startRow, count) {
  if (!CFG.EXPENSES_GID || count === 0) return;
  // startRow is 0-based (after header row 0). Append adds after existing data.
  // We get the row index from the append response updatedRange.
  const requests = [{
    repeatCell: {
      range: {
        sheetId: CFG.EXPENSES_GID,
        startRowIndex: startRow,
        endRowIndex: startRow + count,
        startColumnIndex: 0,
        endColumnIndex: 20,
      },
      cell: {
        userEnteredFormat: {
          backgroundColor: { red: 0.8, green: 0.906, blue: 1.0 }, // #CCE8FF light blue
        },
      },
      fields: 'userEnteredFormat.backgroundColor',
    },
  }];
  await sheetsBatchUpdate(token, requests);
}

// ── Sort Expenses tab by Posted Date (col A) ──────────────────────
async function sortExpenses(token) {
  if (!CFG.EXPENSES_GID) return;
  await sheetsBatchUpdate(token, [{
    sortRange: {
      range: { sheetId: CFG.EXPENSES_GID, startRowIndex: 1, startColumnIndex: 0, endColumnIndex: 20 },
      sortSpecs: [{ dimensionIndex: 0, sortOrder: 'ASCENDING' }],
    },
  }]);
}

// ── Main handler ──────────────────────────────────────────────────
exports.handler = async (event) => {
  if (event.httpMethod && event.httpMethod !== 'GET') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  try {
    // 1. Auth
    const sa = await fetchServiceAccount();
    const gToken = await getGoogleToken(sa);

    // 2. Last sync time from Config!B2
    const cfgRes = await sheetsGet(gToken, 'Config!B2');
    const since = (cfgRes.values && cfgRes.values[0] && cfgRes.values[0][0])
      || new Date(new Date().getFullYear(), 0, 1).toISOString();
    console.log('Syncing since:', since);

    // 3. Existing transaction IDs (col T) for dedup
    const idsRes = await sheetsGet(gToken, CFG.EXPENSES_TAB + '!T:T');
    const existingIds = new Set((idsRes.values || []).flat().filter(Boolean));

    // 4. Fetch all Starling accounts + spaces
    const { accounts = [] } = await starling('/api/v2/accounts');
    if (!accounts.length) throw new Error('No Starling accounts found');

    const syncTime = new Date().toISOString();
    const newRows = [];

    for (const { accountUid, defaultCategory } of accounts) {
      const { feedItems: txs = [] } = await starling(
        `/api/v2/feed/account/${accountUid}/category/${defaultCategory}?changesSince=${encodeURIComponent(since)}`
      );
      for (const tx of txs) {
        if (existingIds.has(tx.feedItemUid)) continue;
        const row = mapTx(tx, 'Starling');
        if (row) newRows.push(row);
      }

      const { savingsGoals: spaces = [] } = await starling(`/api/v2/account/${accountUid}/spaces`);
      for (const sp of spaces) {
        if (!sp.savedObjectUid) continue;
        const { feedItems: stxs = [] } = await starling(
          `/api/v2/feed/account/${accountUid}/category/${sp.savedObjectUid}?changesSince=${encodeURIComponent(since)}`
        );
        for (const tx of stxs) {
          if (existingIds.has(tx.feedItemUid)) continue;
          const row = mapTx(tx, `Starling (${sp.name})`);
          if (row) newRows.push(row);
        }
      }
    }

    console.log(`Found ${newRows.length} new expense rows`);

    // 5. Append + format + sort
    if (newRows.length > 0) {
      const appendRes = await sheetsAppend(gToken, newRows);

      // Parse the first appended row index from the updatedRange e.g. "Expenses!A45:T47"
      let firstNewRow = null;
      try {
        const updatedRange = appendRes.data && appendRes.data.updates && appendRes.data.updates.updatedRange;
        if (updatedRange) {
          const match = updatedRange.match(/!A(\d+):/);
          if (match) firstNewRow = parseInt(match[1], 10) - 1; // convert to 0-based
        }
      } catch {}

      if (firstNewRow !== null) {
        await formatNewRows(gToken, firstNewRow, newRows.length);
      }

      await sortExpenses(gToken);
    }

    // 6. Update last sync time
    await sheetsPut(gToken, 'Config!B2', [[syncTime]]);

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, newRows: newRows.length, since, syncTime }),
    };

  } catch (err) {
    console.error('Starling sync error:', err);
    return { statusCode: 500, body: JSON.stringify({ ok: false, error: err.message }) };
  }
};
