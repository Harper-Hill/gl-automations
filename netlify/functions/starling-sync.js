// ================================================================
// netlify/functions/starling-sync.js
// Fetches OUT transactions from all Starling spaces
// Writes new expenses to the Expenses tab in the Google Sheet
// Runs daily at 6am (configured in netlify.toml)
// Also callable via HTTP GET for on-demand sync
// ================================================================

'use strict';

const crypto = require('crypto');
const https  = require('https');

const CFG = {
  STARLING_TOKEN: process.env.STARLING_ACCESS_TOKEN,
  SHEET_ID:       process.env.GL_SHEET_ID,
  EXPENSES_TAB:   process.env.GL_EXPENSES_TAB || 'Expenses',
  CONFIG_TAB:     'Config',
  LAST_SYNC_CELL: 'B6', // stores Starling last sync timestamp in Config tab
};

// Expenses tab column indices (0-based, A=0)
const COL = {
  POSTED_DATE:   0,  // A
  TAX_DATE:      1,  // B
  SUPPLIER:      2,  // C
  DESCRIPTION:   3,  // D
  PAYMENT_TYPE:  4,  // E
  OVERHEAD:      5,  // F — leave blank
  SOURCE:        6,  // G — "Starling"
  EXP_CATEGORY:  7,  // H — leave blank
  TAX_TYPE:      8,  // I — leave blank
  CASH_MOVEMENT: 9,  // J — leave blank
  TOTAL:         10, // K
  // L-T left blank for manual entry
};

const NUM_COLS = 20; // A–T

exports.handler = async (event) => {
  // Allow both scheduled (no httpMethod) and HTTP GET
  if (event.httpMethod && event.httpMethod !== 'GET') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  if (!CFG.STARLING_TOKEN) {
    return { statusCode: 500, body: 'STARLING_ACCESS_TOKEN not set' };
  }

  try {
    const googleToken = await getGoogleToken();

    // Read last sync date from Config!B2
    const lastSync = await readConfigCell(googleToken, CFG.LAST_SYNC_CELL);
    const sinceDate = lastSync
      ? new Date(lastSync)
      : new Date(new Date().getFullYear(), 0, 1); // default: start of year

    console.log('Syncing Starling transactions since:', sinceDate.toISOString());

    // Get all accounts
    const accounts = await starlingGet('/api/v2/accounts');
    if (!accounts.accounts || accounts.accounts.length === 0) {
      throw new Error('No Starling accounts found');
    }

    const allTransactions = [];

    for (const account of accounts.accounts) {
      const accountUid    = account.accountUid;
      const defaultCat    = account.defaultCategory;
      const currencyCode  = account.currency || 'GBP';

      // Main account feed
      const mainTxns = await fetchTransactions(accountUid, defaultCat, sinceDate);
      mainTxns.forEach(t => {
        t._spaceName  = 'Main Account';
        t._currency   = currencyCode;
      });
      allTransactions.push(...mainTxns);

      // Savings spaces
      try {
        const spacesResp = await starlingGet(`/api/v2/spaces/${accountUid}/spaces`);
        const spaces = (spacesResp.savingsGoalList || spacesResp.spaces || []);
        for (const space of spaces) {
          const spaceUid  = space.savingsGoalUid || space.spaceUid || space.uid;
          const spaceName = space.name || 'Space';
          if (!spaceUid) continue;
          try {
            const spaceTxns = await fetchTransactions(accountUid, spaceUid, sinceDate);
            spaceTxns.forEach(t => {
              t._spaceName = spaceName;
              t._currency  = currencyCode;
            });
            allTransactions.push(...spaceTxns);
          } catch (e) {
            console.log(`Space ${spaceName} feed error:`, e.message);
          }
        }
      } catch (e) {
        console.log('Spaces fetch error:', e.message);
      }
    }

    // Filter OUT only (expenses)
    const outTxns = allTransactions.filter(t => t.direction === 'OUT');
    console.log(`Found ${outTxns.length} OUT transactions since last sync`);

    if (outTxns.length === 0) {
      await writeConfigCell(googleToken, CFG.LAST_SYNC_CELL, new Date().toISOString());
      return { statusCode: 200, body: 'OK - no new transactions' };
    }

    // Read existing sheet to get existing transaction IDs (col D contains ref/description which includes uid)
    // We store the feedItemUid in Notes col (col S, index 18) for deduplication
    const existingRows = await readSheet(googleToken);
    const existingUids = new Set();
    for (let i = 1; i < existingRows.length; i++) {
      const uid = String(existingRows[i][18] || '').trim(); // col S
      if (uid) existingUids.add(uid);
    }

    // Build new rows
    const newRows = [];
    for (const t of outTxns) {
      if (existingUids.has(t.feedItemUid)) continue; // skip duplicates

      const amount      = t.amount ? (t.amount.minorUnits / 100) : 0;
      const postedDate  = t.transactionTime ? toDate(t.transactionTime) : '';
      const supplier    = t.counterPartyName || '';
      const description = t.reference || t.userNote || t.feedItemUid;
      const paymentType = t.paymentSubtype || t.paymentType || '';
      const space       = t._spaceName || 'Main Account';
      const source      = `Starling (${space})`;

      const row = new Array(NUM_COLS).fill('');
      row[COL.POSTED_DATE]  = postedDate;
      row[COL.TAX_DATE]     = postedDate;
      row[COL.SUPPLIER]     = supplier;
      row[COL.DESCRIPTION]  = description;
      row[COL.PAYMENT_TYPE] = paymentType;
      row[COL.SOURCE]       = source;
      row[COL.TOTAL]        = amount;
      row[18]               = t.feedItemUid; // col S — for deduplication
      newRows.push(row);
    }

    if (newRows.length === 0) {
      console.log('All transactions already in sheet');
      await writeConfigCell(googleToken, CFG.LAST_SYNC_CELL, new Date().toISOString());
      return { statusCode: 200, body: 'OK - all already synced' };
    }

    // Sort by date oldest first
    newRows.sort((a, b) => {
      const da = parseDate(a[COL.POSTED_DATE]);
      const db = parseDate(b[COL.POSTED_DATE]);
      return da - db;
    });

    // Append all new rows
    await appendRows(googleToken, newRows);
    console.log(`Appended ${newRows.length} new expense rows`);

    // Sort the entire sheet by Posted Date
    const gid = process.env.GL_EXPENSES_GID;
    if (gid) {
      const totalRows = existingRows.length + newRows.length;
      await sortSheet(googleToken, parseInt(gid), totalRows);
      console.log('Sheet sorted by Posted Date');
    }

    // Update last sync time
    await writeConfigCell(googleToken, CFG.LAST_SYNC_CELL, new Date().toISOString());

    return {
      statusCode: 200,
      body: JSON.stringify({ synced: newRows.length, message: `${newRows.length} new expenses added` }),
    };

  } catch (err) {
    console.error('Starling sync error:', err.message);
    return { statusCode: 500, body: 'Error: ' + err.message };
  }
};


// ================================================================
// STARLING API
// ================================================================
async function fetchTransactions(accountUid, categoryUid, since) {
  const minDate = since.toISOString();
  const maxDate = new Date().toISOString();
  const path = `/api/v2/feed/account/${accountUid}/category/${categoryUid}/transactions-between`
    + `?minTransactionTimestamp=${encodeURIComponent(minDate)}`
    + `&maxTransactionTimestamp=${encodeURIComponent(maxDate)}`;
  const resp = await starlingGet(path);
  return resp.feedItems || [];
}

async function starlingGet(path) {
  return new Promise((resolve, reject) => {
    https.get({
      hostname: 'api.starlingbank.com',
      path,
      headers: {
        Authorization: 'Bearer ' + CFG.STARLING_TOKEN,
        Accept:        'application/json',
      },
    }, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => {
        try {
          const json = JSON.parse(d);
          if (res.statusCode >= 400) reject(new Error(`Starling ${res.statusCode}: ${d.substring(0, 200)}`));
          else resolve(json);
        } catch (e) { reject(new Error('Starling parse error: ' + d.substring(0, 100))); }
      });
    }).on('error', reject);
  });
}


// ================================================================
// GOOGLE SHEETS
// ================================================================
async function getGoogleToken() {
  const sa  = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
  const now = Math.floor(Date.now() / 1000);
  const claim = {
    iss:   sa.client_email,
    scope: 'https://www.googleapis.com/auth/spreadsheets',
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
  if (!data.access_token) throw new Error('Google auth failed');
  return data.access_token;
}

async function readSheet(token) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.EXPENSES_TAB)}`;
  const resp = await sheetsGet(token, url);
  return resp.values || [];
}

async function readConfigCell(token, cell) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.CONFIG_TAB + '!' + cell)}`;
  const resp = await sheetsGet(token, url);
  return resp.values && resp.values[0] && resp.values[0][0] ? resp.values[0][0].trim() : null;
}

async function writeConfigCell(token, cell, value) {
  const range = `${CFG.CONFIG_TAB}!${cell}`;
  const url   = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=RAW`;
  await sheetsRequest('PUT', token, url, JSON.stringify({ range, values: [[value]] }));
}

async function appendRows(token, rows) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.EXPENSES_TAB)}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`;
  await sheetsRequest('POST', token, url, JSON.stringify({ values: rows }));
}

async function sortSheet(token, sheetId, numRows) {
  const url  = `https://sheets.googleapis.com/v4/spreadsheets/${CFG.SHEET_ID}:batchUpdate`;
  const body = JSON.stringify({
    requests: [{
      sortRange: {
        range: {
          sheetId,
          startRowIndex:    3, // skip header rows (rows 1-3)
          endRowIndex:      numRows,
          startColumnIndex: 0,
          endColumnIndex:   NUM_COLS,
        },
        sortSpecs: [{ dimensionIndex: 0, sortOrder: 'ASCENDING' }],
      },
    }],
  });
  await sheetsRequest('POST', token, url, body);
}

function sheetsGet(token, url) {
  return new Promise((resolve, reject) => {
    const p = new URL(url);
    https.get({ hostname: p.hostname, path: p.pathname + p.search, headers: { Authorization: `Bearer ${token}` } }, (res) => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(new Error('Parse: ' + d.substring(0,100))); } });
    }).on('error', reject);
  });
}

function sheetsRequest(method, token, url, body) {
  return new Promise((resolve, reject) => {
    const p = new URL(url);
    const req = https.request({
      hostname: p.hostname, path: p.pathname + p.search, method,
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, (res) => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => {
        if (res.statusCode >= 400) reject(new Error(`Sheets ${method} ${res.statusCode}: ${d.substring(0,200)}`));
        else { try { resolve(JSON.parse(d)); } catch { resolve({}); } }
      });
    });
    req.on('error', reject); req.write(body); req.end();
  });
}


// ================================================================
// JWT + HELPERS
// ================================================================
function buildJWT(privateKey, claims) {
  const header  = b64u(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64u(JSON.stringify(claims));
  const signing = `${header}.${payload}`;
  const sign    = crypto.createSign('RSA-SHA256'); sign.update(signing);
  return `${signing}.${sign.sign(privateKey, 'base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'')}`;
}
function b64u(s) { return Buffer.from(s).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }

function toDate(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (isNaN(d)) return '';
  return `${pad(d.getDate())}/${pad(d.getMonth()+1)}/${d.getFullYear()}`;
}
function pad(n) { return String(n).padStart(2,'0'); }

function parseDate(str) {
  const m = String(str||'').match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return 0;
  return new Date(parseInt(m[3]), parseInt(m[2])-1, parseInt(m[1])).getTime();
}

function httpsPost(hostname, path, body, headers) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname, path, method: 'POST',
      headers: { 'Content-Length': Buffer.byteLength(body), ...headers },
    }, (res) => { let d = ''; res.on('data', c => d += c); res.on('end', () => resolve(d)); });
    req.on('error', reject); req.write(body); req.end();
  });
}
