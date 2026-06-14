// ================================================================
// netlify/functions/starling-sync.js
// Fetches Starling OUT transactions since last sync
// Applies classification rules at write time
// New rows get light blue fill (needs manual review)
// ================================================================
'use strict';
const DRY_RUN = false;   // ← logs intended writes, touches no workbook. Set false to go live.
const SALARY_SOURCE = 'starling';        // col G must equal this (real bank payment, not forecast)
const WAGE_RE    = /^\s*wage/i;          // description starts "Wage" (incl. "Wage Correction") → M
const MILEAGE_RE = /mile/i;              // description contains "mile" → N

async function fetchServiceAccount() {
  try {
    const { getStore } = require("@netlify/blobs");
    const store = getStore({ name: "service-account", siteID: process.env.NETLIFY_SITE_ID, token: process.env.NETLIFY_ACCESS_TOKEN });
    const raw = await store.get('sa_json');
    if (!raw) throw new Error("SA JSON not found in Netlify Blobs");
    return JSON.parse(raw);
  } catch(e) {
    console.error('fetchServiceAccount failed:', e.message);
    throw e;
  }
}

const https = require('https');

const CFG = {
  STARLING_TOKEN: process.env.STARLING_ACCESS_TOKEN,
  SHEET_ID:       process.env.GL_SHEET_ID,
  EXPENSES_TAB:   process.env.GL_EXPENSES_TAB || 'Expenses',
  EXPENSES_GID:   parseInt(process.env.GL_EXPENSES_GID, 10),
  STAFF_CONFIG_TAB: process.env.GL_STAFF_CONFIG_TAB || 'Staff Config',
  SYNC_LOG_TAB:     process.env.GL_SYNC_LOG_TAB || 'Sync Log',
};

// ── Classification rules (loaded from Sync Rules sheet tab) ──────
// Rules tab columns: A=Keyword, B=Match Field, C=Expenditure Category,
//                    D=Tax Type, E=VAT Type
// Applied top-to-bottom, first match wins.

const SYNC_RULES_TAB = 'Sync Rules';

async function loadRules(token) {
  // Columns: KW1|Field1|KW2|Field2|KW3|Field3|KW4|Field4|ExpCat|TaxType|VATType
  const res = await sheetsGet(token, SYNC_RULES_TAB + '!A2:K1000');
  const rows = res.values || [];
  return rows
    .filter(r => r[0] && r[1]) // must have at least keyword 1 and field 1
    .map(r => ({
      pairs: [
        { kw: (r[0]||'').toLowerCase(), field: (r[1]||'').toLowerCase() },
        { kw: (r[2]||'').toLowerCase(), field: (r[3]||'').toLowerCase() },
        { kw: (r[4]||'').toLowerCase(), field: (r[5]||'').toLowerCase() },
        { kw: (r[6]||'').toLowerCase(), field: (r[7]||'').toLowerCase() },
      ].filter(p => p.kw && p.field),
      expCat:  r[8]  || '',
      taxType: r[9]  || '',
      vatType: r[10] || '',
    }));
}

function applyRules(rules, supplier, description) {
  const s = (supplier || '').toLowerCase();
  const d = (description || '').toLowerCase();
  for (const rule of rules) {
    // All keyword/field pairs must match (AND logic)
    const allMatch = rule.pairs.every(p => {
      const haystack = p.field === 'supplier' ? s : d;
      return haystack.includes(p.kw);
    });
    if (allMatch) {
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
    if (body) r.write(typeof body === "string" ? body : JSON.stringify(body));
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
// Sum of main account effectiveBalance + all spaces' totalSaved, returned in £
async function fetchTotalBalance() {
  const { accounts = [] } = await starling('/api/v2/accounts');
  let pence = 0;
  for (const { accountUid } of accounts) {
    const bal = await starling(`/api/v2/accounts/${accountUid}/balance`);
    pence += (bal.effectiveBalance && bal.effectiveBalance.minorUnits) || 0;
    const { savingsGoals = [] } = await starling(`/api/v2/account/${accountUid}/spaces`);
    for (const sg of savingsGoals) {
      pence += (sg.totalSaved && sg.totalSaved.minorUnits) || 0;
    }
  }
  return pence / 100;
}

// UK-local month/year + Cash At Bank row (Jan=86 ... Dec=97) for a given Date
function monthRowFromDate(d) {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', month: 'numeric', year: 'numeric'
  }).formatToParts(d);
  const year = +parts.find(p => p.type === 'year').value;
  const month = +parts.find(p => p.type === 'month').value;
  return { year, month, row: 85 + month };
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

// ── Cross-workbook helpers (take an explicit spreadsheet ID) ─────
function wbGet(token, sheetId, range) {
  return httpsReq({
    hostname: 'sheets.googleapis.com',
    path: `/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(range)}`,
    method: 'GET',
    headers: { Authorization: `Bearer ${token}` },
  }).then(r => r.data);
}

function wbPut(token, sheetId, range, values) {
  return httpsReq({
    hostname: 'sheets.googleapis.com',
    path: `/v4/spreadsheets/${sheetId}/values/${encodeURIComponent(range)}?valueInputOption=USER_ENTERED`,
    method: 'PUT',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
  }, { range, majorDimension: 'ROWS', values });
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

function mapTx(tx, source, rules) {
  if (tx.direction !== 'OUT') return null;
  // Filter out internal transfers between Starling spaces/categories
  if (tx.source === 'INTERNAL_TRANSFER') return null;
  if (tx.status === 'DECLINED') return null;

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

  const { expCat, taxType, vatType } = applyRules(rules, supplier, description);

  const row = new Array(20).fill('');
  row[0]  = date;
  row[1]  = date;
  row[2]  = supplier;
  row[3]  = description;
  row[4]  = paymentType;
  row[5]  = '-';         // Overhead?
  row[6]  = source;
  row[7]  = expCat;      // Expenditure Category
  row[8]  = taxType;     // Tax Type
  row[9]  = 'Yes';       // Cash Movement — always Yes for Starling
  row[10] = amount;      // Total
  // VAT calculations based on VAT Type
  const amt = parseFloat(amount);
  let vat = '', exVat = '';
  if (vatType === 'No VAT') {
    vat = '0.00';
    exVat = amount;
  } else if (vatType === 'Standard') {
    vat = (amt / 6).toFixed(2);
    exVat = (amt / 1.2).toFixed(2);
  } else if (vatType === 'Reduced VAT') {
    vat = (amt * 5 / 105).toFixed(2);
    exVat = (amt / 1.05).toFixed(2);
  }
  row[11] = vat;         // VAT
  row[12] = exVat;       // Ex VAT
  row[13] = vatType;     // VAT Type
  row[17] = '-';         // R - Notes (default)
  row[18] = tx.status === 'PENDING' ? 'PENDING' : '-';  // S - pending flag
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
async function getSheetRowCount(token) {
  const res = await httpsReq({
    hostname: 'sheets.googleapis.com',
    path: `/v4/spreadsheets/${CFG.SHEET_ID}?fields=sheets.properties`,
    method: 'GET',
    headers: { Authorization: `Bearer ${token}` },
  });
  const sheets = res.data.sheets || [];
  const sheet = sheets.find(s => s.properties.sheetId === CFG.EXPENSES_GID);
  return sheet ? sheet.properties.gridProperties.rowCount : 10000;
}

async function sortExpenses(token) {
  if (!CFG.EXPENSES_GID) { console.log('sortExpenses: no GID, skipping'); return; }
  const rowCount = await getSheetRowCount(token);
  console.log('sortExpenses: starting, sheetId=' + CFG.EXPENSES_GID + ' type=' + typeof CFG.EXPENSES_GID + ' rowCount=' + rowCount);
  const res = await sheetsBatchUpdate(token, [{
    sortRange: {
      range: { sheetId: CFG.EXPENSES_GID, startRowIndex: 3, endRowIndex: rowCount, startColumnIndex: 0, endColumnIndex: 20 },
      sortSpecs: [{ dimensionIndex: 0, sortOrder: 'ASCENDING' }],
    },
  }]);
  console.log('sortExpenses: done, status=' + (res && res.status) + ' body=' + JSON.stringify(res && res.data));
}

// ── Recheck pending transactions ─────────────────────────────────
async function recheckPending(token) {
  // Read all rows from Expenses tab
  const res = await sheetsGet(token, CFG.EXPENSES_TAB + '!A:T');
  const rows = res.values || [];
  const updates = [];

  for (let i = 3; i < rows.length; i++) { // skip header rows (0-based, row 4 = index 3)
    const row = rows[i];
    const statusCell = row[18] || ''; // col S
    const txId = row[19] || '';       // col T
    if (statusCell !== 'PENDING' || !txId) continue;

    // Re-fetch transaction from Starling by feedItemUid
    try {
      const { accounts = [] } = await starling('/api/v2/accounts');
      for (const { accountUid, defaultCategory } of accounts) {
        const txRes = await starling(`/api/v2/feed/account/${accountUid}/category/${defaultCategory}/feed-items/${txId}`);
        if (txRes && txRes.feedItemUid) {
          if (txRes.status === 'SETTLED') {
            const newAmount = txRes.amount ? (txRes.amount.minorUnits / 100).toFixed(2) : row[10];
            updates.push({
              rowIndex: i + 1, // 1-based sheet row
              amount: newAmount,
            });
            console.log(`recheckPending: ${txId} now SETTLED, amount=${newAmount}`);
          }
          break;
        }
      }
    } catch(e) {
      console.log(`recheckPending: error fetching ${txId}: ${e.message}`);
    }
  }

  // Apply updates
  for (const u of updates) {
    // Clear PENDING flag in col S, update amount in col K
    await sheetsPut(token, `${CFG.EXPENSES_TAB}!K${u.rowIndex}`, [[u.amount]]);
    await sheetsPut(token, `${CFG.EXPENSES_TAB}!S${u.rowIndex}`, [['']]);
  }

  console.log(`recheckPending: ${updates.length} transactions updated`);
}

// ════════════════════════════════════════════════════════════════
// Push employee Wage/Mileage payments into Salary Workings workbooks
//   Wage    → column M   Mileage → column N
//   Authoritative recompute: for each (employee, date) we re-sum ALL
//   matching Expenses rows and write that total. Idempotent on re-run,
//   sums same-day multiples, self-heals on corrections.
//   Writes M/N even on Approved rows (only timesheet cols are locked).
// ════════════════════════════════════════════════════════════════
async function pushSalaryPayments(token) {
  // 3a. Load Staff Config → map Starling payee alias → { wbId, tab, name }
  const cfg = await sheetsGet(token, `${CFG.STAFF_CONFIG_TAB}!A2:H1000`);
  const staff = [];
  for (const r of (cfg.values || [])) {
    const wbId = (r[2] || '').trim();      // C = Workbook ID
    const tab  = (r[3] || '2026').trim();  // D = Workbook Tab
    const active = String(r[5] || '').trim().toUpperCase() === 'TRUE'; // F
    const payees = (r[7] || '').trim();    // H = Starling Payee (comma-sep aliases)
    if (!wbId || !active || !payees) continue;
    for (const alias of payees.split(',').map(s => s.trim().toLowerCase()).filter(Boolean)) {
      staff.push({ alias, wbId, tab, name: (r[1] || r[0] || '').trim() });
    }
  }
  if (!staff.length) { console.log('pushSalaryPayments: no active staff with payees'); return; }

  // 3b. Read full Expenses tab, collect employee wage/mileage payments
  const exp = await sheetsGet(token, `${CFG.EXPENSES_TAB}!A:T`);
  const rows = exp.values || [];
  // affected[wbId|tab|date] = { wbId, tab, name, date, wage, mileage }
  const affected = {};
  for (const row of rows) {
    const date     = (row[0] || '').trim();              // A dd/mm/yyyy
    const supplier = (row[2] || '').trim().toLowerCase();// C
    const desc     = (row[3] || '');                     // D
    const source   = (row[6] || '').trim().toLowerCase();// G
    const amount   = parseFloat(String(row[10] || '0').replace(/,/g, '')); // K — strip thousands separators

    if (!date || !amount) continue;
    if (source !== SALARY_SOURCE) continue;              // skip forecast rows (no bank source)

    const match = staff.find(s => supplier.includes(s.alias));
    if (!match) continue;

    const isWage    = WAGE_RE.test(desc);
    const isMileage = MILEAGE_RE.test(desc);
    if (!isWage && !isMileage) continue;

    const key = `${match.wbId}|${match.tab}|${date}`;
    if (!affected[key]) affected[key] = { wbId: match.wbId, tab: match.tab, name: match.name, date, wage: 0, mileage: 0 };
    if (isWage)    affected[key].wage    += amount;
    if (isMileage) affected[key].mileage += amount;
  }

  const keys = Object.keys(affected);
  if (!keys.length) { console.log('pushSalaryPayments: no matching payments'); return; }

  // 3c. For each affected workbook, map dates → row numbers (col A), write M/N
  const byWb = {};
  for (const k of keys) { const a = affected[k]; (byWb[a.wbId + '|' + a.tab] ||= []).push(a); }

  for (const grp of Object.keys(byWb)) {
    const [wbId, tab] = grp.split('|');
    let dateCol;
    try {
      dateCol = await wbGet(token, wbId, `${tab}!A1:A2000`);
    } catch (e) {
      console.error(`pushSalaryPayments: cannot read ${wbId} (${tab}) — ${e.message}`); continue;
    }
    const colA = (dateCol.values || []).map(r => (r[0] || '').trim());
    // Build dd/mm/yyyy → 1-based row index (sheet rows). Col A holds long-form
    // dates like "Monday, 1 June 2026"; normalise both sides to a comparable key.
    const rowFor = {};
    colA.forEach((cell, i) => {
      const norm = normaliseDate(cell);
      if (norm) rowFor[norm] = i + 1;
    });

    for (const a of byWb[grp]) {
      const norm = normaliseDate(a.date);
      const rowNum = rowFor[norm];
      if (!rowNum) { console.warn(`  ${a.name}: no row for ${a.date} (${norm}) — skipped`); continue; }

      const writes = [];
      if (a.wage > 0)    writes.push({ range: `${tab}!M${rowNum}`, val: a.wage });
      if (a.mileage > 0) writes.push({ range: `${tab}!M${rowNum}`, val: a.mileage });

      for (const w of writes) {
        if (DRY_RUN) {
          console.log(`  [DRY] ${a.name} ${a.date} → ${w.range} = ${w.val.toFixed(2)}`);
        } else {
          await wbPut(token, wbId, w.range, [[w.val.toFixed(2)]]);
          console.log(`  ${a.name} ${a.date} → ${w.range} = ${w.val.toFixed(2)}`);
          await logSync(token, a.name, a.date, w.range.includes('M') ? 'wage' : 'mileage', w.val);
        }
      }
    }
  }
}

// Normalise either "Monday, 1 June 2026" or "01/06/2026" → "2026-06-01"
function normaliseDate(s) {
  if (!s) return null;
  s = String(s).trim();
  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);  // dd/mm/yyyy
  if (m) return `${m[3]}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`;
  const months = { january:'01',february:'02',march:'03',april:'04',may:'05',june:'06',july:'07',august:'08',september:'09',october:'10',november:'11',december:'12' };
  m = s.match(/(\d{1,2})\s+([a-z]+)\s+(\d{4})/i);       // "1 June 2026"
  if (m) { const mo = months[m[2].toLowerCase()]; if (mo) return `${m[3]}-${mo}-${m[1].padStart(2,'0')}`; }
  return null;
}

async function logSync(token, name, date, kind, val) {
  try {
    await httpsReq({
      hostname: 'sheets.googleapis.com',
      path: `/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.SYNC_LOG_TAB + '!A:F')}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`,
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    }, { values: [[new Date().toISOString(), name, date, `salary-payment:${kind}`, val.toFixed(2), 'starling-sync']] });
  } catch (e) { console.error('logSync failed:', e.message); }
}

// ── Main handler ──────────────────────────────────────────────────
exports.handler = async (event) => {

  try {
      // 1. Auth
    const sa = await fetchServiceAccount();
    const gToken = await getGoogleToken(sa);

    // 2. Last sync time from Config!B2
    const cfgRes = await sheetsGet(gToken, 'Config!B2');
    const since = (cfgRes.values && cfgRes.values[0] && cfgRes.values[0][0])
      || new Date(new Date().getFullYear(), 0, 1).toISOString();
    console.log('Syncing since:', since);

    // 2b. Load classification rules from sheet
    const rules = await loadRules(gToken);
    console.log(`Loaded ${rules.length} classification rules`);

    // 2c. Recheck any pending transactions from previous syncs
    await recheckPending(gToken);

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
        const row = mapTx(tx, 'Starling', rules);
        if (row) newRows.push(row);
      }

      const { savingsGoals: spaces = [] } = await starling(`/api/v2/account/${accountUid}/spaces`);
      for (const sp of spaces) {
        if (!sp.savingsGoalUid) continue;
        const { feedItems: stxs = [] } = await starling(
          `/api/v2/feed/account/${accountUid}/category/${sp.savingsGoalUid}?changesSince=${encodeURIComponent(since)}`
        );
        for (const tx of stxs) {
          if (existingIds.has(tx.feedItemUid)) continue;
          if (newRows.length < 3) console.log("space tx: source=" + tx.source + " cpType=" + tx.counterPartyType + " cpName=" + tx.counterPartyName + " dir=" + tx.direction);
          const row = mapTx(tx, `Starling (${sp.name})`, rules);
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

    // 5a. Push employee Wage/Mileage payments into Salary Workings workbooks
    try {
      await pushSalaryPayments(gToken);
    } catch (e) {
      console.error('pushSalaryPayments failed:', e.message); // non-fatal
    }

    // 5b. Update Cash At Bank balance on Overhead Calcs
    try {
      const balance = await fetchTotalBalance();
      const sinceMonth = monthRowFromDate(new Date(since));
      const nowMonth = monthRowFromDate(new Date());

      // If UK month rolled over since last sync, freeze the previous month
      if (sinceMonth.year === 2026 && nowMonth.year === 2026
          && sinceMonth.month !== nowMonth.month) {
        await sheetsPut(gToken, `Overhead Calcs!C${sinceMonth.row}`, [[balance]]);
        console.log(`Cash At Bank: froze month ${sinceMonth.month} → C${sinceMonth.row} at £${balance}`);
      }

      // Update current month
      if (nowMonth.year === 2026) {
        await sheetsPut(gToken, `Overhead Calcs!C${nowMonth.row}`, [[balance]]);
        console.log(`Cash At Bank: £${balance} → C${nowMonth.row}`);
      } else {
        console.warn(`Cash At Bank: skipping — outside 2026 (got ${nowMonth.year}). Set up new sheet.`);
      }
    } catch(e) {
      console.error('Balance update failed:', e.message);
      // Non-fatal — don't break the sync if this fails
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
