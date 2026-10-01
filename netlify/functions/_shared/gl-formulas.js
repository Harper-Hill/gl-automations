'use strict';
// Shared helpers for keeping GL 2026 formula columns correct.
//
// Why: rows are appended and then the tab is sorted by date. Any formula
// written with a hard-coded row number (e.g. "=I591/6") points at the wrong
// row once the sort moves it. Instead we (re)write the formula columns for the
// whole data range with repeatCell after every append + sort. repeatCell
// shifts relative references row by row, so every row ends up self-referencing.

const https = require('https');

function batchUpdate(token, sheetId, requests) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({ requests });
    const req = https.request({
      hostname: 'sheets.googleapis.com',
      path: `/v4/spreadsheets/${sheetId}:batchUpdate`,
      method: 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, res => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => res.statusCode >= 400 ? reject(new Error(`formula refresh ${res.statusCode}: ${d.slice(0, 200)}`)) : resolve(d));
    });
    req.on('error', reject); req.write(body); req.end();
  });
}

function repeat(gid, firstRow, lastRow, col, formula) {
  return { repeatCell: {
    range: { sheetId: gid, startRowIndex: firstRow - 1, endRowIndex: lastRow, startColumnIndex: col, endColumnIndex: col + 1 },
    cell: { userEnteredValue: { formulaValue: formula } },
    fields: 'userEnteredValue',
  } };
}

// Expenses tab: L = VAT, M = Ex VAT, O/P = running totals, Q = cashflow.
// lastRow = last sheet row (1-based) that holds data.
async function refreshExpenseFormulas(token, spreadsheetId, gid, lastRow) {
  if (!gid || !lastRow || lastRow < 2) return;
  await batchUpdate(token, spreadsheetId, [
    repeat(gid, 2, lastRow, 11, '=IF(N2="Standard",ROUND(K2/6,2),IF(N2="Reduced VAT",ROUND(K2*5/105,2),0))'),
    repeat(gid, 2, lastRow, 12, '=K2-L2'),
    repeat(gid, 2, lastRow, 14, '=SUMIFS($K$2:K2,$I$2:I2,"EXPENSE",$J$2:J2,"<>*No*")'),
    repeat(gid, 2, lastRow, 15, '=SUMIF($J$2:J2,"<>*No*",$K$2:K2)'),
    repeat(gid, 2, lastRow, 16, '=VLOOKUP(A2,Income!A:P,16,1)-P2'),
  ]);
}

// Income tab: J = VAT, K = Ex VAT, L = Flat Rate liability (blank until paid),
// O/P = running totals, S = commercial % of sales.
async function refreshIncomeFormulas(token, spreadsheetId, gid, lastRow) {
  if (!gid || !lastRow || lastRow < 2) return;
  await batchUpdate(token, spreadsheetId, [
    repeat(gid, 2, lastRow, 9,  '=ROUND(I2/6,2)'),
    repeat(gid, 2, lastRow, 10, '=I2-J2'),
    repeat(gid, 2, lastRow, 11, '=IF(OR(T2="",T2="Not Yet Paid"),"",ROUND(I2*0.12,2))'),
    repeat(gid, 2, lastRow, 14, '=SUMIF(H$2:H2,"SALES",I$2:I2)'),
    repeat(gid, 2, lastRow, 15, '=SUMIF(H$2:H2,"SALES",I$2:I2)-SUMIF(H$2:H2,"SALES",L$2:L2)'),
    repeat(gid, 2, lastRow, 18, '=IF(R2="Commercial",IFERROR(I2/O2,0),0)'),
  ]);
}

// UK calendar date (dd/mm/yyyy) for a timestamp. Netlify runs in UTC, so a
// plain toLocaleDateString() puts anything paid between 00:00 and 01:00 BST on
// the previous day (e.g. a 1 July direct debit landing on 30 June).
function ukDate(input) {
  const d = input instanceof Date ? input : new Date(input);
  if (isNaN(d)) return '';
  return d.toLocaleDateString('en-GB', { timeZone: 'Europe/London' });
}

module.exports = { refreshExpenseFormulas, refreshIncomeFormulas, ukDate };
