// ================================================================
// netlify/functions/starling-sync.js
// Scheduled daily at 6am + callable manually via GET
// Fetches Starling Bank outgoing transactions since last sync
// → writes new expense rows to GL Google Sheet
// ================================================================
'use strict';
const https = require('https');
const { createSign } = require('crypto');

const CFG = {
  STARLING_TOKEN: process.env.STARLING_ACCESS_TOKEN,
  SHEET_ID:       process.env.GL_SHEET_ID,
  EXPENSES_TAB:   process.env.GL_EXPENSES_TAB || 'Expenses',
  EXPENSES_GID:   process.env.GL_EXPENSES_GID,
  SA_FETCH_URL:   process.env.SA_FETCH_URL,
  SA_FETCH_TOKEN: process.env.SA_FETCH_TOKEN,
};

async function fetchServiceAccount() {
  const url = CFG.SA_FETCH_URL + '?token=' + CFG.SA_FETCH_TOKEN;
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(new Error('SA parse: ' + d.substring(0,100))); } });
    }).on('error', reject);
  });
}

function req(options, body) {
  return new Promise((resolve, reject) => {
    const r = https.request(options, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => { try { resolve({ status: res.statusCode, data: JSON.parse(d) }); } catch(e) { resolve({ status: res.statusCode, data: d }); } });
    });
    r.on('error', reject);
    if (body) r.write(typeof body === 'string' ? body : JSON.stringify(body));
    r.end();
  });
}

function b64u(s) { return Buffer.from(s).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }

async function getGoogleToken(sa) {
  const now = Math.floor(Date.now()/1000);
  const claim = { iss: sa.client_email, scope: 'https://www.googleapis.com/auth/spreadsheets', aud: 'https://oauth2.googleapis.com/token', exp: now+3600, iat: now };
  const h = b64u(JSON.stringify({alg:'RS256',typ:'JWT'}));
  const p = b64u(JSON.stringify(claim));
  const sig = createSign('RSA-SHA256').update(`${h}.${p}`).sign(sa.private_key,'base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
  const jwt = `${h}.${p}.${sig}`;
  const body = `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`;
  const res = await req({ hostname:'oauth2.googleapis.com', path:'/token', method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'} }, body);
  if (!res.data.access_token) throw new Error('Google token error: ' + JSON.stringify(res.data));
  return res.data.access_token;
}

async function sheetGet(token, range) {
  const res = await req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(range)}`, method:'GET', headers:{Authorization:`Bearer ${token}`} });
  return res.data;
}

async function sheetPut(token, range, values) {
  return req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(range)}?valueInputOption=RAW`, method:'PUT', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'} }, { values });
}

async function sheetAppend(token, values) {
  const range = encodeURIComponent(CFG.EXPENSES_TAB + '!A:T');
  return req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/${range}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`, method:'POST', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'} }, { values });
}

async function sortSheet(token) {
  return req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}:batchUpdate`, method:'POST', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'} },
    { requests:[{ sortRange:{ range:{ sheetId:parseInt(CFG.EXPENSES_GID,10), startRowIndex:1, startColumnIndex:0, endColumnIndex:20 }, sortSpecs:[{ dimensionIndex:0, sortOrder:'ASCENDING' }] } }] });
}

async function starling(path) {
  const res = await req({ hostname:'api.starlingbank.com', path, method:'GET', headers:{ Authorization:`Bearer ${CFG.STARLING_TOKEN}`, Accept:'application/json' } });
  return res.data;
}

function mapTx(tx, source) {
  if (tx.direction !== 'OUT') return null;
  const d = tx.transactionTime ? new Date(tx.transactionTime).toLocaleDateString('en-GB') : '';
  const amt = tx.amount ? (tx.amount.minorUnits/100).toFixed(2) : '0.00';
  const cpty = tx.counterPartyName || tx.reference || '';
  const desc = tx.reference || tx.userNote || '';
  let ptype = 'Bank Transfer';
  if (tx.source === 'MASTER_CARD') ptype = 'Card';
  else if (tx.source === 'DIRECT_DEBIT') ptype = 'Direct Debit';
  else if (tx.source === 'STANDING_ORDER') ptype = 'Standing Order';
  const row = new Array(20).fill('');
  row[0]=d; row[1]=d; row[2]=cpty; row[3]=desc; row[4]=ptype; row[6]=source; row[10]=amt; row[19]=tx.feedItemUid;
  return row;
}

exports.handler = async () => {
  try {
    const sa = await fetchServiceAccount();
    const gToken = await getGoogleToken(sa);

    const cfgData = await sheetGet(gToken, 'Config!B2');
    const since = (cfgData.values && cfgData.values[0] && cfgData.values[0][0]) || new Date(new Date().getFullYear(),0,1).toISOString();
    console.log('Syncing since:', since);

    const idsData = await sheetGet(gToken, CFG.EXPENSES_TAB + '!T:T');
    const existingIds = new Set((idsData.values || []).flat().filter(Boolean));

    const { accounts } = await starling('/api/v2/accounts');
    if (!accounts || !accounts.length) throw new Error('No Starling accounts');

    const syncTime = new Date().toISOString();
    const newRows = [];

    for (const { accountUid, defaultCategory } of accounts) {
      const { feedItems: txs = [] } = await starling(`/api/v2/feed/account/${accountUid}/category/${defaultCategory}?changesSince=${encodeURIComponent(since)}`);
      for (const tx of txs) {
        if (existingIds.has(tx.feedItemUid)) continue;
        const row = mapTx(tx, 'Starling');
        if (row) newRows.push(row);
      }
      const { savingsGoals: spaces = [] } = await starling(`/api/v2/account/${accountUid}/spaces`);
      for (const sp of spaces) {
        if (!sp.savedObjectUid) continue;
        const { feedItems: stxs = [] } = await starling(`/api/v2/feed/account/${accountUid}/category/${sp.savedObjectUid}?changesSince=${encodeURIComponent(since)}`);
        for (const tx of stxs) {
          if (existingIds.has(tx.feedItemUid)) continue;
          const row = mapTx(tx, `Starling (${sp.name})`);
          if (row) newRows.push(row);
        }
      }
    }

    if (newRows.length > 0) {
      await sheetAppend(gToken, newRows);
      await sortSheet(gToken);
    }
    await sheetPut(gToken, 'Config!B2', [[syncTime]]);

    console.log(`Done. ${newRows.length} new rows.`);
    return { statusCode:200, body: JSON.stringify({ ok:true, newRows:newRows.length, since, syncTime }) };
  } catch(err) {
    console.error('Starling sync error:', err.message);
    return { statusCode:500, body: JSON.stringify({ ok:false, error:err.message }) };
  }
};
