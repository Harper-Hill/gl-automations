// ================================================================
// netlify/functions/stripe-sync.js
// Scheduled daily + callable manually via GET
// Stripe charges → Income tab (full amount)
// Stripe fees → Expenses tab (processing fee per charge)
// ================================================================
'use strict';
const https = require('https');
const { createSign } = require('crypto');
const CFG = {
  STRIPE_KEY:     process.env.STRIPE_API_KEY,
  SHEET_ID:       process.env.GL_SHEET_ID,
  INCOME_TAB:     process.env.GL_SHEET_TAB || 'Income',
  INCOME_GID:     process.env.GL_SHEET_GID,
  EXPENSES_TAB:   process.env.GL_EXPENSES_TAB || 'Expenses',
  EXPENSES_GID:   process.env.GL_EXPENSES_GID,
  SA_FETCH_URL:   "",
  SA_FETCH_TOKEN: "",
};
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
    const { getStore } = require('@netlify/blobs');
  const store = getStore('service-account');
  const raw = await store.get('sa_json');
  if (!raw) throw new Error('SA JSON not found in Netlify Blobs');
  return JSON.parse(raw);
}
function req(options, body) {
  return new Promise((resolve, reject) => {
    const r = https.request(options, (res) => {
      let d = ''; res.on('data', c => d += c);
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
  const body = `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${h}.${p}.${sig}`;
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
async function sheetAppend(token, tab, values) {
  const range = encodeURIComponent(tab + '!A:T');
  return req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/${range}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`, method:'POST', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'} }, { values });
}
async function sortSheet(token, gid) {
  return req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}:batchUpdate`, method:'POST', headers:{Authorization:`Bearer ${token}`,'Content-Type':'application/json'} },
    { requests:[{ sortRange:{ range:{ sheetId:parseInt(gid,10), startRowIndex:1, startColumnIndex:0, endColumnIndex:20 }, sortSpecs:[{ dimensionIndex:0, sortOrder:'ASCENDING' }] } }] });
}
async function getExistingIds(token, tab, col) {
  const data = await sheetGet(token, `${tab}!${col}:${col}`);
  return new Set((data.values || []).flat().filter(Boolean));
}
async function getAllCharges(since) {
  const charges = [];
  let startAfter = null;
  while (true) {
    let path = `/v1/charges?limit=100&created[gte]=${since}&expand[]=data.balance_transaction`;
    if (startAfter) path += `&starting_after=${startAfter}`;
    const data = await req({ hostname:'api.stripe.com', path, method:'GET', headers:{ Authorization:`Basic ${Buffer.from(CFG.STRIPE_KEY+':').toString('base64')}` } });
    if (data.data.error) throw new Error('Stripe error: ' + data.data.error.message);
    const items = data.data.data || [];
    charges.push(...items);
    if (!data.data.has_more || items.length === 0) break;
    startAfter = items[items.length-1].id;
  }
  return charges;
}
function mapIncomeRow(charge) {
  const d = new Date(charge.created * 1000).toLocaleDateString('en-GB');
  const amount = (charge.amount / 100).toFixed(2);
  const customer = (charge.billing_details && (charge.billing_details.name || charge.billing_details.email)) || charge.customer || '';
  const description = charge.description || 'Stripe payment';
  const row = new Array(20).fill('');
  row[0]=d; row[1]=d; row[2]=customer; row[3]=description;
  row[4]='Stripe'; row[5]=charge.id; row[7]='SALES'; row[8]=amount;
  row[17]='Domestic'; row[19]=d;
  return row;
}
function mapFeeRow(charge) {
  const bt = charge.balance_transaction;
  if (!bt || typeof bt !== 'object' || !bt.fee || bt.fee === 0) return null;
  const d = new Date(charge.created * 1000).toLocaleDateString('en-GB');
  const fee = (bt.fee / 100).toFixed(2);
  const row = new Array(20).fill('');
  row[0]=d; row[1]=d; row[2]='Stripe'; row[3]=`Stripe fee: ${charge.id}`;
  row[4]='Card'; row[6]='Stripe'; row[10]=fee; row[19]=charge.id+'_fee';
  return row;
}
exports.handler = async () => {
  try {
    console.log('Stripe sync starting...');
    const sa = await fetchServiceAccount();
    const gToken = await getGoogleToken(sa);
    const cfgData = await sheetGet(gToken, 'Config!B4');
    const sinceIso = (cfgData.values && cfgData.values[0] && cfgData.values[0][0]) || new Date(new Date().getFullYear(),0,1).toISOString();
    const sinceUnix = Math.floor(new Date(sinceIso).getTime() / 1000);
    console.log('Syncing since:', sinceIso);
    const existingIncomeIds = await getExistingIds(gToken, CFG.INCOME_TAB, 'F');
    const existingExpenseIds = await getExistingIds(gToken, CFG.EXPENSES_TAB, 'T');
    const charges = await getAllCharges(sinceUnix);
    const successful = charges.filter(c => c.status === 'succeeded' && !c.refunded);
    console.log(`Found ${successful.length} successful charges`);
    const incomeRows = [];
    const expenseRows = [];
    for (const charge of successful) {
      if (!existingIncomeIds.has(charge.id)) incomeRows.push(mapIncomeRow(charge));
      const feeRow = mapFeeRow(charge);
      if (feeRow && !existingExpenseIds.has(charge.id+'_fee')) expenseRows.push(feeRow);
    }
    if (incomeRows.length > 0) { await sheetAppend(gToken, CFG.INCOME_TAB, incomeRows); await sortSheet(gToken, CFG.INCOME_GID); }
    if (expenseRows.length > 0) { await sheetAppend(gToken, CFG.EXPENSES_TAB, expenseRows); await sortSheet(gToken, CFG.EXPENSES_GID); }
    const syncTime = new Date().toISOString();
    await sheetPut(gToken, 'Config!B4', [[syncTime]]);
    console.log(`Done. Income: ${incomeRows.length}, Expenses: ${expenseRows.length}`);
    return { statusCode:200, body: JSON.stringify({ ok:true, incomeRows:incomeRows.length, expenseRows:expenseRows.length, since:sinceIso, syncTime }) };
  } catch(err) {
    console.error('Stripe sync error:', err.message);
    return { statusCode:500, body: JSON.stringify({ ok:false, error:err.message }) };
  }
};
