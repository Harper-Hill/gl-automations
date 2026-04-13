// One-off backfill: fetches Jobber invoices from last N hours and writes to sheet
// Hit: https://gl-automations.netlify.app/.netlify/functions/jobber-backfill?hours=2
'use strict';
const https = require('https');
const { createSign } = require('crypto');

const CFG = {
  CLIENT_ID:      process.env.JOBBER_GL_CLIENT_ID,
  CLIENT_SECRET:  process.env.JOBBER_GL_CLIENT_SECRET,
  SHEET_ID:       process.env.GL_SHEET_ID,
  SHEET_TAB:      process.env.GL_SHEET_TAB || 'Income',
  SHEET_GID:      process.env.GL_SHEET_GID,
  SA_FETCH_URL:   "",
  SA_FETCH_TOKEN: "",
  FRS_RATE:       0.12,
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
  return JSON.parse(Buffer.from((process.env.GOOGLE_SA_B64_1||""+(process.env.GOOGLE_SA_B64_2||"")),"base64").toString("utf8"));
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

async function getJobberToken(googleToken) {
  const cfgRes = await req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/Config!B1`, method:'GET', headers:{Authorization:`Bearer ${googleToken}`} });
  const refreshToken = cfgRes.data.values && cfgRes.data.values[0] && cfgRes.data.values[0][0];
  if (!refreshToken) throw new Error('No refresh token in Config!B1');
  const body = `client_id=${CFG.CLIENT_ID}&client_secret=${CFG.CLIENT_SECRET}&grant_type=refresh_token&refresh_token=${encodeURIComponent(refreshToken)}`;
  const res = await req({ hostname:'api.getjobber.com', path:'/api/oauth/token', method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'} }, body);
  if (!res.data.access_token) throw new Error('Jobber token error: ' + JSON.stringify(res.data));
  // Rotate refresh token
  if (res.data.refresh_token) {
    await req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/Config!B1?valueInputOption=RAW`, method:'PUT', headers:{Authorization:`Bearer ${googleToken}`,'Content-Type':'application/json'} }, { values: [[res.data.refresh_token]] });
  }
  return res.data.access_token;
}

async function fetchRecentInvoices(jobberToken, hoursAgo) {
  const since = new Date(Date.now() - hoursAgo * 60 * 60 * 1000).toISOString();
  const query = `{
    invoices(filter: { createdAt: { after: "${since}" } }) {
      nodes {
        id
        invoiceNumber
        createdAt
        issuedDate
        total
        client { name companyName }
        jobs { nodes { jobNumber title } }
        lineItems { nodes { name quantity unitPrice } }
      }
    }
  }`;
  const res = await req({
    hostname: 'api.getjobber.com',
    path: '/api/graphql',
    method: 'POST',
    headers: { Authorization: `Bearer ${jobberToken}`, 'Content-Type': 'application/json', 'X-JOBBER-GRAPHQL-VERSION': '2026-03-10' },
  }, { query });
  if (res.data.errors) throw new Error('Jobber GQL: ' + JSON.stringify(res.data.errors));
  return res.data.data.invoices.nodes || [];
}

async function getExistingInvoiceNos(googleToken) {
  const res = await req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(CFG.SHEET_TAB + '!F:F')}`, method:'GET', headers:{Authorization:`Bearer ${googleToken}`} });
  return new Set((res.data.values || []).flat().filter(Boolean));
}

function mapInvoiceRow(inv) {
  const postedDate = inv.createdAt ? new Date(inv.createdAt).toLocaleDateString('en-GB') : '';
  const taxDate = inv.issuedDate ? new Date(inv.issuedDate).toLocaleDateString('en-GB') : postedDate;
  const client = inv.client ? (inv.client.companyName || inv.client.name || '') : '';
  const job = inv.jobs && inv.jobs.nodes && inv.jobs.nodes[0];
  const desc = job ? `${job.jobNumber} – ${job.title}` : (inv.lineItems && inv.lineItems.nodes.length ? inv.lineItems.nodes[0].name : '');
  const total = parseFloat(inv.total || 0).toFixed(2);
  const datePaid = '';
  const row = new Array(20).fill('');
  row[0] = postedDate;
  row[1] = taxDate;
  row[2] = client;
  row[3] = desc;
  row[4] = 'Jobber';
  row[5] = String(inv.invoiceNumber);
  row[7] = 'SALES';
  row[8] = total;
  row[19] = datePaid;
  return row;
}

async function appendAndSort(googleToken, rows) {
  const range = encodeURIComponent(CFG.SHEET_TAB + '!A:T');
  await req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/${range}:append?valueInputOption=USER_ENTERED&insertDataOption=INSERT_ROWS`, method:'POST', headers:{Authorization:`Bearer ${googleToken}`,'Content-Type':'application/json'} }, { values: rows });
  await req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}:batchUpdate`, method:'POST', headers:{Authorization:`Bearer ${googleToken}`,'Content-Type':'application/json'} },
    { requests:[{ sortRange:{ range:{ sheetId:parseInt(CFG.SHEET_GID,10), startRowIndex:1, startColumnIndex:0, endColumnIndex:20 }, sortSpecs:[{ dimensionIndex:0, sortOrder:'ASCENDING' }] } }] });
}

exports.handler = async (event) => {
  try {
    const hours = parseInt((event.queryStringParameters && event.queryStringParameters.hours) || '2', 10);
    console.log(`Backfilling Jobber invoices from last ${hours} hours`);
    const sa = await fetchServiceAccount();
    const googleToken = await getGoogleToken(sa);
    const jobberToken = await getJobberToken(googleToken);
    const invoices = await fetchRecentInvoices(jobberToken, hours);
    console.log(`Found ${invoices.length} invoices`);
    const existing = await getExistingInvoiceNos(googleToken);
    const newRows = invoices.filter(inv => !existing.has(String(inv.invoiceNumber))).map(mapInvoiceRow);
    console.log(`Writing ${newRows.length} new rows`);
    if (newRows.length > 0) await appendAndSort(googleToken, newRows);
    return { statusCode:200, body: JSON.stringify({ ok:true, found: invoices.length, written: newRows.length }) };
  } catch(err) {
    console.error('Backfill error:', err.message);
    return { statusCode:500, body: JSON.stringify({ ok:false, error:err.message }) };
  }
};
