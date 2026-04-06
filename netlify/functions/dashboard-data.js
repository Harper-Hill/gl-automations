'use strict';
const https = require('https');
const { createSign } = require('crypto');
const CFG = {
  SHEET_ID: process.env.GL_DASHBOARD_SHEET_ID,
  SA_FETCH_URL: process.env.SA_FETCH_URL,
  SA_FETCH_TOKEN: process.env.SA_FETCH_TOKEN,
};
async function fetchSA() {
  function get(url) {
    return new Promise((res, rej) => {
      https.get(url, r => {
        if (r.statusCode >= 300 && r.statusCode < 400 && r.headers.location) return get(r.headers.location).then(res).catch(rej);
        let d = ''; r.on('data', c => d += c);
        r.on('end', () => { try { res(JSON.parse(d)); } catch(e) { rej(new Error('SA:' + d.substring(0,80))); } });
      }).on('error', rej);
    });
  }
  return get(CFG.SA_FETCH_URL + '?token=' + CFG.SA_FETCH_TOKEN);
}
function req(o, b) {
  return new Promise((res, rej) => {
    const r = https.request(o, rr => {
      let d = ''; rr.on('data', c => d += c);
      rr.on('end', () => { try { res(JSON.parse(d)); } catch(e) { res(d); } });
    });
    r.on('error', rej);
    if (b) r.write(typeof b === 'string' ? b : JSON.stringify(b));
    r.end();
  });
}
function b64u(s) { return Buffer.from(s).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }
async function gToken(sa) {
  const now = Math.floor(Date.now()/1000);
  const claim = { iss: sa.client_email, scope: 'https://www.googleapis.com/auth/spreadsheets.readonly', aud: 'https://oauth2.googleapis.com/token', exp: now+3600, iat: now };
  const h = b64u(JSON.stringify({alg:'RS256',typ:'JWT'})), p = b64u(JSON.stringify(claim));
  const sig = createSign('RSA-SHA256').update(`${h}.${p}`).sign(sa.private_key,'base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
  const body = `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${h}.${p}.${sig}`;
  const r = await req({ hostname:'oauth2.googleapis.com', path:'/token', method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'} }, body);
  if (!r.access_token) throw new Error('gToken: ' + JSON.stringify(r));
  return r.access_token;
}
async function get(token, range) {
  const r = await req({ hostname:'sheets.googleapis.com', path:`/v4/spreadsheets/${CFG.SHEET_ID}/values/${encodeURIComponent(range)}`, method:'GET', headers:{Authorization:`Bearer ${token}`} });
  if (r.error) {
    console.error('Sheets API error for', range, ':', JSON.stringify(r.error));
    throw new Error(`Sheets API ${r.error.code}: ${r.error.message}`);
  }
  return (r.values || []);
}
function n(v) { return parseFloat(String(v).replace(/,/g,'')) || 0; }
function rv(rows, i) { return (rows[0] || [])[i] || 0; }

exports.handler = async () => {
  try {
    const sa = await fetchSA();
    const token = await gToken(sa);
    const [
      r35, r36, r39, r42,
      r47, r49, r50, r53, r57,
      r61, r64, r67, r70, r73, r76, r79, r81,
      r84, r85,
      goals
    ] = await Promise.all([
      get(token,'Dashboard!D35:P35'), get(token,'Dashboard!D36:P36'),
      get(token,'Dashboard!D39:P39'), get(token,'Dashboard!D42:P42'),
      get(token,'Dashboard!P47'), get(token,'Dashboard!P49'),
      get(token,'Dashboard!P50'), get(token,'Dashboard!P53'),
      get(token,'Dashboard!P57'),
      get(token,'Dashboard!P61'), get(token,'Dashboard!P64'),
      get(token,'Dashboard!P67'), get(token,'Dashboard!P70'),
      get(token,'Dashboard!P73'), get(token,'Dashboard!P76'),
      get(token,'Dashboard!P79'), get(token,'Dashboard!P81'),
      get(token,'Dashboard!P84'), get(token,'Dashboard!P85'),
      get(token,'Dashboard!C93:C96'),
    ]);
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    const row26 = r35[0] || [], row25 = r36[0] || [];
    const dom26 = r39[0] || [], com26 = r42[0] || [];
    const cogs = n(rv(r47,0)), vat = n(rv(r49,0)), labour = n(rv(r50,0));
    const totalCoS = n(rv(r53,0)), grossProfit = n(rv(r57,0));
    const vehicles = n(rv(r61,0)), directors = n(rv(r64,0));
    const training = n(rv(r67,0)), repairs = n(rv(r70,0));
    const tools = n(rv(r73,0)), depreciation = n(rv(r76,0));
    const otherOps = n(rv(r79,0)), totalOps = n(rv(r81,0));
    const netProfit2025 = n(rv(r84,0)), netProfit = n(rv(r85,0));
    const ytd26 = n(row26[12]), ytd25 = n(row25[12]);
    const domYtd = n(dom26[12]), comYtd = n(com26[12]);
    const totalExpenses = totalCoS + totalOps;
    function expPct(v) { return totalExpenses > 0 ? Math.round((v/totalExpenses)*1000)/10 : 0; }
    return {
      statusCode: 200,
      headers: { 'Content-Type':'application/json','Access-Control-Allow-Origin':'*','Cache-Control':'public,max-age=300' },
      body: JSON.stringify({
        updatedAt: new Date().toISOString(),
        revenue: {
          ytd2026: ytd26, ytd2025: ytd25,
          monthly2026: months.map((m,i) => ({ month:m, value:n(row26[i]) })),
          monthly2025: months.map((m,i) => ({ month:m, value:n(row25[i]) })),
        },
        domestic: { ytd: domYtd },
        commercial: { ytd: comYtd },
        grossProfit: { ytd2026: grossProfit, ytd2025: 38480.40 },
        netProfit: { ytd2026: netProfit, ytd2025: netProfit2025 },
        expenses: {
          total: totalExpenses,
          costOfSales: totalCoS,
          operationalCosts: totalOps,
          breakdown: [
            { label:'Direct labour', value:labour, pct:expPct(labour), info:'Wages paid directly to staff working on jobs' },
            { label:'COGS', value:cogs, pct:expPct(cogs), info:'Cost of goods sold — materials bought specifically for jobs' },
            { label:'VAT liability', value:vat, pct:expPct(vat), info:'VAT collected from clients that is owed to HMRC under the Flat Rate Scheme' },
            { label:'Director remuneration', value:directors, pct:expPct(directors), info:'Salary and benefits paid to company directors' },
            { label:'Vehicles', value:vehicles, pct:expPct(vehicles), info:'Fuel, insurance, servicing and running costs for all company vehicles' },
            { label:'Depreciation', value:depreciation, pct:expPct(depreciation), info:'Reduction in book value of assets like machinery and vehicles over time' },
            { label:'Other overheads', value:otherOps, pct:expPct(otherOps), info:'All other running costs not captured elsewhere' },
            { label:'Small tools & equipment', value:tools, pct:expPct(tools), info:'Purchases of tools and equipment under £250' },
            { label:'Repairs & maintenance', value:repairs, pct:expPct(repairs), info:'One-off repairs to machinery or premises' },
            { label:'Training', value:training, pct:expPct(training), info:'Staff training and development costs' },
          ]
        },
        goals: goals.map(r => r[0]).filter(Boolean),
      })
    };
  } catch(e) {
    console.error(e.message);
    return { statusCode:500, body:JSON.stringify({ error:e.message }) };
  }
};
