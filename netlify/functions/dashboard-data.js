'use strict';
const https = require('https');
const { createSign } = require('crypto');
const CFG = { SHEET_ID: process.env.GL_SHEET_ID, SA_FETCH_URL: process.env.SA_FETCH_URL, SA_FETCH_TOKEN: process.env.SA_FETCH_TOKEN };
async function fetchSA() {
  function get(url) { return new Promise((resolve, reject) => { https.get(url, (res) => { if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) return get(res.headers.location).then(resolve).catch(reject); let d = ''; res.on('data', c => d += c); res.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { reject(new Error('SA: ' + d.substring(0,100))); } }); }).on('error', reject); }); }
  return get(CFG.SA_FETCH_URL + '?token=' + CFG.SA_FETCH_TOKEN);
}
function req(o, b) { return new Promise((resolve, reject) => { const r = https.request(o, rr => { let d = ''; rr.on('data', c => d += c); rr.on('end', () => { try { resolve(JSON.parse(d)); } catch(e) { resolve(d); } }); }); r.on('error', reject); if (b) r.write(typeof b === 'string' ? b : JSON.stringify(b)); r.end(); }); }
function b64u(s) { return Buffer.from(s).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }
async function gToken(sa) {
  const now = Math.floor(Date.now()/1000);
  const claim = { iss: sa.client_email, scope: 'https://www.googleapis.com/auth/spreadsheets.readonly', aud: 'https://oauth2.googleapis.com/token', exp: now+3600, iat: now };
  const h = b64u(JSON.stringify({alg:'RS256',typ:'JWT'})), p = b64u(JSON.stringify(claim));
  const sig = createSign('RSA-SHA256').update(h+'.'+p).sign(sa.private_key,'base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
  const body = 'grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion='+h+'.'+p+'.'+sig;
  const r = await req({ hostname:'oauth2.googleapis.com', path:'/token', method:'POST', headers:{'Content-Type':'application/x-www-form-urlencoded'} }, body);
  if (!r.access_token) throw new Error('gToken: ' + JSON.stringify(r));
  return r.access_token;
}
exports.handler = async () => {
  try {
    const sa = await fetchSA();
    const token = await gToken(sa);
    const range = encodeURIComponent('Config!B5');
    const res = await req({ hostname:'sheets.googleapis.com', path:'/v4/spreadsheets/'+CFG.SHEET_ID+'/values/'+range, method:'GET', headers:{Authorization:'Bearer '+token} });
    const raw = res.values && res.values[0] && res.values[0][0];
    if (!raw) throw new Error('No data in Config B5');
    return { statusCode:200, headers:{'Content-Type':'application/json','Access-Control-Allow-Origin':'*','Cache-Control':'public,max-age=300'}, body:raw };
  } catch(err) {
    console.error('Dashboard error:', err.message);
    return { statusCode:500, body:JSON.stringify({error:err.message}) };
  }
};
