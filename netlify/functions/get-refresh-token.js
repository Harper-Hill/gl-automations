'use strict';
const https  = require('https');
const crypto = require('crypto');
exports.handler = async (event) => {
  try {
    const sa = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
    const now = Math.floor(Date.now() / 1000);
    const claim = { iss: sa.client_email, scope: 'https://www.googleapis.com/auth/drive', aud: 'https://oauth2.googleapis.com/token', exp: now + 3600, iat: now };
    const jwt = buildJWT(sa.private_key, claim);
    const body = `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`;
    const tokenResp = await httpReq('POST', 'oauth2.googleapis.com', '/token', body, { 'Content-Type': 'application/x-www-form-urlencoded' });
    const driveToken = JSON.parse(tokenResp).access_token;
    const searchPath = '/drive/v3/files?q=' + encodeURIComponent('name="gl-automations-tokens.json" and trashed=false') + '&fields=files(id,name)';
    const searchResp = JSON.parse(await httpReq('GET', 'www.googleapis.com', searchPath, null, { Authorization: 'Bearer ' + driveToken }));
    if (!searchResp.files || searchResp.files.length === 0) return { statusCode: 200, body: 'No tokens file found: ' + JSON.stringify(searchResp) };
    const content = await httpReq('GET', 'www.googleapis.com', '/drive/v3/files/' + searchResp.files[0].id + '?alt=media', null, { Authorization: 'Bearer ' + driveToken });
    const tokens = JSON.parse(content);
    return { statusCode: 200, headers: { 'Content-Type': 'text/plain' }, body: 'REFRESH_TOKEN: ' + tokens.refresh_token };
  } catch(e) { return { statusCode: 500, body: 'Error: ' + e.message }; }
};
function buildJWT(privateKey, claims) {
  const header = b64u(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64u(JSON.stringify(claims));
  const signing = `${header}.${payload}`;
  const sign = crypto.createSign('RSA-SHA256');
  sign.update(signing);
  return `${signing}.${sign.sign(privateKey, 'base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'')}`;
}
function b64u(str) { return Buffer.from(str).toString('base64').replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,''); }
function httpReq(method, hostname, path, body, headers) {
  return new Promise((resolve, reject) => {
    const opts = { hostname, path, method, headers: { ...headers } };
    if (body) opts.headers['Content-Length'] = Buffer.byteLength(body);
    const req = https.request(opts, (res) => { let d = ''; res.on('data', c => d += c); res.on('end', () => resolve(d)); });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}
