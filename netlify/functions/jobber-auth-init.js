// ================================================================
// netlify/functions/jobber-auth-init.js
// One-time OAuth setup — visit this URL in a browser to connect Jobber
// Tokens stored in Google Drive as gl-automations-tokens.json
// ================================================================

'use strict';

const https  = require('https');
const crypto = require('crypto');

const CFG = {
  CLIENT_ID:     process.env.JOBBER_GL_CLIENT_ID,
  CLIENT_SECRET: process.env.JOBBER_GL_CLIENT_SECRET,
  REDIRECT_PATH: '/.netlify/functions/jobber-auth-init',
};

const TOKENS_FILE_NAME = 'gl-automations-tokens.json';

exports.handler = async (event) => {
  const params = event.queryStringParameters || {};

  if (params.code) {
    const redirectUri = getRedirectUri(event);
    const body = new URLSearchParams({
      client_id:     CFG.CLIENT_ID,
      client_secret: CFG.CLIENT_SECRET,
      grant_type:    'authorization_code',
      code:          params.code,
      redirect_uri:  redirectUri,
    });

    const resp = await httpPost('api.getjobber.com', '/api/oauth/token', body.toString());
    const data = JSON.parse(resp.body);

    if (!data.access_token) {
      return { statusCode: 400, body: 'Token exchange failed: ' + resp.body };
    }

    // Store tokens in Google Drive
    const driveToken = await getGoogleAccessToken();
    const tokens = {
      access_token:  data.access_token,
      refresh_token: data.refresh_token,
      expires_at:    Date.now() + (data.expires_in || 3600) * 1000,
    };
    await writeTokensToDrive(driveToken, tokens);

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: `
        <html><body style="font-family:sans-serif;padding:40px">
          <h2>&#x2705; Jobber connected for GL Automations</h2>
          <p>Tokens stored in Google Drive as <code>${TOKENS_FILE_NAME}</code>.</p>
          <p>No redeploy needed — the webhook function reads tokens from Drive on every invocation.</p>
        </body></html>
      `,
    };
  }

  const redirectUri = getRedirectUri(event);
  const authUrl =
    'https://api.getjobber.com/api/oauth/authorize' +
    '?client_id='     + encodeURIComponent(CFG.CLIENT_ID) +
    '&redirect_uri='  + encodeURIComponent(redirectUri) +
    '&response_type=code';

  return {
    statusCode: 302,
    headers: { Location: authUrl },
    body: '',
  };
};

// ── GOOGLE DRIVE ──────────────────────────────────────────────
async function getGoogleAccessToken() {
  const sa = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
  const now = Math.floor(Date.now() / 1000);
  const claim = {
    iss: sa.client_email,
    scope: 'https://www.googleapis.com/auth/drive',
    aud: 'https://oauth2.googleapis.com/token',
    exp: now + 3600,
    iat: now,
  };
  const jwt  = buildJWT(sa.private_key, claim);
  const body = new URLSearchParams({
    grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
    assertion:  jwt,
  });
  const resp = await httpPost('oauth2.googleapis.com', '/token', body.toString(), {
    'Content-Type': 'application/x-www-form-urlencoded',
  }, true);
  const data = JSON.parse(resp.body);
  if (!data.access_token) throw new Error('Google auth failed: ' + resp.body);
  return data.access_token;
}

async function writeTokensToDrive(driveToken, tokens) {
  const json = JSON.stringify(tokens, null, 2);
  const buf  = Buffer.from(json, 'utf8');

  const searchResp = await new Promise((resolve, reject) => {
    const opts = {
      hostname: 'www.googleapis.com',
      path:     '/drive/v3/files?q=' + encodeURIComponent('name="' + TOKENS_FILE_NAME + '" and trashed=false') + '&fields=files(id)',
      headers:  { Authorization: 'Bearer ' + driveToken },
    };
    https.get(opts, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(JSON.parse(d)));
    }).on('error', reject);
  });

  const files = (searchResp.files || []);
  if (files.length > 0) {
    // Update existing
    await new Promise((resolve, reject) => {
      const opts = {
        hostname: 'www.googleapis.com',
        path:     '/upload/drive/v3/files/' + files[0].id + '?uploadType=media',
        method:   'PATCH',
        headers:  {
          Authorization:    'Bearer ' + driveToken,
          'Content-Type':   'application/json',
          'Content-Length': buf.length,
        },
      };
      const req = https.request(opts, (res) => {
        let d = '';
        res.on('data', c => d += c);
        res.on('end', () => resolve(d));
      });
      req.on('error', reject);
      req.write(buf);
      req.end();
    });
  } else {
    // Create new
    const boundary = 'gl_tok_boundary';
    const meta = JSON.stringify({ name: TOKENS_FILE_NAME, mimeType: 'application/json' });
    const multipart = Buffer.concat([
      Buffer.from('--' + boundary + '\r\nContent-Type: application/json\r\n\r\n'),
      Buffer.from(meta),
      Buffer.from('\r\n--' + boundary + '\r\nContent-Type: application/json\r\n\r\n'),
      buf,
      Buffer.from('\r\n--' + boundary + '--'),
    ]);
    await new Promise((resolve, reject) => {
      const opts = {
        hostname: 'www.googleapis.com',
        path:     '/upload/drive/v3/files?uploadType=multipart',
        method:   'POST',
        headers:  {
          Authorization:    'Bearer ' + driveToken,
          'Content-Type':   'multipart/related; boundary=' + boundary,
          'Content-Length': multipart.length,
        },
      };
      const req = https.request(opts, (res) => {
        let d = '';
        res.on('data', c => d += c);
        res.on('end', () => resolve(d));
      });
      req.on('error', reject);
      req.write(multipart);
      req.end();
    });
  }
}

// ── JWT ───────────────────────────────────────────────────────
function buildJWT(privateKey, claims) {
  const header  = base64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = base64url(JSON.stringify(claims));
  const signing = `${header}.${payload}`;
  const sign    = crypto.createSign('RSA-SHA256');
  sign.update(signing);
  const sig = sign.sign(privateKey, 'base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
  return `${signing}.${sig}`;
}
function base64url(str) {
  return Buffer.from(str).toString('base64')
    .replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

function getRedirectUri(event) {
  const host  = event.headers['x-forwarded-host'] || event.headers.host;
  const proto = event.headers['x-forwarded-proto'] || 'https';
  return `${proto}://${host}${CFG.REDIRECT_PATH}`;
}

async function httpPost(hostname, path, body, extraHeaders, rawBody) {
  const bodyStr = rawBody ? body : JSON.stringify(body);
  const contentType = rawBody
    ? (extraHeaders && extraHeaders['Content-Type'] ? extraHeaders['Content-Type'] : 'application/x-www-form-urlencoded')
    : 'application/json';
  const headers = {
    'Content-Type':   contentType,
    'Content-Length': Buffer.byteLength(bodyStr),
  };
  return new Promise((resolve, reject) => {
    const req = https.request({ hostname, path, method: 'POST', headers }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ ok: res.statusCode < 400, status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(bodyStr);
    req.end();
  });
}
