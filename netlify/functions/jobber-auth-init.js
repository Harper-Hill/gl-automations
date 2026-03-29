// ================================================================
// netlify/functions/jobber-auth-init.js
// One-time OAuth setup — visit this URL in a browser to connect
// Jobber. Tokens are stored as Netlify environment variables.
// DELETE or restrict this function after first use.
// ================================================================

'use strict';

const https = require('https');

const CFG = {
  CLIENT_ID:     process.env.JOBBER_GL_CLIENT_ID,
  CLIENT_SECRET: process.env.JOBBER_GL_CLIENT_SECRET,
  NETLIFY_TOKEN: process.env.NETLIFY_ACCESS_TOKEN,
  SITE_ID:       process.env.NETLIFY_SITE_ID,
};

exports.handler = async (event) => {
  const params = event.queryStringParameters || {};

  // Step 2 — exchange code for tokens and store as env vars
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

    const expiry = String(Date.now() + (data.expires_in || 3600) * 1000);

    // Store tokens as Netlify env vars
    await setNetlifyEnvVar('JOBBER_ACCESS_TOKEN',  data.access_token);
    await setNetlifyEnvVar('JOBBER_REFRESH_TOKEN', data.refresh_token);
    await setNetlifyEnvVar('JOBBER_TOKEN_EXPIRY',  expiry);

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: `
        <html><body style="font-family:sans-serif;padding:40px">
          <h2>✅ Jobber connected for GL Automations</h2>
          <p>Tokens stored as Netlify environment variables.</p>
          <p><strong>You can now delete the <code>jobber-auth-init</code> function.</strong></p>
        </body></html>
      `,
    };
  }

  // Step 1 — redirect to Jobber OAuth
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

async function setNetlifyEnvVar(key, value) {
  const body = JSON.stringify([{ value, context: 'all' }]);
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: 'api.netlify.com',
      path:     `/api/v1/sites/${CFG.SITE_ID}/env/${key}`,
      method:   'PUT',
      headers:  {
        'Authorization': 'Bearer ' + CFG.NETLIFY_TOKEN,
        'Content-Type':  'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    };
    const req = https.request(opts, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve({ status: res.statusCode, body: d }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function getRedirectUri(event) {
  const host  = event.headers['x-forwarded-host'] || event.headers.host;
  const proto = event.headers['x-forwarded-proto'] || 'https';
  return `${proto}://${host}/.netlify/functions/jobber-auth-init`;
}

async function httpPost(hostname, path, body) {
  return new Promise((resolve, reject) => {
    const headers = {
      'Content-Type':   'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body),
    };
    const req = https.request({ hostname, path, method: 'POST', headers }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}
