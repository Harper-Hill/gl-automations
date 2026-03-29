// ================================================================
// netlify/functions/jobber-auth-init.js
// One-time OAuth setup — visit this URL in a browser to connect
// Jobber and store tokens in Netlify Blobs
// DELETE or restrict this function after first use
// ================================================================

'use strict';

const https = require('https');
const { getStore } = require('@netlify/blobs');

const CFG = {
  CLIENT_ID:     process.env.JOBBER_GL_CLIENT_ID,
  CLIENT_SECRET: process.env.JOBBER_GL_CLIENT_SECRET,
  REDIRECT_PATH: '/.netlify/functions/jobber-auth-init',
};

exports.handler = async (event) => {
  const params = event.queryStringParameters || {};

  // Step 2 — exchange code for tokens
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
      return {
        statusCode: 400,
        body: `Token exchange failed: ${resp.body}`,
      };
    }

    const store = getStore({ name: 'gl-jobber-tokens', siteID: process.env.SITE_ID, token: process.env.TOKEN });
    await store.set('access_token',  data.access_token);
    await store.set('refresh_token', data.refresh_token);
    await store.set('expiry', String(Date.now() + (data.expires_in || 3600) * 1000));

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: `
        <html><body style="font-family:sans-serif;padding:40px">
          <h2>✅ Jobber connected for GL Automations</h2>
          <p>Tokens stored in Netlify Blobs successfully.</p>
          <p><strong>You can now delete or disable the <code>jobber-auth-init</code> function.</strong></p>
        </body></html>
      `,
    };
  }

  // Step 1 — redirect to Jobber OAuth
  const redirectUri = getRedirectUri(event);
  const authUrl =
    'https://api.getjobber.com/api/oauth/authorize' +
    `?client_id=${encodeURIComponent(CFG.CLIENT_ID)}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&response_type=code`;

  return {
    statusCode: 302,
    headers: { Location: authUrl },
    body: '',
  };
};

function getRedirectUri(event) {
  const host  = event.headers['x-forwarded-host'] || event.headers.host;
  const proto = event.headers['x-forwarded-proto'] || 'https';
  return `${proto}://${host}${CFG.REDIRECT_PATH}`;
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
