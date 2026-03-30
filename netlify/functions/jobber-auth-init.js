// ================================================================
// netlify/functions/jobber-auth-init.js
// Step 1: Visit this URL in browser → redirects to Jobber login
// Step 2: After authorising, this page shows you the refresh token
// Step 3: Copy the refresh token into Netlify env var JOBBER_REFRESH_TOKEN
// Step 4: Delete this function (it's no longer needed)
// ================================================================

'use strict';

const https = require('https');

const CLIENT_ID     = process.env.JOBBER_GL_CLIENT_ID;
const CLIENT_SECRET = process.env.JOBBER_GL_CLIENT_SECRET;
const REDIRECT_PATH = '/.netlify/functions/jobber-auth-init';

exports.handler = async (event) => {
  const params = event.queryStringParameters || {};

  // Step 2: Jobber redirected back with a code — exchange it for tokens
  if (params.code) {
    const redirectUri = getRedirectUri(event);
    const body = [
      'client_id='     + encodeURIComponent(CLIENT_ID),
      'client_secret=' + encodeURIComponent(CLIENT_SECRET),
      'grant_type=authorization_code',
      'code='          + encodeURIComponent(params.code),
      'redirect_uri='  + encodeURIComponent(redirectUri),
    ].join('&');

    const resp = await post('api.getjobber.com', '/api/oauth/token', body);

    let data;
    try {
      data = JSON.parse(resp);
    } catch (e) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'text/html' },
        body: `<html><body><h2>Token exchange failed</h2><pre>${resp}</pre></body></html>`,
      };
    }

    if (!data.access_token) {
      return {
        statusCode: 400,
        headers: { 'Content-Type': 'text/html' },
        body: `<html><body><h2>Token exchange failed</h2><pre>${JSON.stringify(data, null, 2)}</pre></body></html>`,
      };
    }

    // PATCH JOBBER_REFRESH_TOKEN in Netlify automatically
    await patchNetlifyRefreshToken(data.refresh_token);

    // Show the refresh token so the user can add it to Netlify
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'text/html' },
      body: `
        <html>
        <body style="font-family:monospace;padding:40px;max-width:800px">
          <h2>&#x2705; Jobber Connected</h2>
          <p>Copy the refresh token below and add it to Netlify as <strong>JOBBER_REFRESH_TOKEN</strong>:</p>
          <textarea rows="3" style="width:100%;font-size:12px;padding:8px" onclick="this.select()">${data.refresh_token}</textarea>
          <br><br>
          <p style="color:#666">Steps:</p>
          <ol>
            <li>Copy the token above</li>
            <li>Go to Netlify → gl-automations → Environment variables</li>
            <li>Add/update <code>JOBBER_REFRESH_TOKEN</code> with this value</li>
            <li>Trigger a redeploy</li>
            <li>Delete this <code>jobber-auth-init</code> function from the repo</li>
          </ol>
        </body>
        </html>
      `,
    };
  }

  // Step 1: Redirect to Jobber OAuth
  const redirectUri = getRedirectUri(event);
  const authUrl =
    'https://api.getjobber.com/api/oauth/authorize' +
    '?client_id='     + encodeURIComponent(CLIENT_ID) +
    '&redirect_uri='  + encodeURIComponent(redirectUri) +
    '&response_type=code';

  return {
    statusCode: 302,
    headers: { Location: authUrl },
    body: '',
  };
};

function getRedirectUri(event) {
  const host  = event.headers['x-forwarded-host'] || event.headers.host;
  const proto = event.headers['x-forwarded-proto'] || 'https';
  return `${proto}://${host}${REDIRECT_PATH}`;
}

async function patchNetlifyRefreshToken(token) {
  const siteId = process.env.NETLIFY_SITE_ID;
  const pat    = process.env.NETLIFY_ACCESS_TOKEN;
  if (!siteId || !pat) return;
  const body = JSON.stringify([{ value: token, context: 'all' }]);
  return new Promise((resolve) => {
    const req = https.request({
      hostname: 'api.netlify.com',
      path:     `/api/v1/sites/${siteId}/env/JOBBER_REFRESH_TOKEN`,
      method:   'PATCH',
      headers:  {
        Authorization:    'Bearer ' + pat,
        'Content-Type':   'application/json',
        'Content-Length': Buffer.byteLength(body),
      },
    }, (res) => {
      let d = ''; res.on('data', c => d += c);
      res.on('end', () => {
        console.log('Netlify PATCH status:', res.statusCode, d.substring(0, 100));
        resolve();
      });
    });
    req.on('error', resolve);
    req.write(body);
    req.end();
  });
}

function post(hostname, path, body) {
  return new Promise((resolve, reject) => {
    const opts = {
      hostname,
      path,
      method: 'POST',
      headers: {
        'Content-Type':   'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(body),
      },
    };
    const req = https.request(opts, (res) => {
      let d = '';
      res.on('data', c => d += c);
      res.on('end', () => resolve(d));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}
