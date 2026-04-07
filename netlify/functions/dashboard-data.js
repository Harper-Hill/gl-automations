'use strict';
const https = require('https');

const CFG = {
  SA_FETCH_URL:   process.env.SA_FETCH_URL,
  SA_FETCH_TOKEN: process.env.SA_FETCH_TOKEN,
};

exports.handler = async () => {
  try {
    const url = CFG.SA_FETCH_URL + '?token=' + CFG.SA_FETCH_TOKEN + '&action=dashboard';
    const data = await new Promise((resolve, reject) => {
      function get(u) {
        https.get(u, (res) => {
          if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
            return get(res.headers.location);
          }
          let d = '';
          res.on('data', c => d += c);
          res.on('end', () => {
            try { resolve(JSON.parse(d)); }
            catch(e) { reject(new Error('Parse error: ' + d.substring(0, 200))); }
          });
        }).on('error', reject);
      }
      get(url);
    });

    if (data.error) throw new Error('Apps Script error: ' + data.error);

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'public, max-age=300',
      },
      body: JSON.stringify(data),
    };
  } catch(err) {
    console.error('Dashboard error:', err.message);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
