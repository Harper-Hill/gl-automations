'use strict';
const https = require('https');

const CFG = {
  SA_FETCH_URL:   process.env.SA_FETCH_URL,
  SA_FETCH_TOKEN: process.env.SA_FETCH_TOKEN,
};

exports.handler = async () => {
  try {
    const data = await new Promise((resolve, reject) => {
      function get(url) {
        const u = new URL(url);
        const options = {
          hostname: u.hostname,
          path: u.pathname + u.search,
          method: 'GET',
          headers: { 'User-Agent': 'Mozilla/5.0' }
        };
        https.get(options, (res) => {
          if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
            let loc = res.headers.location;
            // Re-append our query params if they got stripped
            if (!loc.includes('action=dashboard')) {
              loc += (loc.includes('?') ? '&' : '?') + 'token=' + CFG.SA_FETCH_TOKEN + '&action=dashboard';
            }
            return get(loc);
          }
          let d = '';
          res.on('data', c => d += c);
          res.on('end', () => {
            try { resolve(JSON.parse(d)); }
            catch(e) { reject(new Error('Parse: ' + d.substring(0, 300))); }
          });
        }).on('error', reject);
      }
      get(CFG.SA_FETCH_URL + '?token=' + CFG.SA_FETCH_TOKEN + '&action=dashboard');
    });

    if (data.error) throw new Error('Script error: ' + data.error);
    if (!data.revenue) throw new Error('No revenue data in response');

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
