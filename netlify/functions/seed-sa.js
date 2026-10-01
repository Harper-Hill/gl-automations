'use strict';
const { isAuthorised } = require('./_shared/auth');
exports.handler = async (event) => {
  try {
    if (!isAuthorised(event)) {
      return { statusCode: 401, body: 'unauthorized' };
    }
    if (event.httpMethod !== 'POST') {
      return { statusCode: 405, body: 'POST only with JSON body' };
    }
    const { getStore } = require('@netlify/blobs');
    const store = getStore({
      name: 'service-account',
      siteID: process.env.NETLIFY_SITE_ID,
      token: process.env.NETLIFY_ACCESS_TOKEN,
    });
    const body = event.body;
    JSON.parse(body);
    await store.set('sa_json', body);
    const readback = await store.get('sa_json');
    return { statusCode: 200, body: JSON.stringify({ ok: true, wrote: body.length, readback: readback ? readback.length : 0 }) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};
