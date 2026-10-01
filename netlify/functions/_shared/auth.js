'use strict';
// Shared-secret check for admin endpoints (seed-sa, jobber-backfill).
// Set ADMIN_KEY in Netlify site environment variables. If unset, endpoints refuse all calls.
const crypto = require('crypto');
function isAuthorised(event) {
  const expected = process.env.ADMIN_KEY || '';
  const given = (event.headers && (event.headers['x-admin-key'] || event.headers['X-Admin-Key'])) ||
                (event.queryStringParameters && event.queryStringParameters.key) || '';
  if (!expected || !given || given.length !== expected.length) return false;
  return crypto.timingSafeEqual(Buffer.from(given), Buffer.from(expected));
}
module.exports = { isAuthorised };
