'use strict';
// Jobber OAuth refresh token storage.
// Kept in Netlify Blobs (store "jobber-auth", key "refresh_token") rather than in
// the GL sheet, so anyone with access to the spreadsheet cannot read it.
// One-time migration: if the blob is empty, fall back to the legacy Config!B1 value.
const { getStore } = require('@netlify/blobs');

function store() {
  return getStore({ name: 'jobber-auth', siteID: process.env.NETLIFY_SITE_ID, token: process.env.NETLIFY_ACCESS_TOKEN });
}

async function getRefreshToken(legacyReader) {
  const v = await store().get('refresh_token');
  if (v && v.trim()) return v.trim();
  if (typeof legacyReader === 'function') {
    const legacy = await legacyReader().catch(() => null);
    if (legacy && String(legacy).trim()) {
      console.log('Refresh token migrated from legacy Config!B1 to Netlify Blobs');
      return String(legacy).trim();
    }
  }
  return null;
}

async function saveRefreshToken(token) {
  if (token) await store().set('refresh_token', token);
}

module.exports = { getRefreshToken, saveRefreshToken };
