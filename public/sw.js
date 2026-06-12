const VERSION = '2026-06-12';
const CACHE = 'hh-' + VERSION;

self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Always network for API / function calls — never cache
  if (url.pathname.startsWith('/.netlify/')) {
    e.respondWith(fetch(e.request));
    return;
  }

  // Always fetch HTML fresh from network
  if (url.pathname.endsWith('.html') || url.pathname === '/') {
    e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
    return;
  }

  // Cache-first for static assets only (fonts, Chart.js, icons)
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(res => {
        const clone = res.clone();
        caches.open(CACHE).then(c => c.put(e.request, clone));
        return res;
      });
    })
  );
});
