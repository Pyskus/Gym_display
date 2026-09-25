self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(clients.claim());
});

self.addEventListener('fetch', (event) => {
  // Envoie les requêtes directement au serveur Flask / Socket.IO
  event.respondWith(fetch(event.request));
});