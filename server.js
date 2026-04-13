const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
const app = express();

app.use(express.json());

// CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type,X-Api-Key,Authorization,Accept,Cache-Control');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// Serve static JS files with cache
app.get('/app.js', (req, res) => {
  res.set({'Cache-Control':'public, max-age=3600','Content-Type':'application/javascript; charset=utf-8'});
  res.sendFile(path.join(__dirname, 'app.js'));
});
app.get('/poll-worker.js', (req, res) => {
  res.set({
    'Cache-Control': 'public, max-age=3600',
    'Content-Type': 'application/javascript; charset=utf-8',
    'Vary': 'Accept-Encoding'
  });
  res.sendFile(path.join(__dirname, 'app.js'));
});

// Serve index.html (no cache - always fresh)
app.get('/', (req, res) => {
  res.set('Cache-Control', 'no-cache');
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Apollo proxy
app.all('/apollo/*', async (req, res) => {
  try {
    const apolloPath = req.path.replace('/apollo', '');
    const qs = req.url.includes('?') ? '?' + req.url.split('?').slice(1).join('?') : '';
    const url = 'https://api.apollo.io' + apolloPath + qs;
    const key = req.headers['x-api-key'] || '';
    const body = ['POST','PUT','PATCH'].includes(req.method) ? JSON.stringify(req.body) : undefined;
    const r = await fetch(url, {
      method: req.method,
      headers: { 'X-Api-Key': key, 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body,
    });
    const data = await r.text();
    res.status(r.status).set('Content-Type', 'application/json').send(data);
  } catch (e) {
    res.status(503).json({ error: e.message });
  }
});

app.get('/health', (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log('Dmand running on port', PORT));
