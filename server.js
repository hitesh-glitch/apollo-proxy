const express = require('express');
const fetch = require('node-fetch');
const path = require('path');
const app = express();

app.use(express.json());

// CORS — allow all origins
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,DELETE,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type,X-Api-Key,Authorization,Accept,Cache-Control');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── Serve the Dmand app as static HTML ──
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

// Proxy Apollo requests
app.all('/apollo/*', async (req, res) => {
  try {
    const apolloPath = req.path.replace('/apollo', '');
    const qs = req.url.includes('?') ? '?' + req.url.split('?').slice(1).join('?') : '';
    const url = 'https://api.apollo.io' + apolloPath + qs;
    const key = req.headers['x-api-key'] || '';

    const body = ['POST','PUT','PATCH'].includes(req.method)
      ? JSON.stringify(req.body) : undefined;

    const r = await fetch(url, {
      method: req.method,
      headers: {
        'X-Api-Key': key,
        'Content-Type': 'application/json',
        'Accept': 'application/json',
      },
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
app.listen(PORT, () => console.log('Dmand proxy+app running on port', PORT));
