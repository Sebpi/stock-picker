/**
 * Playwright dev server: serves the frontend static files and proxies
 * all /api/* requests to the FastAPI backend (default port 8000).
 *
 * Run the backend first:
 *   cd backend && uvicorn main:app --port 8000 --reload
 * Then:
 *   npx playwright test
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

const FRONTEND = path.join(__dirname, '..', 'frontend');
const PORT = 4321;
const BACKEND_PORT = parseInt(process.env.BACKEND_PORT || '8000', 10);

const MIME = {
  '.html': 'text/html', '.js': 'application/javascript',
  '.css': 'text/css', '.svg': 'image/svg+xml',
  '.png': 'image/png', '.ico': 'image/x-icon', '.json': 'application/json',
};

function proxyToBackend(req, res) {
  const options = {
    hostname: 'localhost',
    port: BACKEND_PORT,
    path: req.url,
    method: req.method,
    headers: Object.assign({}, req.headers, { host: `localhost:${BACKEND_PORT}` }),
  };
  const proxy = http.request(options, (backendRes) => {
    res.writeHead(backendRes.statusCode, backendRes.headers);
    backendRes.pipe(res, { end: true });
  });
  proxy.on('error', () => {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ detail: `Backend not available on port ${BACKEND_PORT}` }));
  });
  req.pipe(proxy, { end: true });
}

http.createServer((req, res) => {
  // Proxy API + auth routes to the FastAPI backend
  if (req.url.startsWith('/api/') || req.url.startsWith('/v1/') || req.url.startsWith('/static/logo')) {
    proxyToBackend(req, res);
    return;
  }

  let filePath;
  const urlPath = req.url.split('?')[0];
  if (urlPath === '/' || urlPath === '/index.html') {
    filePath = path.join(FRONTEND, 'index.html');
  } else if (urlPath.startsWith('/static/')) {
    filePath = path.join(FRONTEND, urlPath.slice('/static/'.length));
  } else {
    // SPA fallback — serve index.html for any unknown path
    filePath = path.join(FRONTEND, 'index.html');
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
}).listen(PORT, () => console.log(`Test server on http://localhost:${PORT} → backend on :${BACKEND_PORT}`));
