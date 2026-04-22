const http = require('http');
const fs = require('fs');
const path = require('path');

const FRONTEND = path.join(__dirname, '..', 'frontend');
const PORT = 4321;

const MIME = {
  '.html': 'text/html', '.js': 'application/javascript',
  '.css': 'text/css', '.svg': 'image/svg+xml',
  '.png': 'image/png', '.ico': 'image/x-icon', '.json': 'application/json',
};

http.createServer((req, res) => {
  let filePath;
  if (req.url === '/' || req.url === '/index.html') {
    filePath = path.join(FRONTEND, 'index.html');
  } else if (req.url.startsWith('/static/')) {
    filePath = path.join(FRONTEND, req.url.slice('/static/'.length).split('?')[0]);
  } else {
    res.writeHead(404); res.end('Not found'); return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
    res.end(data);
  });
}).listen(PORT, () => console.log(`Test server on http://localhost:${PORT}`));
