// World Alchemy Strategy — онлайн-сервер без зависимостей (Node 18+).
// Запуск: node server.js   (порт PORT или 8080). Игра по адресу http://localhost:8080/
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = +process.env.PORT || 8080, DATA = process.env.DATA_FILE || path.join(__dirname, 'data.json'), GAME = path.join(__dirname, 'index.html');
let store = {}; try { store = JSON.parse(fs.readFileSync(DATA, 'utf8')); } catch (e) { }
let dirty = false; setInterval(() => { if (dirty) { dirty = false; try { fs.writeFileSync(DATA + '.tmp', JSON.stringify(store)); fs.renameSync(DATA + '.tmp', DATA); } catch (e) { console.error('save', e.message); } } }, 5000);
const clients = new Map(); /* cid -> {res, subs: Map(subId -> {path, type})} */
const parent = p => p.slice(0, p.lastIndexOf('/'));
const send = (c, o) => { try { c.res.write('data: ' + JSON.stringify(o) + '\n\n'); } catch (e) { } };
function notify(p, kind, data) {
  for (const c of clients.values()) for (const [id, s] of c.subs) {
    if (s.type === 'doc' && s.path === p) send(c, { sub: id, type: 'doc', exists: kind !== 'removed', data: kind === 'removed' ? null : data });
    else if (s.type === 'col' && parent(p) === s.path) send(c, { sub: id, type: 'changes', changes: [{ type: kind, id: p.slice(p.lastIndexOf('/') + 1), data: kind === 'removed' ? null : data }] });
  }
}
function put(p, d, merge) { const had = Object.prototype.hasOwnProperty.call(store, p); store[p] = merge ? Object.assign({}, store[p] || {}, d) : d; dirty = true; notify(p, had ? 'modified' : 'added', store[p]); }
function del(p) { if (!(p in store)) return; delete store[p]; dirty = true; notify(p, 'removed'); }
function body(req) { return new Promise((ok, no) => { let b = ''; req.on('data', d => { b += d; if (b.length > 2e6) { no(new Error('big')); req.destroy(); } }); req.on('end', () => { try { ok(b ? JSON.parse(b) : {}); } catch (e) { no(e); } }); }); }
const okPath = p => typeof p === 'string' && /^[A-Za-z0-9_\-\/А-Яа-я]{1,200}$/.test(p) && !p.includes('//') && !p.startsWith('/') && !p.endsWith('/');
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET,POST,OPTIONS' };
const J = (res, code, o) => { res.writeHead(code, Object.assign({ 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }, CORS)); res.end(JSON.stringify(o)); };
http.createServer(async (req, res) => {
  const u = new URL(req.url, 'http://x'); const pn = u.pathname;
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }
  try {
    if (req.method === 'GET' && (pn === '/' || pn === '/index.html')) { res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' }); return fs.createReadStream(GAME).pipe(res); }
    if (req.method === 'GET' && pn === '/api/stream') {
      const cid = u.searchParams.get('cid') || crypto.randomUUID(); res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-store', Connection: 'keep-alive', 'X-Accel-Buffering': 'no', 'Access-Control-Allow-Origin': '*' }); res.write('retry: 1000\n\n');
      const old = clients.get(cid); const c = { res, subs: old ? old.subs : new Map() }; clients.set(cid, c);
      for (const [id, s] of c.subs) snapshot(c, id, s); /* переподключение: повторно отдать состояние */
      const hb = setInterval(() => { try { res.write(': hb\n\n'); } catch (e) { } }, 20000); req.on('close', () => { clearInterval(hb); if (clients.get(cid) === c) clients.delete(cid); }); return;
    }
    if (req.method === 'POST' && pn === '/api/sub') { const b = await body(req); const c = clients.get(b.cid); if (!c) return J(res, 409, { error: 'no stream' }); if (!okPath(b.path) || !['doc', 'col'].includes(b.type)) return J(res, 400, { error: 'bad' }); const s = { path: b.path, type: b.type }; c.subs.set(String(b.sub), s); snapshot(c, String(b.sub), s); return J(res, 200, { ok: 1 }); }
    if (req.method === 'POST' && pn === '/api/unsub') { const b = await body(req); const c = clients.get(b.cid); if (c) c.subs.delete(String(b.sub)); return J(res, 200, { ok: 1 }); }
    if (req.method === 'POST' && pn === '/api/get') { const b = await body(req); if (!okPath(b.path)) return J(res, 400, { error: 'bad' }); return J(res, 200, { exists: b.path in store, data: store[b.path] === undefined ? null : store[b.path] }); }
    if (req.method === 'POST' && pn === '/api/write') {
      const b = await body(req); if (!okPath(b.path) || b.path.split('/').length % 2) return J(res, 400, { error: 'bad path' });
      if (b.op === 'delete') del(b.path); else if (b.op === 'set' || b.op === 'update') { if (b.data == null || typeof b.data !== 'object' || JSON.stringify(b.data).length > 200000) return J(res, 400, { error: 'bad data' }); if (Object.keys(store).length > 200000) return J(res, 507, { error: 'full' }); put(b.path, b.data, b.op === 'update'); } else return J(res, 400, { error: 'bad op' });
      return J(res, 200, { ok: 1 });
    }
    if (pn === '/health') return J(res, 200, { ok: 1, docs: Object.keys(store).length, clients: clients.size });
    res.writeHead(404); res.end('not found');
  } catch (e) { try { J(res, 500, { error: String(e.message || e) }); } catch (x) { } }
}).listen(PORT, () => console.log('World Alchemy online: http://localhost:' + PORT + '/'));
function snapshot(c, id, s) {
  if (s.type === 'doc') send(c, { sub: id, type: 'doc', exists: s.path in store, data: store[s.path] === undefined ? null : store[s.path] });
  else { const ch = []; for (const k in store) if (parent(k) === s.path) ch.push({ type: 'added', id: k.slice(k.lastIndexOf('/') + 1), data: store[k] }); send(c, { sub: id, type: 'changes', changes: ch, initial: true }); }
}
