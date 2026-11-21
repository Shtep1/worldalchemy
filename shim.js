/* Локальная замена claude.use('db'|'user') для автономной версии: общается с online/server.js */
(() => {
  if (window.claude) return;
  let uid; try { uid = localStorage.getItem('wa_uid'); if (!uid) { uid = 'u' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4); localStorage.setItem('wa_uid', uid); } } catch (e) { uid = 'u' + Math.random().toString(36).slice(2, 10); }
  const BASE = (new URLSearchParams(location.search).get('server') || window.WA_SERVER || '').replace(/\/$/, '');
  const cid = 'c' + Math.random().toString(36).slice(2) + Date.now().toString(36); let es = null, nsub = 0; const subs = new Map(); let ready = null;
  const post = (p, o) => fetch(BASE + p, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(o), cache: 'no-store' }).then(r => r.json().then(j => { if (!r.ok) throw new Error(j.error || r.status); return j; }));
  const open = () => { if (ready) return ready; ready = new Promise(res => { es = new EventSource(BASE + '/api/stream?cid=' + cid); es.onopen = () => res(); es.onmessage = m => { let o; try { o = JSON.parse(m.data); } catch (e) { return; } const s = subs.get(String(o.sub)); if (s) s.on(o); }; es.onerror = () => { /* EventSource переподключается сам; сервер отдаст состояние заново */ }; }); return ready; };
  const snapDoc = (id, d) => ({ id, exists: d != null, data: () => d == null ? undefined : d, metadata: {} });
  const subscribe = async (path, type, on) => { await open(); const id = String(++nsub); subs.set(id, { on }); await post('/api/sub', { cid, sub: id, path, type }); return () => { subs.delete(id); post('/api/unsub', { cid, sub: id }).catch(() => { }); }; };
  const db = {
    doc(path) {
      if (String(path).split('/').length % 2) throw new Error('db doc(): document paths must have an even number of segments: ' + path); const idd = String(path).split('/').pop();
      return { path, id: idd, async get() { const r = await post('/api/get', { path }); return snapDoc(idd, r.exists ? r.data : null); }, set(d) { return post('/api/write', { op: 'set', path, data: d }); }, update(d) { return post('/api/write', { op: 'update', path, data: d }); }, delete() { return post('/api/write', { op: 'delete', path }); },
        onSnapshot(cb, err) { let un = null, dead = false; subscribe(path, 'doc', o => cb(snapDoc(idd, o.exists ? o.data : null))).then(f => { if (dead) f(); else un = f; }).catch(e => err && err(e)); return () => { dead = true; if (un) un(); }; } };
    },
    collection(path) {
      if (!(String(path).split('/').length % 2)) throw new Error('db collection(): odd segments required: ' + path);
      return { path, onSnapshot(cb, err) { let un = null, dead = false; subscribe(path, 'col', o => { const ch = o.changes.map(c => ({ type: c.type, doc: snapDoc(c.id, c.data) })); if (ch.length) cb({ docChanges: () => ch, docs: [], size: ch.length }); }).then(f => { if (dead) f(); else un = f; }).catch(e => err && err(e)); return () => { dead = true; if (un) un(); }; } };
    }
  };
  const user = { id: async () => uid, isOwner: () => true, canEdit: () => true, can: () => true };
  window.claude = { use: async n => n === 'db' ? db : n === 'user' ? user : null };
})();
