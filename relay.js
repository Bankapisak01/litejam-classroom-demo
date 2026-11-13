/* Litejam Classroom demo · cross-device relay
   The prototypes talk over BroadcastChannel "litejam-classroom-ws" (same browser only). On the demo site this shim swaps that
   channel for a public MQTT-over-WebSocket broker, topic litejam-cc-demo/<ROOM>, so a teacher laptop and student phones/tablets
   with the same room code pair over the internet. Room code: ?room=XXXX (saved in localStorage) · sample data only. */
(function () {
  const NAME = 'litejam-classroom-ws', KEY = 'lj-demo-room';
  const qs = new URLSearchParams(location.search);
  let room = (qs.get('room') || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
  if (!room) { try { room = new URLSearchParams(window.top.location.search).get('room') || ''; } catch (e) { /* */ } }
  if (!room) { try { room = localStorage.getItem(KEY) || ''; } catch (e) { /* */ } }
  room = String(room).toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
  if (!room) return;   // no room → plain BroadcastChannel (same-browser demo)
  try { localStorage.setItem(KEY, room); } catch (e) { /* */ }
  window.LJ_DEMO_ROOM = room;
  const BROKERS = ['wss://broker.hivemq.com:8884/mqtt', 'wss://broker.emqx.io:8084/mqtt'];
  const TOPIC = 'litejam-cc-demo/' + room, me = Math.random().toString(36).slice(2, 10);
  const Native = window.BroadcastChannel, inst = new Set(), queue = [];
  /* both public brokers at once: publish to every connected one, drop duplicates by sender + sequence number */
  const clients = [], seen = new Map(); let seq = 0;
  const isUp = () => clients.some(c => c.connected);
  let up = false;
  const deliver = (d, from) => inst.forEach(c => { if (c !== from) { try { c.dispatchEvent(new MessageEvent('message', { data: d })); } catch (e) { /* */ } } });
  const publish = d => { const s = JSON.stringify({ s: me, n: ++seq, d }); if (isUp()) clients.forEach(c => { if (c.connected) c.publish(TOPIC, s); }); else { queue.push(s); if (queue.length > 50) queue.shift(); } };
  class RelayChannel extends EventTarget {
    constructor(n) { super(); this.name = n; this._onm = null; if (n === NAME) inst.add(this); else this._nat = new Native(n); }
    set onmessage(f) { if (this._nat) { this._nat.onmessage = f; return; } if (this._onm) this.removeEventListener('message', this._onm); this._onm = f; if (f) this.addEventListener('message', f); }
    get onmessage() { return this._nat ? this._nat.onmessage : this._onm; }
    postMessage(d) { if (this._nat) return this._nat.postMessage(d); const c = JSON.parse(JSON.stringify(d)); setTimeout(() => deliver(c, this), 0); publish(c); }
    close() { if (this._nat) this._nat.close(); inst.delete(this); }
  }
  window.BroadcastChannel = RelayChannel;
  /* status badge */
  let badge = null; const paint = () => { if (badge && !badge.isConnected && document.body) document.body.appendChild(badge); if (!badge && document.body) { badge = document.createElement('div'); badge.id = 'lj-relay'; badge.style.cssText = 'position:fixed;right:8px;bottom:8px;z-index:99999;font:700 11px/1 system-ui,sans-serif;padding:6px 9px;border-radius:99px;background:rgba(20,17,24,.88);color:#fff;border:1px solid rgba(255,255,255,.2);pointer-events:none'; document.body.appendChild(badge); }
    if (badge) badge.innerHTML = '<span style="color:' + (up ? '#ADFE00' : '#FFCC00') + '">●</span> ห้อง ' + room + (up ? ' · online' : ' · กำลังเชื่อมต่อ…'); };
  if (window.top === window.self) { document.addEventListener('DOMContentLoaded', paint); setInterval(paint, 2000); }
  function onMsg(t, buf) { let m; try { m = JSON.parse(buf.toString()); } catch (e) { return; } if (!m || m.s === me) return;
    const last = seen.get(m.s) || 0; if (m.n && m.n <= last && last - m.n < 5000) { if (m.n === last || seen.has(m.s + ':' + m.n)) return; }
    if (seen.has(m.s + ':' + m.n)) return; seen.set(m.s + ':' + m.n, 1); seen.set(m.s, Math.max(last, m.n || 0));
    if (seen.size > 4000) { const k = [...seen.keys()]; k.slice(0, 2000).forEach(x => seen.delete(x)); }
    deliver(m.d, null); }
  function connect() {
    if (!window.mqtt) { setTimeout(connect, 300); return; }
    BROKERS.forEach((url, i) => {
      const c = window.mqtt.connect(url, { clientId: 'lj-' + me + '-' + i, clean: true, reconnectPeriod: 3000, connectTimeout: 8000 });
      c.on('connect', () => { c.subscribe(TOPIC); while (queue.length) { const s = queue.shift(); clients.forEach(x => { if (x.connected) x.publish(TOPIC, s); }); } up = true; paint(); });
      c.on('close', () => { up = isUp(); paint(); });
      c.on('error', () => { /* reconnects by itself */ });
      c.on('message', onMsg); clients.push(c);
    });
  }
  connect();
})();
