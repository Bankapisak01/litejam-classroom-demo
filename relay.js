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
  let client = null, up = false, bi = 0;
  const deliver = (d, from) => inst.forEach(c => { if (c !== from) { try { c.dispatchEvent(new MessageEvent('message', { data: d })); } catch (e) { /* */ } } });
  const publish = d => { const s = JSON.stringify({ s: me, d }); if (up) client.publish(TOPIC, s); else { queue.push(s); if (queue.length > 50) queue.shift(); } };
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
  function connect() {
    if (!window.mqtt) { setTimeout(connect, 300); return; }
    client = window.mqtt.connect(BROKERS[bi % BROKERS.length], { clientId: 'lj-' + me, clean: true, reconnectPeriod: 2000, connectTimeout: 8000 });
    let ok = false;
    client.on('connect', () => { ok = true; up = true; client.subscribe(TOPIC); while (queue.length) client.publish(TOPIC, queue.shift()); paint(); });
    client.on('close', () => { up = false; paint(); if (!ok) { client.end(true); bi++; setTimeout(connect, 500); } });
    client.on('message', (t, buf) => { let m; try { m = JSON.parse(buf.toString()); } catch (e) { return; } if (!m || m.s === me) return; deliver(m.d, null); });
  }
  connect();
})();
