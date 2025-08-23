// server.js  — HTTP + WebSocket (upgrade), detailed logs, pairing, health + tiny test page
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;

// ---------- HTTP server (for platforms that require HTTP routing) ----------
const server = http.createServer((req, res) => {
  if (req.url === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, ts: Date.now() }));
    return;
  }

  // Minimal in-browser tester (open two tabs to simulate two clients)
  if (req.url === "/test") {
    res.writeHead(200, { "content-type": "text/html" });
    res.end(`<!doctype html>
<html><body style="font:14px system-ui">
<h3>WS test</h3>
<input id="url" style="width:420px" value="wss://${req.headers.host}"><button id="c">connect</button>
<pre id="log" style="height:260px;overflow:auto;background:#111;color:#0f0;padding:8px"></pre>
<button id="join">join</button>
<button id="queue">queue</button>
<script>
let ws, room="double_dices", name="web_"+Math.floor(Math.random()*999);
const log = (...a)=>{ const el=document.getElementById('log'); el.textContent+=a.join(" ")+"\\n"; el.scrollTop=1e9;}
document.getElementById('c').onclick=()=>{
  const u=document.getElementById('url').value;
  ws=new WebSocket(u.replace(/^http/,'ws'));
  ws.onopen=()=>log("open");
  ws.onclose=()=>log("close");
  ws.onerror=e=>log("error", e && (e.message||e));
  ws.onmessage=m=>log("recv", m.data);
};
document.getElementById('join').onclick=()=>ws&&ws.send(JSON.stringify({type:"join", room, name}));
document.getElementById('queue').onclick=()=>ws&&ws.send(JSON.stringify({type:"queue", mode:"1v1", room}));
</script>
</body></html>`);
    return;
  }

  // default
  res.writeHead(200, { "content-type": "text/plain" });
  res.end("Cashdeal WS Server. Try /health or /test");
});

server.listen(PORT, () => {
  console.log("[boot] HTTP listening on", PORT);
});

// ---------- WS server (attached to HTTP; works on Render/Railway/Heroku/Ngrok) ----------
const wss = new WebSocketServer({ server, path: "/" });
console.log("[boot] WS attached at /");

const rooms = Object.create(null);     // { [room]: { players: [], state: {} } }
const queues = Object.create(null);    // { [room]: [ws, ws, ...] }

function ensureRoom(room) {
  if (!rooms[room]) rooms[room] = { players: [], state: {} };
  if (!queues[room]) queues[room] = [];
}
function send(ws, obj) { try { ws.send(JSON.stringify(obj)); } catch {} }
function broadcast(room, obj) {
  const s = JSON.stringify(obj);
  wss.clients.forEach(c => {
    if (c.readyState === 1 && c.room === room) try { c.send(s); } catch {}
  });
}

function removeFromQueue(ws) {
  if (!ws || !ws.room) return;
  const q = queues[ws.room]; if (!q) return;
  const i = q.indexOf(ws);
  if (i >= 0) q.splice(i, 1);
  ws.inQueue = false;
}

function pairIfPossible(room) {
  const q = queues[room];
  if (!q) return;

  // cleanup
  for (let i = q.length - 1; i >= 0; i--) {
    const s = q[i];
    if (!s || s.readyState !== 1 || s.room !== room) q.splice(i, 1);
  }

  if (q.length >= 2) {
    const p1 = q.shift();
    const p2 = q.shift();
    if (!p1 || !p2) return;

    p1.inQueue = false;
    p2.inQueue = false;

    const matchId = crypto.randomBytes(6).toString("hex");
    const players = [p1.name || "P1", p2.name || "P2"];

    console.log(`[match] room=${room} ${players[0]} vs ${players[1]} #${matchId}`);

    // Match payload both clients expect
    const payload = { type: "match_found", mode: "1v1", room, players, matchId };
    send(p1, payload);
    send(p2, payload);

    // optional: broadcast state snapshot (no harm)
    broadcast(room, { type: "state", room, state: rooms[room].state });
  }
}

// Keep-alive to prevent idle disconnects on some platforms
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
}, 30000);

// Main connection
wss.on("connection", (ws, req) => {
  ws.room = null;
  ws.name = null;
  ws.inQueue = false;
  ws.isAlive = true;

  console.log("[conn] from", req.socket.remoteAddress);
  send(ws, { type: "hello", msg: "Welcome to Cashdeal WS" });

  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", raw => {
    let data;
    try { data = JSON.parse(raw); } catch {
      return send(ws, { type: "error", msg: "Invalid JSON" });
    }
    const t = data.type;

    if (t === "join") {
      const room = String(data.room || "").trim();
      const name = String(data.name || "").trim();
      if (!room || !name) return send(ws, { type: "error", msg: "room and name required" });

      ensureRoom(room);
      if (ws.inQueue) removeFromQueue(ws);

      ws.room = room;
      ws.name = name;

      const list = rooms[room].players;
      if (!list.includes(name)) list.push(name);

      console.log(`[join] ${name} -> ${room} players=${list.length}`);
      send(ws, { type: "joined", room, you: name, players: list });
      broadcast(room, { type: "state", room, state: rooms[room].state });
      return;
    }

    if (t === "queue") {
      const room = data.room || ws.room;
      if (!room || !rooms[room] || !ws.name) {
        return send(ws, { type: "error", msg: "join first" });
      }
      ensureRoom(room);
      if (!ws.inQueue) {
        queues[room].push(ws);
        ws.inQueue = true;
        console.log(`[queue] ${ws.name} in room=${room} (len=${queues[room].length})`);
        send(ws, { type: "queue", status: "waiting", room, mode: data.mode || "1v1" });
      }
      pairIfPossible(room);
      return;
    }

    if (t === "queue_cancel") {
      if (ws.inQueue) {
        removeFromQueue(ws);
        console.log(`[queue_cancel] ${ws.name} room=${ws.room}`);
        send(ws, { type: "queue", status: "cancelled", room: ws.room });
      }
      return;
    }

    if (t === "move") {
      const room = data.room || ws.room;
      if (!room || !rooms[room] || !ws.name) {
        return send(ws, { type: "error", msg: "join first" });
      }
      rooms[room].state[ws.name] = data.payload || {};
      return broadcast(room, { type: "state", room, state: rooms[room].state });
    }

    send(ws, { type: "error", msg: "unknown type" });
  });

  ws.on("close", () => {
    if (ws.inQueue) removeFromQueue(ws);
    const room = ws.room, name = ws.name;
    if (room && rooms[room]) {
      const list = rooms[room].players;
      const i = list.indexOf(name);
      if (i >= 0) list.splice(i, 1);
      if (rooms[room].state && name && rooms[room].state[name]) {
        delete rooms[room].state[name];
      }
      console.log(`[close] ${name||"?"} left ${room}; players=${list.length}`);
      broadcast(room, { type: "state", room, state: rooms[room].state });
      if (list.length === 0) { delete rooms[room]; delete queues[room]; }
    }
  });
});
