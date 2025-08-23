// server.js
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });

console.log("Cashdeal WS Server running on port", PORT);

// ---------------- In‑memory state ----------------
// rooms[room] = { players: [name,...], state: { name: {...payload} } }
const rooms = Object.create(null);
// queues[room] = [ws, ws, ...]   // waiting list for 1v1
const queues = Object.create(null);

// ---------------- helpers ----------------
function ensureRoom(room) {
  if (!rooms[room]) rooms[room] = { players: [], state: {} };
  if (!queues[room]) queues[room] = [];
}

function send(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch (_) {}
}

function broadcast(room, obj) {
  const msg = JSON.stringify(obj);
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN && c.room === room) {
      try { c.send(msg); } catch (_) {}
    }
  });
}

function removeFromQueue(ws) {
  const room = ws.room;
  if (!room || !queues[room]) return;
  const q = queues[room];
  const idx = q.indexOf(ws);
  if (idx >= 0) q.splice(idx, 1);
  ws.inQueue = false;
}

function uid() {
  // tiny unique id: yymmddHHMMSS-xxxx
  const d = new Date();
  const pad = (n)=> String(n).padStart(2,"0");
  const ts = d.getUTCFullYear().toString().slice(2)
           + pad(d.getUTCMonth()+1) + pad(d.getUTCDate())
           + pad(d.getUTCHours()) + pad(d.getUTCMinutes())
           + pad(d.getUTCSeconds());
  return ts + "-" + Math.random().toString(16).slice(2,6);
}

function pairIfPossible(room) {
  const q = queues[room];
  if (!q) return;

  // prune closed/room-mismatched sockets
  for (let i = q.length - 1; i >= 0; i--) {
    const s = q[i];
    if (!s || s.readyState !== WebSocket.OPEN || s.room !== room) q.splice(i, 1);
  }

  if (q.length >= 2) {
    const p1 = q.shift();
    const p2 = q.shift();
    if (!p1 || !p2) return;

    p1.inQueue = false;
    p2.inQueue = false;

    const players = [p1.name || "PlayerA", p2.name || "PlayerB"];
    const matchId = uid();

    // 👇 EXACT shape your Android client expects
    const payload = { type: "match_found", mode: "1v1", room, players, matchId };

    send(p1, payload);
    send(p2, payload);

    // Optional: clear any per-player state for this match
    // rooms[room].state[p1.name] = {};
    // rooms[room].state[p2.name] = {};

    broadcast(room, { type: "state", room, state: rooms[room].state });
    console.log(`[match] room=${room} ${players.join(" vs ")} #${matchId}`);
  }
}

// ---------------- heartbeat (avoid ghost clients) ----------------
function heartbeat() { this.isAlive = true; }
wss.on("connection", (ws) => { ws.isAlive = true; ws.on("pong", heartbeat); });
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false; ws.ping(() => {});
  });
}, 30000);

// ---------------- main connection handler ----------------
wss.on("connection", (ws) => {
  ws.room = null;
  ws.name = null;
  ws.inQueue = false;

  send(ws, { type: "hello", msg: "Welcome to Cashdeal WS" });

  ws.on("message", (raw) => {
    let data;
    try { data = JSON.parse(raw); } catch (_) { return send(ws, { type: "error", msg: "Invalid JSON" }); }

    const t = data.type;

    // --- JOIN ---
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

      send(ws, { type: "joined", room, you: name, players: rooms[room].players });
      broadcast(room, { type: "state", room, state: rooms[room].state });
      return;
    }

    // --- MOVE (optional per-game state sync) ---
    if (t === "move") {
      const room = data.room || ws.room;
      if (!room || !rooms[room] || !ws.name) return send(ws, { type: "error", msg: "join first" });

      rooms[room].state[ws.name] = data.payload || {};
      return broadcast(room, { type: "state", room, state: rooms[room].state });
    }

    // --- QUEUE (1v1) ---
    if (t === "queue") {
      const room = data.room || ws.room;
      if (!room || !rooms[room] || !ws.name) return send(ws, { type: "error", msg: "join first" });
      ensureRoom(room);

      if (!ws.inQueue) {
        queues[room].push(ws);
        ws.inQueue = true;
        send(ws, { type: "queue", status: "waiting", room, mode: data.mode || "1v1" });
        console.log(`[queue] ${ws.name} in room=${room} (len=${queues[room].length})`);
      }

      pairIfPossible(room);
      return;
    }

    // --- QUEUE CANCEL ---
    if (t === "queue_cancel") {
      if (ws.inQueue) {
        removeFromQueue(ws);
        send(ws, { type: "queue", status: "cancelled", room: ws.room });
      }
      return;
    }

    send(ws, { type: "error", msg: "unknown type" });
  });

  ws.on("close", () => {
    if (ws.inQueue) removeFromQueue(ws);

    const room = ws.room;
    const name = ws.name;
    if (room && rooms[room]) {
      const list = rooms[room].players;
      const i = list.indexOf(name);
      if (i >= 0) list.splice(i, 1);
      if (rooms[room].state && name && rooms[room].state[name]) delete rooms[room].state[name];
      broadcast(room, { type: "state", room, state: rooms[room].state });

      if (rooms[room].players.length === 0) { delete rooms[room]; delete queues[room]; }
    }
  });
});
