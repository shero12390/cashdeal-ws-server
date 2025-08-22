// server.js
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });

console.log("Cashdeal WS Server running on port", PORT);

// In-memory state
// rooms[room] = { players: [name,...], state: { name: {...payload} } }
const rooms = Object.create(null);

// queues[room] = [ws, ws, ...]   // waiting list for 1v1 in that room
const queues = Object.create(null);

// ---- helpers ----
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

function pairIfPossible(room) {
  const q = queues[room];
  // remove any closed sockets at the front
  for (let i = q.length - 1; i >= 0; i--) {
    const s = q[i];
    if (!s || s.readyState !== WebSocket.OPEN || s.room !== room) {
      q.splice(i, 1);
    }
  }
  if (q.length >= 2) {
    const p1 = q.shift();
    const p2 = q.shift();
    if (!p1 || !p2) return;

    p1.inQueue = false;
    p2.inQueue = false;

    // notify both players a match is found (stays in same room)
    send(p1, { type: "match_found", mode: "1v1", room, opponent: p2.name || "Opponent" });
    send(p2, { type: "match_found", mode: "1v1", room, opponent: p1.name || "Opponent" });

    // optional: reset any duel substate here if you want
    // rooms[room].state[p1.name] = {};
    // rooms[room].state[p2.name] = {};
    broadcast(room, { type: "state", room, state: rooms[room].state });
  }
}

// ---- main connection handler ----
wss.on("connection", (ws) => {
  // identify this socket
  ws.room = null;
  ws.name = null;
  ws.inQueue = false;

  // A friendly hello (optional)
  send(ws, { type: "hello", msg: "Welcome to Cashdeal WS" });

  ws.on("message", (raw) => {
    let data;
    try {
      data = JSON.parse(raw);
    } catch (e) {
      return send(ws, { type: "error", msg: "Invalid JSON" });
    }

    const t = data.type;

    // --- JOIN ---
    if (t === "join") {
      const room = String(data.room || "").trim();
      const name = String(data.name || "").trim();
      if (!room || !name) return send(ws, { type: "error", msg: "room and name required" });

      ensureRoom(room);

      // leave previous queue if any
      if (ws.inQueue) removeFromQueue(ws);

      ws.room = room;
      ws.name = name;

      // add to room player list if not present
      const list = rooms[room].players;
      if (!list.includes(name)) list.push(name);

      // reply to this client
      send(ws, { type: "joined", room, you: name, players: rooms[room].players });

      // notify everyone of current state
      broadcast(room, { type: "state", room, state: rooms[room].state });
      return;
    }

    // --- MOVE ---
    if (t === "move") {
      const room = data.room || ws.room;
      if (!room || !rooms[room] || !ws.name) {
        return send(ws, { type: "error", msg: "join first" });
      }
      const payload = data.payload || {};
      rooms[room].state[ws.name] = payload;
      return broadcast(room, { type: "state", room, state: rooms[room].state });
    }

    // --- QUEUE (1v1) ---
    if (t === "queue") {
      // requires join first
      const room = data.room || ws.room;
      if (!room || !rooms[room] || !ws.name) {
        return send(ws, { type: "error", msg: "join first" });
      }
      ensureRoom(room);

      // avoid duplicates
      if (!ws.inQueue) {
        queues[room].push(ws);
        ws.inQueue = true;
        send(ws, { type: "queue", status: "waiting", room, mode: data.mode || "1v1" });
      }

      // try to pair
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

    // Unknown type
    send(ws, { type: "error", msg: "unknown type" });
  });

  ws.on("close", () => {
    // remove from queue
    if (ws.inQueue) removeFromQueue(ws);

    // remove from room
    const room = ws.room;
    const name = ws.name;
    if (room && rooms[room]) {
      const list = rooms[room].players;
      const i = list.indexOf(name);
      if (i >= 0) list.splice(i, 1);
      // (optional) clear personal state
      if (rooms[room].state && name && rooms[room].state[name]) {
        delete rooms[room].state[name];
      }
      // broadcast updated state
      broadcast(room, { type: "state", room, state: rooms[room].state });

      // optional: delete empty room
      if (rooms[room].players.length === 0) {
        delete rooms[room];
        delete queues[room];
      }
    }
  });
});
