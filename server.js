// server.js — Cashdeal Multiplayer (Rooms + Shared State + Broadcast)
// Runtime: Node.js with only "ws" dependency
// How it works:
//  - Clients connect via WebSocket
//  - Join a room with:   {"type":"join","room":"demo1","name":"Alice"}
//  - Send state updates: {"type":"move","room":"demo1","payload":{"x":100,"y":250,"score":3}}
//  - Optional chat:      {"type":"chat","room":"demo1","text":"hi"}
//  - Ping/pong:          {"type":"ping"}
//  - Leave:              {"type":"leave","room":"demo1"}
// Server keeps per-room shared state (one entry per player name) and
// broadcasts updates to everyone in that room (not to other rooms).

const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const wss  = new WebSocket.Server({ port: PORT }, () => {
  console.log("Cashdeal WS Server running on port", PORT);
});

// In-memory store
// rooms: Map<roomId, { players: Map<ws, name>, state: Record<name, payload> }>
const rooms = new Map();

function send(ws, obj) {
  try { ws.send(typeof obj === "string" ? obj : JSON.stringify(obj)); } catch (_) {}
}

function ensureRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, { players: new Map(), state: {} });
  return rooms.get(roomId);
}

function broadcastRoom(roomId, obj, exceptWS = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const msg = typeof obj === "string" ? obj : JSON.stringify(obj);
  for (const client of room.players.keys()) {
    if (client !== exceptWS && client.readyState === WebSocket.OPEN) {
      try { client.send(msg); } catch (_) {}
    }
  }
}

function removeFromAllRooms(ws) {
  for (const [roomId, room] of rooms) {
    if (room.players.has(ws)) {
      const name = room.players.get(ws);
      room.players.delete(ws);
      delete room.state[name];

      // Notify others and sync state
      broadcastRoom(roomId, { type: "system", msg: `${name} left`, serverTs: Date.now() });
      broadcastRoom(roomId, { type: "state", state: room.state, serverTs: Date.now() });

      if (room.players.size === 0) rooms.delete(roomId);
    }
  }
}

wss.on("connection", (ws) => {
  ws.isAlive = true;                    // heartbeat flag
  ws.displayName = null;                // set after join
  ws.currentRooms = new Set();          // support multiple rooms if you want

  // greet client
  send(ws, { type: "hello", msg: "Connected to Cashdeal server", serverTs: Date.now() });

  // heartbeat replies
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return send(ws, { type: "error", msg: "Invalid JSON", serverTs: Date.now() }); }

    const t = msg.type;
    const now = Date.now();

    // ---- JOIN ROOM ----
    if (t === "join") {
      const roomId = String(msg.room || "").trim();
      const name   = String(msg.name || "").trim() || "Player";
      if (!roomId) return send(ws, { type: "error", msg: "room required", serverTs: now });

      const room = ensureRoom(roomId);
      room.players.set(ws, name);
      ws.displayName = name;
      ws.currentRooms.add(roomId);

      // Welcome the joiner with roster + current state
      send(ws, {
        type: "joined",
        room: roomId,
        you: name,
        players: Array.from(room.players.values()),
        serverTs: now
      });
      send(ws, { type: "state", room: roomId, state: room.state, serverTs: now });

      // Notify others (except the joiner)
      broadcastRoom(roomId, { type: "system", msg: `${name} joined`, serverTs: now }, ws);
      return;
    }

    // ---- LEAVE ROOM ----
    if (t === "leave") {
      const roomId = String(msg.room || "").trim();
      if (!roomId) return send(ws, { type: "error", msg: "room required", serverTs: now });

      const room = rooms.get(roomId);
      if (!room || !room.players.has(ws)) return;

      const name = room.players.get(ws);
      room.players.delete(ws);
      ws.currentRooms.delete(roomId);
      delete room.state[name];

      broadcastRoom(roomId, { type: "system", msg: `${name} left`, serverTs: now });
      broadcastRoom(roomId, { type: "state", room: roomId, state: room.state, serverTs: now });

      if (room.players.size === 0) rooms.delete(roomId);
      return;
    }

    // ---- MOVE / STATE UPDATE ----
    if (t === "move") {
      const roomId = String(msg.room || "").trim();
      const room = rooms.get(roomId);
      if (!room || !room.players.has(ws)) {
        return send(ws, { type: "error", msg: "join first", serverTs: now });
      }
      const name = room.players.get(ws);

      // payload can be any object: {x,y,score,angle,...}
      room.state[name] = msg.payload || {};
      // Broadcast the entire state so all clients render the same view
      return broadcastRoom(roomId, { type: "state", room: roomId, state: room.state, serverTs: now });
    }

    // ---- CHAT (optional) ----
    if (t === "chat") {
      const roomId = String(msg.room || "").trim();
      const room = rooms.get(roomId);
      if (!room || !room.players.has(ws)) return;
      const name = room.players.get(ws);

      return broadcastRoom(roomId, {
        type: "chat",
        room: roomId,
        from: name,
        text: String(msg.text || ""),
        serverTs: now
      });
    }

    // ---- PING -> PONG ----
    if (t === "ping") {
      return send(ws, { type: "pong", serverTs: now });
    }

    // Unknown
    send(ws, { type: "error", msg: `Unknown type: ${t}`, serverTs: now });
  });

  ws.on("close", () => removeFromAllRooms(ws));
  ws.on("error", () => removeFromAllRooms(ws));
});

// Heartbeat to keep free tiers healthy and drop dead sockets
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (_) {}
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  });
}, 25000);
