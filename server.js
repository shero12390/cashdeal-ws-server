// Cashdeal WS Server — 1v1 Tap Duel prototype
// No external libs beyond `ws`.
// Message spec documented in comments below.

const WebSocket = require("ws");
const PORT = process.env.PORT || 3000;

const COUNTDOWN_S = parseInt(process.env.COUNTDOWN_S || "3", 10); // seconds
const MATCH_S     = parseInt(process.env.MATCH_S     || "10", 10); // seconds
const STATE_TICK_MS = 300; // how often we broadcast scores during the match

const wss = new WebSocket.Server({ port: PORT }, () => {
  console.log("Cashdeal WS Server running on port", PORT);
});

// --- State ------------------------------------------------------------------
/** waiting players (FIFO) */
let queue = [];
/** roomId -> room */
const rooms = new Map();

function genId(prefix = "r") {
  return `${prefix}${Math.random().toString(36).slice(2, 8)}${Date.now()
    .toString(36)
    .slice(-2)}`;
}

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(room, obj) {
  send(room.A, obj);
  send(room.B, obj);
}

function removeFromQueue(ws) {
  queue = queue.filter((x) => x !== ws);
}

function endRoom(room, payload) {
  if (room._ticker) clearInterval(room._ticker);
  rooms.delete(room.id);

  // tell clients
  broadcast(room, { type: "end", ...payload });

  // release client meta
  [room.A, room.B].forEach((ws) => {
    if (!ws) return;
    if (ws.meta) {
      ws.meta.state = "idle";
      ws.meta.roomId = null;
      ws.meta.side = null;
    }
  });
}

// --- Room lifecycle ---------------------------------------------------------
function makeRoom(a, b) {
  const id = genId();
  const room = {
    id,
    A: a,
    B: b,
    scores: { A: 0, B: 0 },
    phase: "countdown",
    startAt: Date.now() + COUNTDOWN_S * 1000,
    endAt: null,
    _ticker: null,
  };
  rooms.set(id, room);

  a.meta = { ...(a.meta || {}), state: "playing", roomId: id, side: "A" };
  b.meta = { ...(b.meta || {}), state: "playing", roomId: id, side: "B" };

  // announce match + start
  const nowSec = Math.floor(Date.now() / 1000);
  const startMsg = {
    type: "start",
    countdown: COUNTDOWN_S,
    duration: MATCH_S,
    serverTs: nowSec,
  };
  // Let clients know they matched first
  const matchMsgA = {
    type: "match",
    room: id,
    you: "A",
    opp: { userId: b.meta?.userId || "guest" },
  };
  const matchMsgB = {
    type: "match",
    room: id,
    you: "B",
    opp: { userId: a.meta?.userId || "guest" },
  };
  send(a, matchMsgA);
  send(b, matchMsgB);
  broadcast(room, startMsg);

  // schedule transitions
  setTimeout(() => beginPlay(room), COUNTDOWN_S * 1000);
}

function beginPlay(room) {
  if (!rooms.has(room.id)) return; // already gone
  room.phase = "playing";
  room.endAt = Date.now() + MATCH_S * 1000;

  // periodic state broadcaster
  room._ticker = setInterval(() => {
    const timeLeft = Math.max(0, (room.endAt - Date.now()) / 1000);
    broadcast(room, {
      type: "state",
      A: room.scores.A,
      B: room.scores.B,
      timeLeft: Number(timeLeft.toFixed(2)),
    });
    if (timeLeft <= 0) {
      clearInterval(room._ticker);
      conclude(room, "time");
    }
  }, STATE_TICK_MS);
}

function conclude(room, reason) {
  // decide winner
  let winner = "draw";
  if (room.scores.A > room.scores.B) winner = "A";
  else if (room.scores.B > room.scores.A) winner = "B";

  endRoom(room, { winner, A: room.scores.A, B: room.scores.B, reason });
}

function opponent(ws, room) {
  if (!room) return null;
  return ws === room.A ? room.B : room.A;
}

// --- Queue pairing -----------------------------------------------------------
function tryPair() {
  while (queue.length >= 2) {
    const a = queue.shift();
    const b = queue.shift();
    if (!a || a.readyState !== WebSocket.OPEN) continue;
    if (!b || b.readyState !== WebSocket.OPEN) continue;
    makeRoom(a, b);
  }
}

// --- Connection handling -----------------------------------------------------
wss.on("connection", (ws) => {
  ws.id = genId("c");
  ws.meta = { state: "idle", userId: `guest-${ws.id.slice(-4)}` };
  ws.isAlive = true;

  // keepalive
  ws.on("pong", () => (ws.isAlive = true));

  send(ws, { type: "hello", userId: ws.meta.userId, server: "cashdeal" });

  ws.on("message", (data) => {
    let msg = null;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      // allow legacy plain text to keep earlier echo tests happy
      return send(ws, { type: "error", message: "Use JSON messages" });
    }

    const t = msg.type;
    if (t === "ping") return send(ws, { type: "pong" });

    if (t === "hello") {
      if (typeof msg.userId === "string" && msg.userId.trim() !== "") {
        ws.meta.userId = msg.userId.trim();
      }
      return; // nothing else to do
    }

    if (t === "queue") {
      if (ws.meta.state === "playing") {
        return send(ws, { type: "error", message: "Already in a match" });
      }
      if (!queue.includes(ws)) queue.push(ws);
      ws.meta.state = "queued";
      send(ws, { type: "queued", pos: queue.indexOf(ws) + 1 });
      tryPair();
      return;
    }

    if (t === "leave") {
      removeFromQueue(ws);
      const room = rooms.get(ws.meta.roomId);
      if (room) {
        // forfeit
        const winner = ws.meta.side === "A" ? "B" : "A";
        endRoom(room, {
          winner,
          A: room?.scores?.A || 0,
          B: room?.scores?.B || 0,
          reason: "forfeit",
        });
      }
      ws.meta.state = "idle";
      return;
    }

    if (t === "tap") {
      const room = rooms.get(ws.meta.roomId);
      if (!room || room.phase !== "playing") return;
      // increment the correct side
      const side = ws.meta.side === "B" ? "B" : "A";
      room.scores[side] += 1;
      // (we rely on periodic state broadcast; can also send instant update if desired)
      return;
    }

    // Unknown
    send(ws, { type: "error", message: "Unknown type" });
  });

  ws.on("close", () => {
    removeFromQueue(ws);
    // if in room, opponent wins by forfeit
    const room = rooms.get(ws.meta.roomId);
    if (room) {
      const opp = opponent(ws, room);
      endRoom(room, {
        winner: opp === room.A ? "A" : "B",
        A: room?.scores?.A || 0,
        B: room?.scores?.B || 0,
        reason: "disconnect",
      });
    }
  });

  ws.on("error", () => {
    try { ws.close(); } catch (_) {}
  });
});

// server-wide heartbeat to drop dead sockets
const interval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      try { ws.terminate(); } catch (_) {}
      return;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  });
}, 30000);

wss.on("close", () => clearInterval(interval));

// -----------------------------------------------------------------------------
// Protocol summary (JSON):
//
// Client -> Server
//   { "type":"hello", "userId":"guest-1234" }
//   { "type":"queue", "game":"tap_duel" }
//   { "type":"leave" }
//   { "type":"tap" }
//   { "type":"ping" }
//
// Server -> Client
//   { "type":"hello", "userId":"guest-xxxx", "server":"cashdeal" }
//   { "type":"queued", "pos":1 }
//   { "type":"match", "room":"r42", "you":"A|B", "opp":{"userId":"..." } }
//   { "type":"start", "countdown":3, "duration":10, "serverTs":169... }
//   { "type":"state", "A":12, "B":9, "timeLeft":7.2 }
//   { "type":"end", "winner":"A|B|draw", "A":27, "B":25, "reason":"time|forfeit|disconnect" }
//   { "type":"error", "message":"..." }
//   { "type":"pong" }
// -----------------------------------------------------------------------------
