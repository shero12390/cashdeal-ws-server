// server.js — HTTP + WebSocket + Postgres + simple Rooms/Join (escrow)

// ---- core deps
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");
const { Pool } = require("pg");

// ---- config
const PORT = process.env.PORT || 3000;
const pool = new Pool({
  connectionString: process.env.DB_URL,
  ssl: { rejectUnauthorized: false }
});

// ---- tiny helpers
function sendJSON(res, code, obj) {
  res.writeHead(code, { "content-type": "application/json" });
  res.end(JSON.stringify(obj));
}

async function readJSON(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";
  try { return JSON.parse(raw); } catch { return {}; }
}

// Simple UUID helper (server-side)
function newId() { return crypto.randomUUID(); }

// ---- in-memory room registry (keeps it simple for now)
// room = { id, game, stake, maxPlayers, status:'open'|'full', players: [userId] }
const ROOMS = new Map();

// ---- DB: bootstrap minimal schema if missing
async function initDb() {
  const c = await pool.connect();
  try {
    await c.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

    await c.query(`
      CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await c.query(`
      CREATE TABLE IF NOT EXISTS wallets (
        user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
        balance NUMERIC(18,2) NOT NULL DEFAULT 0
      );
    `);

    await c.query(`
      CREATE TABLE IF NOT EXISTS rooms (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        game TEXT NOT NULL,
        stake NUMERIC(18,2) NOT NULL,
        max_players INT NOT NULL,
        status TEXT NOT NULL DEFAULT 'open',
        created_by UUID,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await c.query(`
      CREATE TABLE IF NOT EXISTS matches (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        room_id UUID REFERENCES rooms(id) ON DELETE SET NULL,
        game TEXT NOT NULL,
        stake NUMERIC(18,2) NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    await c.query(`
      CREATE TABLE IF NOT EXISTS match_players (
        match_id UUID REFERENCES matches(id) ON DELETE CASCADE,
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        result TEXT,
        PRIMARY KEY (match_id, user_id)
      );
    `);

    await c.query(`
      CREATE TABLE IF NOT EXISTS transactions (
        id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        user_id UUID REFERENCES users(id) ON DELETE CASCADE,
        kind TEXT NOT NULL,                    -- 'deposit','stake_debit','payout','refund'
        amount NUMERIC(18,2) NOT NULL,         -- positive numbers
        meta JSONB DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);

    console.log("✅ DB ready (tables ensured)");
  } finally {
    c.release();
  }
}

// ---- DB helpers (super small, safe)
async function ensureUserAndWallet(client, userId) {
  // create user row if missing
  await client.query(`INSERT INTO users(id) VALUES($1) ON CONFLICT(id) DO NOTHING`, [userId]);
  // create wallet if missing
  await client.query(`INSERT INTO wallets(user_id,balance) VALUES($1,0)
                      ON CONFLICT(user_id) DO NOTHING`, [userId]);
}

async function getBalance(client, userId) {
  const r = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
  return r.rows[0]?.balance ?? null;
}

async function debitStakeTx(client, userId, amount, meta) {
  // try atomic debit (fails if not enough)
  const r = await client.query(
    `UPDATE wallets SET balance = balance - $2
     WHERE user_id=$1 AND balance >= $2
     RETURNING balance`,
    [userId, amount]
  );
  if (r.rowCount === 0) throw new Error("INSUFFICIENT_FUNDS");
  await client.query(
    `INSERT INTO transactions(user_id, kind, amount, meta)
     VALUES($1,'stake_debit',$2,$3)`,
    [userId, amount, meta || {}]
  );
}

// ---- HTTP server (routes kept simple)
const server = http.createServer(async (req, res) => {
  try {
    // Health
    if (req.url === "/health") {
      return sendJSON(res, 200, { ok: true, ts: Date.now() });
    }

    // DB ping
    if (req.url === "/db-ping") {
      const r = await pool.query(`SELECT NOW() AS now`);
      return sendJSON(res, 200, { ok: true, dbTime: r.rows[0].now });
    }

    // List tables (debug)
    if (req.url === "/tables") {
      const r = await pool.query(
        `SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename`
      );
      return sendJSON(res, 200, { ok: true, tables: r.rows.map(x => x.tablename) });
    }

    // Tiny browser tester
    if (req.url === "/test") {
      res.writeHead(200, { "content-type": "text/html" });
      res.end(`<!doctype html><html><body style="font:14px system-ui">
<h3>WS test</h3>
<input id="url" style="width:420px" value="wss://${req.headers.host}/ws">
<button onclick="go()">Connect</button>
<pre id="log"></pre>
<script>
function log(m){document.getElementById('log').textContent+=m+'\\n'}
function go(){
  const u=document.getElementById('url').value;
  const ws=new WebSocket(u);
  ws.onopen=()=>{log('open'); ws.send('hello');};
  ws.onmessage=e=>log('msg: '+e.data);
  ws.onclose=()=>log('close');
}
</script></body></html>`);
      return;
    }

    // ---- API: create room (creator does NOT pay yet)
    // POST /rooms/create   JSON: { userId, game, stake, maxPlayers }
    if (req.method === "POST" && req.url === "/rooms/create") {
      const body = await readJSON(req);
      const userId = String(body.userId || "").trim();
      const game = String(body.game || "ludo");
      const stake = Number(body.stake || 0);
      const maxPlayers = Number(body.maxPlayers || 4);

      if (!userId) return sendJSON(res, 400, { ok: false, error: "MISSING_userId" });
      if (!(stake > 0)) return sendJSON(res, 400, { ok: false, error: "BAD_stake" });
      if (!(maxPlayers >= 2 && maxPlayers <= 8))
        return sendJSON(res, 400, { ok: false, error: "BAD_maxPlayers" });

      const roomId = newId();

      // persist basic room row
      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        await ensureUserAndWallet(c, userId);
        await c.query(
          `INSERT INTO rooms(id, game, stake, max_players, status, created_by)
           VALUES($1,$2,$3,$4,'open',$5)`,
          [roomId, game, stake, maxPlayers, userId]
        );
        await c.query("COMMIT");
      } catch (e) {
        await c.query("ROLLBACK");
        c.release();
        console.error("create room error:", e);
        return sendJSON(res, 500, { ok: false, error: "ROOM_CREATE_FAILED" });
      } finally {
        c.release();
      }

      // track in-memory too
      ROOMS.set(roomId, { id: roomId, game, stake, maxPlayers, status: "open", players: [] });

      return sendJSON(res, 200, {
        ok: true,
        room: { id: roomId, game, stake, maxPlayers, status: "open", players: [] }
      });
    }

    // ---- API: join room (escrow stake now)
    // POST /rooms/join   JSON: { userId, roomId }
    if (req.method === "POST" && req.url === "/rooms/join") {
      const body = await readJSON(req);
      const userId = String(body.userId || "").trim();
      const roomId = String(body.roomId || "").trim();
      if (!userId || !roomId) return sendJSON(res, 400, { ok: false, error: "MISSING_PARAMS" });

      const room = ROOMS.get(roomId);
      if (!room) return sendJSON(res, 404, { ok: false, error: "ROOM_NOT_FOUND" });
      if (room.status !== "open") return sendJSON(res, 400, { ok: false, error: "ROOM_CLOSED" });
      if (room.players.includes(userId))
        return sendJSON(res, 200, { ok: true, room, note: "ALREADY_JOINED" });
      if (room.players.length >= room.maxPlayers)
        return sendJSON(res, 400, { ok: false, error: "ROOM_FULL" });

      // debit stake inside a DB transaction
      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        await ensureUserAndWallet(c, userId);
        // check balance and debit
        await debitStakeTx(c, userId, room.stake, { roomId });
        const bal = await getBalance(c, userId);
        await c.query("COMMIT");

        // update in-memory list
        room.players.push(userId);
        if (room.players.length >= room.maxPlayers) room.status = "full";
        ROOMS.set(roomId, room);

        return sendJSON(res, 200, {
          ok: true,
          room,
          wallet: { balance: String(bal) }
        });
      } catch (e) {
        await c.query("ROLLBACK");
        c.release();
        if (String(e.message).includes("INSUFFICIENT_FUNDS"))
          return sendJSON(res, 402, { ok: false, error: "INSUFFICIENT_FUNDS" });
        console.error("join error:", e);
        return sendJSON(res, 500, { ok: false, error: "JOIN_FAILED" });
      } finally {
        c.release();
      }
    }

    // ---- API: list rooms (simple)
    // GET /rooms
    if (req.method === "GET" && req.url === "/rooms") {
      return sendJSON(res, 200, {
        ok: true,
        rooms: Array.from(ROOMS.values())
      });
    }

    // default 404
    return sendJSON(res, 404, { ok: false, error: "not_found" });
  } catch (err) {
    console.error("HTTP error:", err);
    return sendJSON(res, 500, { ok: false, error: "server_error" });
  }
});

// ---- WebSocket echo (unchanged)
const wss = new WebSocketServer({ noServer: true });
wss.on("connection", (ws) => {
  ws.on("message", (m) => ws.send(String(m)));
  ws.send("connected");
});
server.on("upgrade", (req, socket, head) => {
  if (req.url === "/ws") {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  } else {
    socket.destroy();
  }
});

// ---- boot
initDb()
  .then(() => {
    server.listen(PORT, () => console.log(`✅ Server on :${PORT}`));
  })
  .catch((err) => {
    console.error("❌ Startup error:", err);
    process.exit(1);
  });