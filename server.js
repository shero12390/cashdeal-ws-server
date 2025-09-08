// server.js — HTTP + WebSocket + Postgres + Rooms/Escrow (Render-friendly)

const http = require("http");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;

// =============== Utilities ===============
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
function uuid() { return crypto.randomUUID(); }

// =============== Postgres ===============
const pool = new Pool({
  connectionString: process.env.DB_URL, // set in Render env
  ssl: { rejectUnauthorized: false }    // required on Render PG
});
const q = (sql, params=[]) => pool.query(sql, params);

// =============== DB Migrations (6 tables) ===============
async function initDb() {
  await pool.connect();
  console.log("✅ Connected to Postgres");

  await q(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

  await q(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS wallets (
      user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      balance NUMERIC(18,2) NOT NULL DEFAULT 0
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS rooms (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL,
      max_players INT NOT NULL,
      status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','full','in_play','closed')),
      created_by UUID REFERENCES users(id),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS matches (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      room_id UUID REFERENCES rooms(id) ON DELETE SET NULL,
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','live','finished','cancelled')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS match_players (
      match_id UUID REFERENCES matches(id) ON DELETE CASCADE,
      user_id  UUID REFERENCES users(id) ON DELETE CASCADE,
      result   TEXT,
      PRIMARY KEY (match_id, user_id)
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS transactions (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      user_id UUID REFERENCES users(id) ON DELETE CASCADE,
      kind TEXT NOT NULL CHECK (kind IN ('deposit','stake_debit','stake_release','payout','refund','withdraw')),
      amount NUMERIC(18,2) NOT NULL,        // positive values; debits recorded as positive with kind
      meta JSONB DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  console.log("✅ DB ready (tables ensured)");
}

// =============== Wallet helpers ===============
async function ensureUserAndWallet(c, userId) {
  await c.query(`INSERT INTO users(id) VALUES($1) ON CONFLICT(id) DO NOTHING`, [userId]);
  await c.query(`INSERT INTO wallets(user_id, balance) VALUES($1,0)
                 ON CONFLICT(user_id) DO NOTHING`, [userId]);
}
async function getBalance(c, userId) {
  const r = await c.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
  return r.rows[0] ? Number(r.rows[0].balance) : 0;
}
// Debit stake atomically; throws if funds insufficient
async function stakeDebit(c, userId, amount, meta) {
  const r = await c.query(
    `UPDATE wallets SET balance = balance - $2
     WHERE user_id=$1 AND balance >= $2
     RETURNING balance`,
    [userId, amount]
  );
  if (r.rowCount === 0) throw new Error("INSUFFICIENT_FUNDS");
  await c.query(
    `INSERT INTO transactions(user_id, kind, amount, meta)
     VALUES($1,'stake_debit',$2,$3)`,
    [userId, amount, meta || {}]
  );
  return Number(r.rows[0].balance);
}
// Simple credit (deposit/refund/payout)
async function credit(c, userId, amount, kind, meta) {
  const r = await c.query(
    `UPDATE wallets SET balance = balance + $2
     WHERE user_id=$1
     RETURNING balance`,
    [userId, amount]
  );
  await c.query(
    `INSERT INTO transactions(user_id, kind, amount, meta)
     VALUES($1,$2,$3,$4)`,
    [userId, kind, amount, meta || {}]
  );
  return Number(r.rows[0].balance);
}

// =============== In-memory room view (for quick list) ===============
const ROOM_CACHE = new Map(); // roomId -> {id, game, stake, maxPlayers, status, players:[]}

// =============== HTTP Server ===============
const server = http.createServer(async (req, res) => {
  try {
    // Health
    if (req.url === "/health") {
      return sendJSON(res, 200, { ok: true, ts: Date.now() });
    }

    // DB ping
    if (req.url === "/db/health" || req.url === "/db-ping") {
      const r = await q(`SELECT NOW() AS now`);
      return sendJSON(res, 200, { ok: true, dbTime: r.rows[0].now });
    }

    // Tables list (debug)
    if (req.url === "/db/tables") {
      const r = await q(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        ORDER BY table_name;
      `);
      return sendJSON(res, 200, { ok: true, tables: r.rows.map(x => x.table_name) });
    }

    // ---- Create Room (creator pays/escrows immediately)
    // POST /rooms/create  { userId, game, stake, maxPlayers }
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

      const roomId = uuid();

      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        await ensureUserAndWallet(c, userId);

        // escrow creator's stake now
        const balAfter = await stakeDebit(c, userId, stake, { roomId, role: "creator" });

        // persist room
        await c.query(
          `INSERT INTO rooms(id, game, stake, max_players, status, created_by)
           VALUES($1,$2,$3,$4,'open',$5)`,
          [roomId, game, stake, maxPlayers, userId]
        );

        await c.query("COMMIT");

        // update memory cache
        ROOM_CACHE.set(roomId, {
          id: roomId, game, stake, maxPlayers,
          status: "open", players: [userId]
        });

        return sendJSON(res, 200, {
          ok: true,
          room: ROOM_CACHE.get(roomId),
          wallet: { balance: String(balAfter) }
        });
      } catch (e) {
        await c.query("ROLLBACK");
        c.release();
        if (String(e.message).includes("INSUFFICIENT_FUNDS"))
          return sendJSON(res, 402, { ok: false, error: "INSUFFICIENT_FUNDS" });
        console.error("create room error:", e);
        return sendJSON(res, 500, { ok: false, error: "ROOM_CREATE_FAILED" });
      } finally {
        c.release();
      }
    }

    // ---- Join Room (escrows joiner)
    // POST /rooms/join  { userId, roomId }
    if (req.method === "POST" && req.url === "/rooms/join") {
      const body = await readJSON(req);
      const userId = String(body.userId || "").trim();
      const roomId = String(body.roomId || "").trim();
      if (!userId || !roomId) return sendJSON(res, 400, { ok: false, error: "MISSING_PARAMS" });

      const room = ROOM_CACHE.get(roomId);
      if (!room) return sendJSON(res, 404, { ok: false, error: "ROOM_NOT_FOUND" });
      if (room.status !== "open") return sendJSON(res, 400, { ok: false, error: "ROOM_CLOSED" });
      if (room.players.includes(userId))
        return sendJSON(res, 200, { ok: true, room, note: "ALREADY_JOINED" });
      if (room.players.length >= room.maxPlayers)
        return sendJSON(res, 400, { ok: false, error: "ROOM_FULL" });

      const c = await pool.connect();
      try {
        await c.query("BEGIN");
        await ensureUserAndWallet(c, userId);

        // escrow joiner's stake
        const balAfter = await stakeDebit(c, userId, room.stake, { roomId, role: "joiner" });

        await c.query("COMMIT");

        // update memory cache
        room.players.push(userId);
        if (room.players.length >= room.maxPlayers) room.status = "full";
        ROOM_CACHE.set(roomId, room);

        return sendJSON(res, 200, {
          ok: true,
          room,
          wallet: { balance: String(balAfter) }
        });
      } catch (e) {
        await c.query("ROLLBACK");
        c.release();
        if (String(e.message).includes("INSUFFICIENT_FUNDS"))
          return sendJSON(res, 402, { ok: false, error: "INSUFFICIENT_FUNDS" });
        console.error("join room error:", e);
        return sendJSON(res, 500, { ok: false, error: "JOIN_FAILED" });
      } finally {
        c.release();
      }
    }

    // ---- List Rooms
    // GET /rooms
    if (req.method === "GET" && req.url === "/rooms") {
      return sendJSON(res, 200, { ok: true, rooms: Array.from(ROOM_CACHE.values()) });
    }

    // Default 404
    return sendJSON(res, 404, { ok: false, error: "not_found" });
  } catch (err) {
    console.error("HTTP error:", err);
    return sendJSON(res, 500, { ok: false, error: "server_error" });
  }
});

// =============== WebSocket (echo for now) ===============
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

// =============== Boot ===============
initDb()
  .then(() => {
    server.listen(PORT, () => console.log(`✅ Server listening on :${PORT}`));
  })
  .catch((err) => {
    console.error("❌ Startup error:", err);
    process.exit(1);
  });