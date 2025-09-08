// server.js — HTTP + WebSocket + Postgres (Render-ready)

const http = require("http");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

/* =========================
   Postgres: pool + helpers
   ========================= */
const pool = new Pool({
  connectionString: process.env.DB_URL, // set on Render
  ssl: { rejectUnauthorized: false },    // required by Render PG
});

async function q(sql, params = []) {
  return pool.query(sql, params);
}

function sendJson(res, code, obj) {
  res.writeHead(code, { "content-type": "application/json" });
  res.end(JSON.stringify(obj));
}

async function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

/* =========================
   DB bootstrap (migrations)
   ========================= */
async function runMigrations() {
  // UUID extension
  await q(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

  // users
  await q(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // wallets
  await q(`
    CREATE TABLE IF NOT EXISTS wallets (
      user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      balance NUMERIC(18,2) NOT NULL DEFAULT 0,
      currency TEXT NOT NULL DEFAULT 'USDT',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // rooms
  await q(`
    CREATE TABLE IF NOT EXISTS rooms (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      host_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL DEFAULT 0,
      max_players INT NOT NULL CHECK (max_players >= 2),
      current_players INT NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'open',  -- open | full | closed
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await q(`CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);`);

  // matches (created when a room fills)
  await q(`
    CREATE TABLE IF NOT EXISTS matches (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      room_id UUID NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL,
      status TEXT NOT NULL DEFAULT 'ready', -- ready | running | finished | cancelled
      started_at TIMESTAMPTZ,
      ended_at TIMESTAMPTZ
    );
  `);

  // match_players
  await q(`
    CREATE TABLE IF NOT EXISTS match_players (
      match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
      user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (match_id, user_id)
    );
  `);

  // transactions (wallet ledger)
  await q(`
    CREATE TABLE IF NOT EXISTS transactions (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      amount NUMERIC(18,2) NOT NULL,
      side TEXT NOT NULL,        -- debit | credit
      reason TEXT NOT NULL,      -- deposit | withdraw | room_join | payout | etc.
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  console.log("✅ Migrations complete");
}

/* =========================
   Helper: ensure user & wallet
   ========================= */
async function ensureUserWithWallet(userIdMaybe) {
  let userId = String(userIdMaybe || "").trim();
  if (userId) {
    // If caller supplied a UUID, be sure the user exists; if not, create.
    const r = await q(`SELECT id FROM users WHERE id = $1::uuid`, [userId]).catch(() => null);
    if (!r || r.rowCount === 0) {
      const rNew = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`);
      userId = rNew.rows[0].id;
    }
  } else {
    const rNew = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`);
    userId = rNew.rows[0].id;
  }

  // Ensure wallet
  await q(
    `INSERT INTO wallets (user_id, balance, currency)
     VALUES ($1, 0, 'USDT')
     ON CONFLICT (user_id) DO NOTHING`,
    [userId]
  );

  return userId;
}

/* =========================
   HTTP server
   ========================= */
const server = http.createServer(async (req, res) => {
  try {
    // health
    if (req.url === "/health") {
      return sendJson(res, 200, { ok: true, ts: Date.now() });
    }

    // db health
    if (req.url === "/db/health") {
      const r = await q(`SELECT NOW() AS now`);
      return sendJson(res, 200, { ok: true, dbTime: r.rows[0].now });
    }

    // list tables
    if (req.url === "/tables") {
      const r = await q(`
        SELECT tablename
        FROM pg_tables
        WHERE schemaname='public'
        ORDER BY tablename ASC
      `);
      return sendJson(res, 200, { ok: true, tables: r.rows.map((x) => x.tablename) });
    }

    // ---------- ROOMS: CREATE ----------
    if (req.method === "POST" && req.url === "/rooms/create") {
      const body = await readJsonBody(req);
      const userIdIn   = String(body.userId || "").trim() || null;
      const game       = String(body.game || "").trim() || "ludo";
      const stake      = Number(body.stake || 0);
      const maxPlayers = Number(body.maxPlayers || 2);

      if (!Number.isFinite(stake) || stake < 0) {
        return sendJson(res, 400, { ok: false, error: "invalid_stake" });
      }
      if (!Number.isInteger(maxPlayers) || maxPlayers < 2 || maxPlayers > 8) {
        return sendJson(res, 400, { ok: false, error: "invalid_maxPlayers" });
      }

      const userId = await ensureUserWithWallet(userIdIn);

      // Make sure host has enough balance to stake (optional rule).
      const rWal = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
      const bal = Number(rWal.rows[0]?.balance || 0);
      if (bal < stake) {
        return sendJson(res, 400, { ok: false, error: "insufficient_funds", balance: bal, needed: stake });
      }

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        // Debit host stake up-front (optional game rule)
        await client.query(
          `UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE user_id=$2`,
          [stake, userId]
        );
        await client.query(
          `INSERT INTO transactions (user_id, amount, side, reason, meta)
           VALUES ($1, $2, 'debit', 'room_create', jsonb_build_object('game',$3))`,
          [userId, stake, game]
        );

        // Create room
        const rRoom = await client.query(
          `INSERT INTO rooms (host_user_id, game, stake, max_players, current_players, status)
           VALUES ($1, $2, $3, $4, 1, 'open')
           RETURNING id, game, stake, max_players, current_players, status, created_at`,
          [userId, game, stake, maxPlayers]
        );

        await client.query("COMMIT");
        return sendJson(res, 200, { ok: true, room: rRoom.rows[0] });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("create error", e);
        return sendJson(res, 500, { ok: false, error: String(e.message || e) });
      } finally {
        client.release();
      }
    }

    // ---------- ROOMS: JOIN ----------
    if (req.method === "POST" && req.url === "/rooms/join") {
      const body = await readJsonBody(req);
      const roomId = String(body.roomId || "").trim();
      const userIdIn = String(body.userId || "").trim() || null;

      if (!roomId) {
        return sendJson(res, 400, { ok: false, error: "missing_roomId" });
      }

      // Load room
      const rRoom = await q(
        `SELECT id, host_user_id, game, stake, max_players, current_players, status
           FROM rooms WHERE id = $1`,
        [roomId]
      );
      if (rRoom.rowCount === 0) {
        return sendJson(res, 404, { ok: false, error: "room_not_found" });
      }
      const room = rRoom.rows[0];
      if (room.status !== "open") {
        return sendJson(res, 400, { ok: false, error: "room_not_open" });
      }
      if (room.current_players >= room.max_players) {
        return sendJson(res, 400, { ok: false, error: "room_full" });
      }

      const userId = await ensureUserWithWallet(userIdIn);

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        // Check wallet balance and hold stake
        const rWal = await client.query(
          `SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`,
          [userId]
        );
        const bal = Number(rWal.rows[0]?.balance || 0);
        const stakeNum = Number(room.stake || 0);
        if (bal < stakeNum) {
          await client.query("ROLLBACK");
          return sendJson(res, 400, { ok: false, error: "insufficient_funds", balance: bal, needed: stakeNum });
        }

        await client.query(
          `UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE user_id=$2`,
          [stakeNum, userId]
        );
        await client.query(
          `INSERT INTO transactions (user_id, amount, side, reason, meta)
           VALUES ($1, $2, 'debit', 'room_join', jsonb_build_object('roomId',$3,'game',$4))`,
          [userId, stakeNum, room.id, room.game]
        );

        // Increment player count
        const rUpd = await client.query(
          `UPDATE rooms
             SET current_players = current_players + 1
           WHERE id = $1
             AND current_players < max_players
           RETURNING id, game, stake, max_players, current_players, status`,
          [room.id]
        );
        if (rUpd.rowCount === 0) {
          await client.query("ROLLBACK");
          return sendJson(res, 400, { ok: false, error: "became_full_try_again" });
        }
        const updatedRoom = rUpd.rows[0];

        // If filled, mark full + create a match entry
        if (updatedRoom.current_players >= updatedRoom.max_players) {
          await client.query(`UPDATE rooms SET status='full' WHERE id=$1`, [room.id]);
          const rMatch = await client.query(
            `INSERT INTO matches (room_id, game, stake, status, started_at)
             VALUES ($1, $2, $3, 'ready', NOW())
             RETURNING id`,
            [room.id, room.game, stakeNum]
          );
          // Note: adding match_players would require knowing all participants; omitted for brevity.
        }

        await client.query("COMMIT");

        // Return updated room and user balance
        const rBal = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        return sendJson(res, 200, {
          ok: true,
          room: updatedRoom,
          balance: Number(rBal.rows[0]?.balance || 0),
        });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("join error", e);
        return sendJson(res, 500, { ok: false, error: String(e.message || e) });
      } finally {
        client.release();
      }
    }

    // default 404
    return sendJson(res, 404, { ok: false, error: "not_found" });
  } catch (err) {
    console.error("HTTP error:", err);
    return sendJson(res, 500, { ok: false, error: "server_error" });
  }
});

/* =========================
   WebSocket (simple echo)
   ========================= */
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

/* =========================
   Boot
   ========================= */
(async () => {
  try {
    await pool.connect();
    console.log("✅ Connected to Postgres");
    await runMigrations();
    server.listen(PORT, () => console.log(`✅ Server on :${PORT}`));
  } catch (e) {
    console.error("❌ Startup error", e);
    process.exit(1);
  }
})();