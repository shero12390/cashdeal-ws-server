// server.js — HTTP + WebSocket + Postgres (Render-ready, with /rooms/ready)

const http = require("http");
const url = require("url");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

/* =========================
   Postgres: pool + helpers
   ========================= */
const pool = new Pool({
  connectionString: process.env.DB_URL,
  ssl: { rejectUnauthorized: false }, // Render PG requires SSL
});

async function q(sql, params = []) {
  return pool.query(sql, params);
}

function sendJson(res, code, obj) {
  res.writeHead(code, {
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(obj));
}

async function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      if (!data) return resolve({});
      try {
        resolve(JSON.parse(data));
      } catch (e) {
        reject(Object.assign(new Error("invalid_json"), { kind: "invalid_json" }));
      }
    });
    req.on("error", reject);
  });
}

/* =========================
   DB bootstrap (migrations)
   ========================= */
async function runMigrations() {
  // Extensions
  await q(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);
  await q(`CREATE EXTENSION IF NOT EXISTS "pgcrypto";`);

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

  // rooms (base)
  await q(`
    CREATE TABLE IF NOT EXISTS rooms (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      host_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL DEFAULT 0,
      max_players INT NOT NULL CHECK (max_players >= 2 AND max_players <= 10),
      current_players INT NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'open',  -- open | full | in_play | closed
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      -- advanced knobs (nullable initially, but we immediately set defaults below)
      mode TEXT,
      min_players INT,
      autostart BOOLEAN,
      countdown_seconds INT,
      require_mutual_ready BOOLEAN,
      config JSONB
    );
  `);

  // ensure advanced columns & defaults exist (idempotent)
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'h2h';`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS min_players INT NOT NULL DEFAULT 2;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS autostart BOOLEAN NOT NULL DEFAULT false;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS countdown_seconds INT NOT NULL DEFAULT 0;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS require_mutual_ready BOOLEAN NOT NULL DEFAULT false;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS config JSONB NOT NULL DEFAULT '{}'::jsonb;`);
  await q(`CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);`);

  // room_players (tracks join + ready)
  await q(`
    CREATE TABLE IF NOT EXISTS room_players (
      room_id UUID NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      ready BOOLEAN NOT NULL DEFAULT false,
      joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (room_id, user_id)
    );
  `);

  // matches
  await q(`
    CREATE TABLE IF NOT EXISTS matches (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      room_id UUID REFERENCES rooms(id) ON DELETE SET NULL,
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
      reason TEXT NOT NULL,      -- deposit | withdraw | room_create | room_join | payout | refund | etc.
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
    const r = await q(`SELECT id FROM users WHERE id = $1::uuid`, [userId]).catch(() => null);
    if (!r || r.rowCount === 0) {
      const rr = await q(`INSERT INTO users(id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id`, [userId]);
      if (rr.rowCount === 0) {
        const rNew = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`);
        userId = rNew.rows[0].id;
      }
    }
  } else {
    const rNew = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`);
    userId = rNew.rows[0].id;
  }

  await q(
    `INSERT INTO wallets (user_id, balance, currency)
     VALUES ($1, 0, 'USDT')
     ON CONFLICT (user_id) DO NOTHING`,
    [userId]
  );

  return userId;
}

/* =========================
   Small helpers
   ========================= */
function toNumber(n, fallback = 0) {
  const v = Number(n);
  return Number.isFinite(v) ? v : fallback;
}

async function startMatchIfEligible(client, roomId) {
  // Load room + players
  const rRoom = await client.query(`SELECT * FROM rooms WHERE id=$1 FOR UPDATE`, [roomId]);
  if (rRoom.rowCount === 0) return null;
  const room = rRoom.rows[0];

  const rPlayers = await client.query(
    `SELECT user_id, ready FROM room_players WHERE room_id=$1 ORDER BY joined_at ASC`,
    [roomId]
  );
  const players = rPlayers.rows;

  // Conditions to start:
  // - if require_mutual_ready: everyone must be ready AND count >= min_players
  // - else if autostart: start when full (current_players >= max_players)
  let canStart = false;
  if (room.require_mutual_ready) {
    const allReady = players.length > 0 && players.every((p) => p.ready);
    if (allReady && room.current_players >= room.min_players) canStart = true;
  } else if (room.autostart) {
    if (room.current_players >= room.max_players) canStart = true;
  }

  if (!canStart) return null;

  // If a match already exists for this room in ready/running, reuse it; otherwise create.
  let matchId = null;
  const rExist = await client.query(
    `SELECT id, status FROM matches WHERE room_id=$1 AND status IN ('ready','running') ORDER BY created_at DESC NULLS LAST LIMIT 1`,
    [roomId]
  ).catch(() => ({ rowCount: 0 }));

  if (rExist.rowCount > 0) {
    matchId = rExist.rows[0].id;
    // If still 'ready', we'll transition to running.
  } else {
    const stakeNum = toNumber(room.stake, 0);
    const rM = await client.query(
      `INSERT INTO matches (room_id, game, stake, status, started_at)
       VALUES ($1,$2,$3,'ready', NOW())
       RETURNING id`,
      [room.id, room.game, stakeNum]
    );
    matchId = rM.rows[0].id;
  }

  // Insert match players (idempotent)
  for (const p of players) {
    await client.query(
      `INSERT INTO match_players (match_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [matchId, p.user_id]
    );
  }

  // Transition match + room
  await client.query(`UPDATE matches SET status='running', started_at = COALESCE(started_at, NOW()) WHERE id=$1`, [matchId]);
  await client.query(`UPDATE rooms SET status='in_play' WHERE id=$1`, [room.id]);

  return matchId;
}

/* =========================
   HTTP server
   ========================= */
const server = http.createServer(async (req, res) => {
  try {
    const { pathname, query } = url.parse(req.url, true);

    // CORS
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET,POST,OPTIONS",
        "access-control-allow-headers": "content-type",
      });
      return res.end();
    }
    res.setHeader("access-control-allow-origin", "*");

    // health
    if (pathname === "/health") {
      return sendJson(res, 200, { ok: true, ts: Date.now() });
    }

    // db health
    if (pathname === "/db/health") {
      const r = await q(`SELECT NOW() AS now`);
      return sendJson(res, 200, { ok: true, dbTime: r.rows[0].now });
    }

    // list tables
    if (pathname === "/tables") {
      const r = await q(`
        SELECT tablename
        FROM pg_tables
        WHERE schemaname='public'
        ORDER BY tablename ASC
      `);
      return sendJson(res, 200, { ok: true, tables: r.rows.map((x) => x.tablename) });
    }

    // ---------- WALLET: DEPOSIT (for testing) ----------
    if (req.method === "POST" && pathname === "/wallet/deposit") {
      let body;
      try { body = await readJsonBody(req); } catch (e) { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }
      const userIdIn = String(body.userId || "").trim();
      const amount = toNumber(body.amount, NaN);
      if (!Number.isFinite(amount) || amount <= 0) {
        return sendJson(res, 400, { ok: false, error: "invalid_amount" });
      }
      const userId = await ensureUserWithWallet(userIdIn);
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(
          `UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE user_id=$2`,
          [amount, userId]
        );
        await client.query(
          `INSERT INTO transactions (user_id, amount, side, reason, meta)
           VALUES ($1, $2, 'credit', 'deposit', '{}'::jsonb)`,
          [userId, amount]
        );
        await client.query("COMMIT");
        const rBal = await q(`SELECT balance, currency FROM wallets WHERE user_id=$1`, [userId]);
        return sendJson(res, 200, { ok: true, userId, wallet: rBal.rows[0] });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("deposit error", e);
        return sendJson(res, 500, { ok: false, error: e.message || "server_error" });
      } finally {
        client.release();
      }
    }

    // ---------- ROOMS: CREATE ----------
    if (req.method === "POST" && pathname === "/rooms/create") {
      let body;
      try { body = await readJsonBody(req); } catch (e) { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }

      const userIdIn   = String(body.userId || "").trim() || null;
      const game       = String(body.game || "").trim() || "ludo";
      const stake      = toNumber(body.stake, 0);
      const maxPlayers = Number.isInteger(body.maxPlayers) ? body.maxPlayers : toNumber(body.maxPlayers, 2);

      // advanced knobs
      const mode                 = String(body.mode || "h2h");
      const minPlayers           = Number.isInteger(body.minPlayers) ? body.minPlayers : toNumber(body.minPlayers, 2);
      const autostart            = Boolean(body.autostart ?? false);
      const countdownSeconds     = Number.isInteger(body.countdownSeconds) ? body.countdownSeconds : toNumber(body.countdownSeconds, 0);
      const requireMutualReady   = Boolean(body.require_mutual_ready ?? false);
      const config               = body.config && typeof body.config === "object" ? body.config : {};

      if (!Number.isFinite(stake) || stake < 0) {
        return sendJson(res, 400, { ok: false, error: "invalid_stake" });
      }
      if (!Number.isInteger(maxPlayers) || maxPlayers < 2 || maxPlayers > 10) {
        return sendJson(res, 400, { ok: false, error: "invalid_maxPlayers" });
      }
      if (!Number.isInteger(minPlayers) || minPlayers < 2 || minPlayers > maxPlayers) {
        return sendJson(res, 400, { ok: false, error: "invalid_minPlayers" });
      }

      const userId = await ensureUserWithWallet(userIdIn);

      // Optional: require balance >= stake to create room
      const rWal = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
      const bal = toNumber(rWal.rows[0]?.balance, 0);
      if (bal < stake) {
        return sendJson(res, 400, { ok: false, error: "insufficient_funds", balance: bal, needed: stake });
      }

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        if (stake > 0) {
          await client.query(
            `UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE user_id=$2`,
            [stake, userId]
          );
          await client.query(
            `INSERT INTO transactions (user_id, amount, side, reason, meta)
             VALUES ($1, $2, 'debit', 'room_create', jsonb_build_object('game',$3))`,
            [userId, stake, game]
          );
        }

        const rRoom = await client.query(
          `INSERT INTO rooms (host_user_id, game, stake, max_players, current_players, status,
                              mode, min_players, autostart, countdown_seconds, require_mutual_ready, config)
           VALUES ($1, $2, $3, $4, 1, 'open', $5, $6, $7, $8, $9, $10)
           RETURNING *`,
          [userId, game, stake, maxPlayers, mode, minPlayers, autostart, countdownSeconds, requireMutualReady, config]
        );

        // add host as first room_player
        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready) VALUES ($1,$2,false)
           ON CONFLICT DO NOTHING`,
          [rRoom.rows[0].id, userId]
        );

        await client.query("COMMIT");
        return sendJson(res, 200, { ok: true, room: rRoom.rows[0] });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/create error", e);
        return sendJson(res, 500, { ok: false, error: e.message || "server_error" });
      } finally {
        client.release();
      }
    }

    // ---------- ROOMS: JOIN ----------
    if (req.method === "POST" && pathname === "/rooms/join") {
      let body;
      try { body = await readJsonBody(req); } catch (e) { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }

      const roomId = String(body.roomId || "").trim();
      const userIdIn = String(body.userId || "").trim() || null;

      if (!roomId) return sendJson(res, 400, { ok: false, error: "missing_roomId" });

      const rRoom = await q(
        `SELECT * FROM rooms WHERE id = $1`,
        [roomId]
      );
      if (rRoom.rowCount === 0) return sendJson(res, 404, { ok: false, error: "room_not_found" });

      const room = rRoom.rows[0];
      if (room.status !== "open") return sendJson(res, 400, { ok: false, error: "room_not_open" });
      if (room.current_players >= room.max_players) return sendJson(res, 400, { ok: false, error: "room_full" });

      const userId = await ensureUserWithWallet(userIdIn);

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        // Hold stake (if any)
        const stakeNum = toNumber(room.stake, 0);
        if (stakeNum > 0) {
          const rWal = await client.query(
            `SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`,
            [userId]
          );
          const bal = toNumber(rWal.rows[0]?.balance, 0);
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
             VALUES ($1, $2, 'debit', 'room_join',
                     jsonb_build_object('roomId',$3,'game',$4))`,
            [userId, stakeNum, room.id, room.game]
          );
        }

        // add to room_players (ready=false by default)
        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready)
           VALUES ($1,$2,false) ON CONFLICT DO NOTHING`,
          [room.id, userId]
        );

        // Increment player count atomically
        const rUpd = await client.query(
          `UPDATE rooms
             SET current_players = current_players + 1
           WHERE id = $1
             AND current_players < max_players
             AND status = 'open'
           RETURNING *`,
          [room.id]
        );
        if (rUpd.rowCount === 0) {
          await client.query("ROLLBACK");
          return sendJson(res, 400, { ok: false, error: "became_full_try_again" });
        }
        const updatedRoom = rUpd.rows[0];

        // If room filled and no mutual_ready, autostart path can pre-create match
        if (updatedRoom.current_players >= updatedRoom.max_players && !updatedRoom.require_mutual_ready && updatedRoom.autostart) {
          await client.query(
            `INSERT INTO matches (room_id, game, stake, status, started_at)
             VALUES ($1, $2, $3, 'ready', NOW())
             ON CONFLICT DO NOTHING`,
            [updatedRoom.id, updatedRoom.game, stakeNum]
          );
          // try to start immediately
          await startMatchIfEligible(client, updatedRoom.id);
          await client.query(`UPDATE rooms SET status = CASE WHEN status='in_play' THEN status ELSE 'full' END WHERE id=$1`, [updatedRoom.id]);
        }

        await client.query("COMMIT");

        const rBal = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        return sendJson(res, 200, {
          ok: true,
          room: updatedRoom,
          balance: toNumber(rBal.rows[0]?.balance, 0),
        });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/join error", e);
        return sendJson(res, 500, { ok: false, error: e.message || "server_error" });
      } finally {
        client.release();
      }
    }

    // ---------- ROOMS: READY (player toggles ready) ----------
    if (req.method === "POST" && pathname === "/rooms/ready") {
      let body;
      try { body = await readJsonBody(req); } catch (e) { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }
      const roomId = String(body.roomId || "").trim();
      const userIdIn = String(body.userId || "").trim() || null;
      const ready = Boolean(body.ready ?? true);
      if (!roomId) return sendJson(res, 400, { ok: false, error: "missing_roomId" });

      const userId = await ensureUserWithWallet(userIdIn);

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        const rRoom = await client.query(`SELECT * FROM rooms WHERE id=$1 FOR UPDATE`, [roomId]);
        if (rRoom.rowCount === 0) {
          await client.query("ROLLBACK");
          return sendJson(res, 404, { ok: false, error: "room_not_found" });
        }
        const room = rRoom.rows[0];
        if (!room.require_mutual_ready && !room.autostart) {
          // nothing to do; but accept
        }

        // ensure user is in room
        const rMember = await client.query(
          `SELECT 1 FROM room_players WHERE room_id=$1 AND user_id=$2`,
          [roomId, userId]
        );
        if (rMember.rowCount === 0) {
          await client.query("ROLLBACK");
          return sendJson(res, 400, { ok: false, error: "not_in_room" });
        }

        await client.query(
          `UPDATE room_players SET ready=$3 WHERE room_id=$1 AND user_id=$2`,
          [roomId, userId, ready]
        );

        // attempt to start if eligible
        const matchId = await startMatchIfEligible(client, roomId);

        await client.query("COMMIT");

        // return room status + players readiness
        const rRoom2 = await q(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
        const rPlayers2 = await q(
          `SELECT user_id, ready, joined_at FROM room_players WHERE room_id=$1 ORDER BY joined_at ASC`,
          [roomId]
        );
        return sendJson(res, 200, {
          ok: true,
          room: rRoom2.rows[0],
          players: rPlayers2.rows,
          startedMatchId: matchId || null,
        });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/ready error", e);
        return sendJson(res, 500, { ok: false, error: e.message || "server_error" });
      } finally {
        client.release();
      }
    }

    // ---------- ROOMS: LEAVE (refund if room is open) ----------
    if (req.method === "POST" && pathname === "/rooms/leave") {
      let body;
      try { body = await readJsonBody(req); } catch (e) { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }

      const roomId = String(body.roomId || "").trim();
      const userIdIn = String(body.userId || "").trim() || null;
      if (!roomId) return sendJson(res, 400, { ok: false, error: "missing_roomId" });

      const rRoom = await q(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
      if (rRoom.rowCount === 0) return sendJson(res, 404, { ok: false, error: "room_not_found" });
      const room = rRoom.rows[0];
      if (room.status !== "open") return sendJson(res, 400, { ok: false, error: "room_not_open" });

      const userId = await ensureUserWithWallet(userIdIn);

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        // remove from room_players (idempotent)
        await client.query(`DELETE FROM room_players WHERE room_id=$1 AND user_id=$2`, [room.id, userId]);

        // Simple refund of stake if stake > 0
        const stakeNum = toNumber(room.stake, 0);
        if (stakeNum > 0) {
          await client.query(
            `UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE user_id=$2`,
            [stakeNum, userId]
          );
          await client.query(
            `INSERT INTO transactions (user_id, amount, side, reason, meta)
             VALUES ($1, $2, 'credit', 'refund', jsonb_build_object('roomId',$3))`,
            [userId, stakeNum, room.id]
          );
        }

        // Decrement player count but keep at minimum 1 (host)
        await client.query(
          `UPDATE rooms
             SET current_players = GREATEST(current_players - 1, 1)
           WHERE id = $1`,
          [room.id]
        );

        await client.query("COMMIT");
        const rRoom2 = await q(`SELECT * FROM rooms WHERE id=$1`, [room.id]);
        return sendJson(res, 200, { ok: true, room: rRoom2.rows[0] });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/leave error", e);
        return sendJson(res, 500, { ok: false, error: e.message || "server_error" });
      } finally {
        client.release();
      }
    }

    // ---------- ROOMS: STATUS ----------
    if (req.method === "GET" && pathname === "/rooms/status") {
      const roomId = String(query.roomId || "").trim();
      if (!roomId) return sendJson(res, 400, { ok: false, error: "missing_roomId" });
      const rRoom = await q(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
      if (rRoom.rowCount === 0) return sendJson(res, 404, { ok: false, error: "room_not_found" });
      const rPlayers = await q(
        `SELECT user_id, ready, joined_at FROM room_players WHERE room_id=$1 ORDER BY joined_at ASC`,
        [roomId]
      );
      return sendJson(res, 200, { ok: true, room: rRoom.rows[0], players: rPlayers.rows });
    }

    // ---------- ROOMS: LIST ----------
    if (req.method === "GET" && pathname === "/rooms/list") {
      const status = String(query.status || "open");
      const r = await q(
        `SELECT id, host_user_id, game, stake, max_players, current_players, status, created_at,
                mode, min_players, autostart, countdown_seconds, require_mutual_ready, config
           FROM rooms
          WHERE status = $1
          ORDER BY created_at DESC
          LIMIT 100`,
        [status]
      );
      return sendJson(res, 200, { ok: true, rooms: r.rows });
    }

    // default 404
    return sendJson(res, 404, { ok: false, error: "not_found" });
  } catch (err) {
    console.error("HTTP error:", err);
    const msg = err?.kind === "invalid_json" ? "invalid_json" : "server_error";
    return sendJson(res, 500, { ok: false, error: msg });
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