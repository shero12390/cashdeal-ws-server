// server.js — CashDeal HTTP + WebSocket + Postgres (Render-ready)

// ------------------ Imports ------------------
const http = require("http");
const Url = require("url");                 // << don't shadow 'url' — use 'Url'
const crypto = require("crypto");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");
const jwt = require("jsonwebtoken");
// Swagger / Express (for API docs)
const express = require("express");
const swaggerUi = require("swagger-ui-express");
const YAML = require("yamljs");
const path = require("path");

const app = express();

// load openapi.yaml from repo root
const swaggerDoc = YAML.load(path.join(__dirname, "openapi.yaml"));

// serve docs at /docs
app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerDoc));

// ------------------ Config / Env ------------------
const PORT = Number(process.env.PORT) || 3000;                 // << fixed port handling
const DB_URL = process.env.DB_URL;
const AUTH_JWT_SECRET = process.env.AUTH_JWT_SECRET || "dev_secret_change_me";
const TOKEN_TTL_SECONDS = Number(process.env.TOKEN_TTL_SECONDS || 3600);
const SYNC_SECRET = process.env.SYNC_SECRET || "superStrongSecret123"; // legacy header fallback
const SYNC_HMAC_KEY = process.env.SYNC_HMAC_KEY || "hmac_key_change_me"; // preferred HMAC

// ------------------ DB Pool ------------------
const pool = new Pool({
  connectionString: DB_URL,
  ssl: { rejectUnauthorized: false },
});
const q = (sql, params = []) => pool.query(sql, params);

function sendJson(res, code, obj) {
  res.writeHead(code, {
    "content-type": "application/json",
    "cache-control": "no-store",
    "access-control-allow-origin": "*",
  });
  res.end(JSON.stringify(obj));
}

async function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", c => (data += c));
    req.on("end", () => {
      if (!data) return resolve({});
      try { resolve(JSON.parse(data)); }
      catch { reject(new Error("invalid_json")); }
    });
    req.on("error", reject);
  });
}
const n = (v, f = 0) => { const x = Number(v); return Number.isFinite(x) ? x : f; };

// ------------------ Migrations ------------------
async function runMigrations() {
  await q(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);
  await q(`CREATE EXTENSION IF NOT EXISTS "pgcrypto";`);

  await q(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS wallets (
      user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      balance NUMERIC(18,2) NOT NULL DEFAULT 0,
      currency TEXT NOT NULL DEFAULT 'USDT',
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS rooms (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      host_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL DEFAULT 0,
      max_players INT NOT NULL CHECK (max_players BETWEEN 2 AND 10),
      current_players INT NOT NULL DEFAULT 1,
      status TEXT NOT NULL DEFAULT 'open',  -- open | locked | in_play | full | closed
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      mode TEXT NOT NULL DEFAULT 'h2h',
      min_players INT NOT NULL DEFAULT 2,
      autostart BOOLEAN NOT NULL DEFAULT false,
      countdown_seconds INT NOT NULL DEFAULT 0,
      require_mutual_ready BOOLEAN NOT NULL DEFAULT false,
      config JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);
  await q(`CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);`);

  await q(`
    CREATE TABLE IF NOT EXISTS room_players (
      room_id UUID NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      ready BOOLEAN NOT NULL DEFAULT false,
      joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (room_id, user_id)
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS matches (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      room_id UUID REFERENCES rooms(id) ON DELETE SET NULL,
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'ready', -- ready | running | live | finished | cancelled
      started_at TIMESTAMPTZ,
      ended_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS match_players (
      match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
      user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      seat SMALLINT,
      result TEXT NOT NULL DEFAULT 'pending', -- pending | won | lost | draw | cancelled
      stake_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
      left_at TIMESTAMPTZ,
      grace_until TIMESTAMPTZ,
      PRIMARY KEY (match_id, user_id)
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS transactions (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      amount NUMERIC(18,2) NOT NULL,
      reason TEXT NOT NULL, -- deposit | withdraw | stake_hold | stake_release | payout | refund | sync_set
      ref_match_id UUID REFERENCES matches(id) ON DELETE SET NULL,
      balance_after NUMERIC(18,2) NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await q(`CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id);`);

  console.log("✅ migrations done");
}

// ------------------ Helpers ------------------
async function ensureUserWithWallet(userIdMaybe) {
  let userId = String(userIdMaybe || "").trim();

  if (userId) {
    const ok = await q(`SELECT 1 FROM users WHERE id=$1::uuid`, [userId]).catch(() => null);
    if (!ok || ok.rowCount === 0) {
      const r = await q(`INSERT INTO users(id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id`, [userId]);
      if (r.rowCount === 0) {
        const r2 = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`);
        userId = r2.rows[0].id;
      }
    }
  } else {
    const r = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`);
    userId = r.rows[0].id;
  }

  await q(
    `INSERT INTO wallets (user_id, balance, currency)
     VALUES ($1, 0, 'USDT')
     ON CONFLICT (user_id) DO NOTHING`,
    [userId]
  );

  return userId;
}

// ------------------ Auth (short-lived JWT) ------------------
function mintToken(userId) {
  const payload = { sub: userId };
  return jwt.sign(payload, AUTH_JWT_SECRET, { expiresIn: TOKEN_TTL_SECONDS });
}
function verifyAuth(req) {
  const hdr = req.headers["authorization"] || "";
  const m = hdr.match(/^Bearer\s+(.+)$/i);
  if (!m) return null;
  try {
    const decoded = jwt.verify(m[1], AUTH_JWT_SECRET);
    return decoded?.sub || null;
  } catch {
    return null;
  }
}

// ------------------ Simple rate limit (per IP) ------------------
const rlMap = new Map(); // ip -> {count, resetTs}
function rateLimit(ip, limit = 60, windowMs = 60_000) {
  const now = Date.now();
  const rec = rlMap.get(ip) || { count: 0, resetTs: now + windowMs };
  if (now > rec.resetTs) { rec.count = 0; rec.resetTs = now + windowMs; }
  rec.count += 1;
  rlMap.set(ip, rec);
  return rec.count <= limit;
}

// ------------------ WebSocket + HTTP (Express-attached) ------------------
// Attach your Express app to the HTTP server so /docs and future REST routes work.
const server = http.createServer(app);

// Create the WS server on the same HTTP server
const wss = new WebSocketServer({ server, perMessageDeflate: false });

// userId -> Set<ws>
const subsByUser = new Map();

function wsSend(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch (_) {}
}

async function fetchBalance(uid) {
  const r = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [uid]);
  return Number(r.rows[0]?.balance ?? 0);
}

function wsBroadcastBalance(userId, balance) {
  const subs = subsByUser.get(userId);
  if (!subs) return;
  for (const ws of subs) {
    if (ws.readyState === ws.OPEN) {
      wsSend(ws, { type: "balance.update", userId, balance });
    }
  }
}

wss.on("connection", (ws) => {
  // Optional heartbeat (keeps connections from idling out)
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", async (buf) => {
    let msg;
    try { msg = JSON.parse(String(buf)); } catch { return; }

    // Ping/Pong
    if (msg?.type === "ping") {
      return wsSend(ws, { type: "pong", ts: msg.ts || Date.now() });
    }

    // Subscribe to balance updates for a user
    if (msg?.type === "subscribe.balance" && msg.userId) {
      const uid = String(msg.userId);
      if (!subsByUser.has(uid)) subsByUser.set(uid, new Set());
      subsByUser.get(uid).add(ws);

      ws.on("close", () => {
        const set = subsByUser.get(uid);
        if (set) { set.delete(ws); if (set.size === 0) subsByUser.delete(uid); }
      });

      wsSend(ws, { type: "balance.subscribed", userId: uid });

      // Send current balance immediately
      try {
        const bal = await fetchBalance(uid);
        wsSend(ws, { type: "balance.update", userId: uid, balance: bal });
      } catch (_) {}
    }
  });
});

// Heartbeat sweep (optional; cleans dead sockets)
const HEARTBEAT_MS = 30000;
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
}, HEARTBEAT_MS);

// Start the combined HTTP+WS server (ensure this is called only once in the file)
server.listen(PORT, () => {
  console.log(`CashDeal server listening on :${PORT}`);
});

// Export broadcaster so your HTTP endpoints can push updates after DB writes
module.exports = { wsBroadcastBalance };
// ------------------ HTTP Router ------------------
server.on("request", async (req, res) => {
  try {
    // CORS preflight
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET,POST,OPTIONS",
        "access-control-allow-headers": "content-type,authorization,x-sync-secret,x-sig,x-timestamp",
      });
      return res.end();
    }

    const { pathname, query } = Url.parse(req.url || "/", true);   // << fixed

    // Basic rate limit
    const ip = req.headers["x-forwarded-for"]?.toString().split(",")[0]?.trim() || req.socket.remoteAddress || "ip";
    if (!rateLimit(ip)) return sendJson(res, 429, { ok: false, error: "rate_limited" });

    // Health
    if (pathname === "/health") return sendJson(res, 200, { ok: true });
    if (pathname === "/db/health") {
      const r = await q(`SELECT NOW() now`);
      return sendJson(res, 200, { ok: true, dbTime: r.rows[0].now });
    }

    // ---------------- Wallet: mint a dev token (optional)
    if (req.method === "POST" && pathname === "/auth/devmint") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const uid = await ensureUserWithWallet(body.userId);
      const token = mintToken(uid);
      return sendJson(res, 200, { ok:true, token, userId: uid, ttl: TOKEN_TTL_SECONDS });
    }

    // ---------------- Wallet: test deposit (manual)
    if (req.method === "POST" && pathname === "/wallet/deposit") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }

      const userIdIn = String(body.userId || "").trim();
      const amount = Number(body.amount);
      if (!Number.isFinite(amount) || amount <= 0) {
        return sendJson(res, 400, { ok: false, error: "invalid_amount" });
      }
      const userId = await ensureUserWithWallet(userIdIn);

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(
          `UPDATE wallets SET balance = balance + $1, updated_at = NOW()
           WHERE user_id = $2`,
          [amount, userId]
        );
        const rBal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        const newBal = Number(rBal.rows[0]?.balance ?? 0);
        await client.query(
          `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
           VALUES ($1, $2, 'deposit', NULL, $3)`,
          [userId, amount, newBal]
        );
        await client.query("COMMIT");
        // push to WS subscribers
        wsBroadcastBalance(userId, newBal);
        return sendJson(res, 200, { ok: true, userId, wallet: { balance: newBal, currency: "USDT" } });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("deposit error", e);
        return sendJson(res, 500, { ok: false, error: "server_error" });
      } finally { client.release(); }
    }

    // ---------------- Rooms: CREATE
    if (req.method === "POST" && pathname === "/rooms/create") {
      const userIdAuth = verifyAuth(req);
      if (!userIdAuth) return sendJson(res, 401, { ok:false, error:"unauthorized" });

      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const userId = await ensureUserWithWallet(body.userId || userIdAuth);
      const game = String(body.game || "ludo");
      const stake = n(body.stake, 0);
      const maxPlayers = Number.isInteger(body.maxPlayers) ? body.maxPlayers : n(body.maxPlayers, 2);
      const mode = String(body.mode || "h2h");
      const minPlayers = Number.isInteger(body.minPlayers) ? body.minPlayers : 2;
      const autostart = !!body.autostart;
      const countdownSeconds = Number.isInteger(body.countdownSeconds) ? body.countdownSeconds : 0;
      const requireMutual = !!body.require_mutual_ready;
      const config = body.config && typeof body.config === "object" ? body.config : {};

      if (!Number.isInteger(maxPlayers) || maxPlayers < 2 || maxPlayers > 10) {
        return sendJson(res, 400, { ok:false, error:"invalid_maxPlayers" });
      }
      if (stake < 0) return sendJson(res, 400, { ok:false, error:"invalid_stake" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        if (stake > 0) {
          const rWal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`, [userId]);
          const bal = n(rWal.rows[0]?.balance, 0);
          if (bal < stake) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"insufficient_funds", balance: bal }); }
          await client.query(`UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE user_id=$2`, [stake, userId]);
          const rBal2 = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
          await client.query(
            `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
             VALUES ($1, $2, 'stake_hold', NULL, $3)`,
            [userId, stake, rBal2.rows[0].balance]
          );
        }

        const rRoom = await client.query(
          `INSERT INTO rooms (host_user_id, game, stake, max_players, current_players, status,
                              mode, min_players, autostart, countdown_seconds, require_mutual_ready, config)
           VALUES ($1,$2,$3,$4,1,'open',$5,$6,$7,$8,$9,$10)
           RETURNING *`,
          [userId, game, stake, maxPlayers, mode, minPlayers, autostart, countdownSeconds, requireMutual, JSON.stringify(config)]
        );

        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready)
           VALUES ($1,$2,$3)
           ON CONFLICT DO NOTHING`,
          [rRoom.rows[0].id, userId, false]
        );

        await client.query("COMMIT");
        return sendJson(res, 200, { ok:true, room: rRoom.rows[0] });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/create error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------------- Rooms: JOIN
    if (req.method === "POST" && pathname === "/rooms/join") {
      const userIdAuth = verifyAuth(req);
      if (!userIdAuth) return sendJson(res, 401, { ok:false, error:"unauthorized" });

      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(body.userId || userIdAuth);
      if (!roomId) return sendJson(res, 400, { ok:false, error:"missing_roomId" });

      const rRoom = await q(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
      if (rRoom.rowCount === 0) return sendJson(res, 404, { ok:false, error:"room_not_found" });
      const room = rRoom.rows[0];
      if (room.status !== "open") return sendJson(res, 400, { ok:false, error:"room_not_open" });
      if (room.current_players >= room.max_players) return sendJson(res, 400, { ok:false, error:"room_full" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        const stake = n(room.stake, 0);
        if (stake > 0) {
          const rWal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`, [userId]);
          const bal = n(rWal.rows[0]?.balance, 0);
          if (bal < stake) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"insufficient_funds", balance: bal }); }
          await client.query(`UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE user_id=$2`, [stake, userId]);
          const rBal2 = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
          await client.query(
            `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
             VALUES ($1, $2, 'stake_hold', NULL, $3)`,
            [userId, stake, rBal2.rows[0].balance]
          );
        }

        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready)
           VALUES ($1,$2,false) ON CONFLICT DO NOTHING`,
          [room.id, userId]
        );

        const rUpd = await client.query(
          `UPDATE rooms SET current_players = current_players + 1
             WHERE id=$1 AND status='open' AND current_players < max_players
           RETURNING *`,
          [room.id]
        );
        if (rUpd.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"became_full_try_again" }); }

        await client.query("COMMIT");
        const rBal = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        return sendJson(res, 200, { ok:true, room: rUpd.rows[0], balance: n(rBal.rows[0]?.balance, 0) });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/join error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------------- Rooms: READY (mutual start)
    if (req.method === "POST" && pathname === "/rooms/ready") {
      const userIdAuth = verifyAuth(req);
      if (!userIdAuth) return sendJson(res, 401, { ok:false, error:"unauthorized" });

      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(body.userId || userIdAuth);
      const ready = !!body.ready;
      if (!roomId) return sendJson(res, 400, { ok:false, error:"missing_roomId" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const rRoom = await client.query(`SELECT * FROM rooms WHERE id=$1 FOR UPDATE`, [roomId]);
        if (rRoom.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 404, { ok:false, error:"room_not_found" }); }
        const room = rRoom.rows[0];

        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready)
           VALUES ($1,$2,$3)
           ON CONFLICT (room_id,user_id) DO UPDATE SET ready = EXCLUDED.ready`,
          [room.id, userId, ready]
        );

        const rPlayers = await client.query(
          `SELECT user_id, ready, joined_at FROM room_players WHERE room_id=$1 ORDER BY joined_at ASC`,
          [room.id]
        );

        let startedMatchId = null;
        const everyoneReady =
          room.require_mutual_ready &&
          rPlayers.rows.length === room.max_players &&
          rPlayers.rows.every(p => p.ready);

        if (everyoneReady) {
          await client.query(`UPDATE rooms SET status='in_play' WHERE id=$1`, [room.id]);
          const rMatch = await client.query(
            `INSERT INTO matches (room_id, game, stake, status, started_at)
             VALUES ($1,$2,$3,'ready',NOW()) RETURNING id`,
            [room.id, room.game, room.stake]
          );
          startedMatchId = rMatch.rows[0].id;

          for (const p of rPlayers.rows) {
            await client.query(
              `INSERT INTO match_players (match_id, user_id, stake_amount)
               VALUES ($1, $2, $3)
               ON CONFLICT DO NOTHING`,
              [startedMatchId, p.user_id, n(room.stake, 0)]
            );
          }
        }

        await client.query("COMMIT");
        return sendJson(res, 200, {
          ok:true,
          room: { ...room, status: everyoneReady ? "in_play" : room.status },
          players: rPlayers.rows,
          startedMatchId
        });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/ready error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------------- Rooms: REJOIN (grace)
    if (req.method === "POST" && pathname === "/rooms/rejoin") {
      const userIdAuth = verifyAuth(req);
      if (!userIdAuth) return sendJson(res, 401, { ok:false, error:"unauthorized" });

      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "").trim();
      const userId = await ensureUserWithWallet(body.userId || userIdAuth);
      if (!roomId || !userId) return sendJson(res, 400, { ok:false, error:"missing_params" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        const rRoom = await client.query(`SELECT * FROM rooms WHERE id=$1 FOR UPDATE`, [roomId]);
        if (rRoom.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 404, { ok:false, error:"room_not_found" }); }
        const room = rRoom.rows[0];

        const rMatch = await client.query(
          `SELECT id, status FROM matches
           WHERE room_id=$1 AND status IN ('ready','running','live','in_play')
           ORDER BY created_at DESC LIMIT 1`,
          [room.id]
        );
        if (rMatch.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"no_active_match" }); }
        const match = rMatch.rows[0];

        const rMP = await client.query(
          `SELECT user_id, left_at, grace_until
           FROM match_players WHERE match_id=$1 AND user_id=$2 FOR UPDATE`,
          [match.id, userId]
        );
        if (rMP.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 404, { ok:false, error:"not_in_match" }); }

        const mp = rMP.rows[0];
        if (!mp.grace_until) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"no_grace_set" }); }
        const rNow = await client.query(`SELECT NOW() AS now`);
        const now = new Date(rNow.rows[0].now);
        const graceUntil = new Date(mp.grace_until);
        if (now > graceUntil) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"grace_expired" }); }

        await client.query(
          `UPDATE match_players SET left_at = NULL, grace_until = NULL
           WHERE match_id=$1 AND user_id=$2`,
          [match.id, userId]
        );

        await client.query("COMMIT");
        return sendJson(res, 200, { ok:true, note:"rejoined_success", roomStatus: room.status, matchId: match.id });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/rejoin error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------------- Internal: wallet sync (Firebase -> Postgres)
    if (req.method === "POST" && pathname === "/internal/wallet/sync") {
      // Accept either legacy header or HMAC: X-Sync-Secret OR (X-Sig + X-Timestamp)
      const legacy = req.headers["x-sync-secret"];
      const sig = req.headers["x-sig"];
      const ts = req.headers["x-timestamp"];

      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const uid = String(body.uid || body.userId || "").trim();
      const balance = n(body.balance, NaN);
      if (!uid || !Number.isFinite(balance)) return sendJson(res, 400, { ok:false, error:"bad_params" });

      let okAuth = false;
      if (legacy && legacy === SYNC_SECRET) okAuth = true;
      else if (sig && ts) {
        // HMAC over `${ts}.${uid}.${balance}`
        const base = `${ts}.${uid}.${balance}`;
        const mac = crypto.createHmac("sha256", SYNC_HMAC_KEY).update(base).digest("hex");
        if (crypto.timingSafeEqual(Buffer.from(mac), Buffer.from(String(sig)))) {
          // (optional) reject stale ts > 5 minutes
          const skew = Math.abs(Date.now() - Number(ts));
          if (skew < 5 * 60 * 1000) okAuth = true;
        }
      }
      if (!okAuth) return sendJson(res, 401, { ok:false, error:"unauthorized" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await ensureUserWithWallet(uid);

        await client.query(
          `UPDATE wallets SET balance=$1, updated_at=NOW() WHERE user_id=$2`,
          [balance, uid]
        );
        await client.query(
          `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
           VALUES ($1, 0, 'sync_set', NULL, $2)`,
          [uid, balance]
        );
        await client.query("COMMIT");

        wsBroadcastBalance(uid, balance);
        return sendJson(res, 200, { ok:true });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("wallet/sync error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // Fallback 404
    return sendJson(res, 404, { ok:false, error:"not_found" });

  } catch (e) {
    console.error("HTTP error", e);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  }
});
// ------------------ Auto-cancel stalled rooms (simple cron) ------------------
const INTERVAL_MS = 60_000; // 1 min
setInterval(async () => {
  try {
    // close rooms stuck open for > 3 hours
    await q(`
      UPDATE rooms SET status='closed'
      WHERE status IN ('open','locked')
        AND NOW() - created_at > INTERVAL '3 hours'
    `);
  } catch (e) {
    console.error("cron rooms close error", e);
  }
}, INTERVAL_MS);

// ------------------ Boot ------------------
(async () => {
  try {
    const r = await q(`SELECT 1`).catch(() => null);
    if (!r) throw new Error("DB connection failed (check DB_URL)");
    await runMigrations();

    server.listen(PORT, () => {
      console.log(`✅ CashDeal server listening on :${PORT}`);
      console.log(`✅ migrations done`);
    });
  } catch (e) {
    console.error("Fatal boot error:", e);
    process.exit(1);
  }
})();