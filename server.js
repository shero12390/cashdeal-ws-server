// server.js — HTTP + WebSocket + Postgres (Render-ready, hardened)

const http = require("http");
const url = require("url");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

// ======= Config secrets / env =======
const DB_URL = process.env.DB_URL;
const JWT_SECRET = process.env.JWT_SECRET || "dev_jwt_secret_change";
const SYNC_SECRET = process.env.SYNC_SECRET || "superStrongSecret123";
const RATE_LIMIT_RPS = Number(process.env.RATE_LIMIT_RPS || 8);        // per IP
const RATE_LIMIT_BURST = Number(process.env.RATE_LIMIT_BURST || 16);
const AUTOCANCEL_MINUTES = Number(process.env.AUTOCANCEL_MINUTES || 30);

// ======= Postgres =======
const pool = new Pool({ connectionString: DB_URL, ssl: { rejectUnauthorized: false } });
const q = (sql, params = []) => pool.query(sql, params);

// ======= tiny utils =======
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
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      if (!data) return resolve({});
      try { resolve(JSON.parse(data)); } catch { reject(new Error("invalid_json")); }
    });
    req.on("error", reject);
  });
}
function n(v, f = 0) { const x = Number(v); return Number.isFinite(x) ? x : f; }

// ======= JWT (HS256) verify (no dependency) =======
function b64url(input) {
  return Buffer.from(input).toString("base64").replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}
function b64urlDecode(str) {
  str = str.replace(/-/g, "+").replace(/_/g, "/");
  while (str.length % 4) str += "=";
  return Buffer.from(str, "base64").toString();
}
function verifyJwtHS256(token, secret) {
  const parts = String(token || "").split(".");
  if (parts.length !== 3) return null;
  const [h, p, s] = parts;
  const sig = crypto.createHmac("sha256", secret).update(`${h}.${p}`).digest("base64")
    .replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
  if (sig !== s) return null;
  try {
    const payload = JSON.parse(b64urlDecode(p));
    const now = Math.floor(Date.now()/1000);
    if (payload.exp && now > payload.exp) return null;
    return payload;
  } catch { return null; }
}
// Auth guard: expects Authorization: Bearer <JWT>
function requireAuth(req) {
  const h = req.headers["authorization"] || "";
  const m = /^Bearer\s+(.+)$/.exec(h);
  if (!m) return null;
  return verifyJwtHS256(m[1], JWT_SECRET);
}

// ======= Rate limiting (simple token bucket per IP) =======
const buckets = new Map(); // ip -> {tokens, last}
function rateLimited(ip) {
  const now = Date.now();
  const b = buckets.get(ip) || { tokens: RATE_LIMIT_BURST, last: now };
  const elapsed = (now - b.last) / 1000;
  b.tokens = Math.min(RATE_LIMIT_BURST, b.tokens + elapsed * RATE_LIMIT_RPS);
  if (b.tokens < 1) {
    buckets.set(ip, b);
    return true;
  }
  b.tokens -= 1;
  b.last = now;
  buckets.set(ip, b);
  return false;
}

// ======= DB migrations (includes idempotency table) =======
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
      max_players INT NOT NULL CHECK (max_players >= 2 AND max_players <= 10),
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
      reason TEXT NOT NULL, -- deposit | withdraw | stake_hold | stake_release | payout | refund
      ref_match_id UUID REFERENCES matches(id) ON DELETE SET NULL,
      balance_after NUMERIC(18,2) NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await q(`CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id);`);

  // For /matches/finish idempotency
  await q(`
    CREATE TABLE IF NOT EXISTS idempotency_keys (
      key TEXT PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      response JSONB NOT NULL DEFAULT '{}'::jsonb
    );
  `);

  console.log("✅ migrations done");
}

// ======= helper: ensure user + wallet =======
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

// ======= WebSocket: subscriptions for balance + room/match events =======
const wss = new WebSocketServer({ noServer: true });
const subsBalance = new Map(); // userId -> Set<ws>

function wsSend(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch {}
}
function pushBalance(userId, balance) {
  const set = subsBalance.get(String(userId));
  if (!set) return;
  for (const ws of set) wsSend(ws, { type: "balance.update", userId, balance });
}

wss.on("connection", (ws) => {
  ws.on("message", (m) => {
    let msg;
    try { msg = JSON.parse(String(m)); } catch { return; }
    if (msg?.type === "subscribe.balance" && msg.userId) {
      const uid = String(msg.userId);
      if (!subsBalance.has(uid)) subsBalance.set(uid, new Set());
      subsBalance.get(uid).add(ws);
      wsSend(ws, { type: "balance.subscribed", userId: uid });
    }
  });
  ws.on("close", () => {
    for (const set of subsBalance.values()) set.delete(ws);
  });
});

const server = http.createServer(async (req, res) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "access-control-allow-origin": "*",
      "access-control-allow-methods": "GET,POST,OPTIONS",
      "access-control-allow-headers": "content-type,authorization,x-idempotency-key,x-sync-secret,x-sync-sig",
    });
    return res.end();
  }

  const { pathname, query } = url.parse(req.url, true);
  const ip = req.socket.remoteAddress || "0.0.0.0";
  if (rateLimited(ip)) return sendJson(res, 429, { ok:false, error:"rate_limited" });

  // health + metrics (no auth)
  if (pathname === "/health") return sendJson(res, 200, { ok: true });
  if (pathname === "/db/health") {
    const r = await q(`SELECT NOW() now`);
    return sendJson(res, 200, { ok: true, dbTime: r.rows[0].now });
  }
  if (pathname === "/metrics") {
    return sendJson(res, 200, {
      ok: true,
      uptime_s: Math.floor(process.uptime()),
      mem_rss_mb: Math.round(process.memoryUsage().rss/1024/1024),
      ws_subscribers: [...subsBalance.values()].reduce((a,s)=>a+s.size,0),
      rate_limit_rps: RATE_LIMIT_RPS,
    });
  }

  // From here down, most endpoints expect JWT (except internal sync which uses HMAC)
  // Allow wallets/deposit for testing without JWT — you can flip this to requireAuth if needed.

try {
    // ---------- WALLET: DEPOSIT (test only; no JWT by design) ----------
    if (req.method === "POST" && pathname === "/wallet/deposit") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok: false, error: "invalid_json" }); }
      const userIdIn = String(body.userId || "").trim();
      const amount = Number(body.amount);
      if (!Number.isFinite(amount) || amount <= 0) return sendJson(res, 400, { ok:false, error:"invalid_amount" });

      const userId = await ensureUserWithWallet(userIdIn);
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(
          `UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE user_id=$2`,
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
        pushBalance(userId, newBal);
        return sendJson(res, 200, { ok:true, userId, wallet:{ balance:newBal, currency:"USDT" } });
      } catch (e) {
        try{ await client.query("ROLLBACK"); }catch{}
        console.error("deposit error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------- AUTH GUARD (JWT) for gameplay endpoints ----------
    const jwt = requireAuth(req);
    if (!jwt) return sendJson(res, 401, { ok:false, error:"unauthorized" });

    // ---------- ROOMS: CREATE ----------
    if (req.method === "POST" && pathname === "/rooms/create") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const userId = await ensureUserWithWallet(body.userId || jwt.sub);
      const game = String(body.game || "ludo");
      const stake = n(body.stake, 0);
      const maxPlayers = Number.isInteger(body.maxPlayers) ? body.maxPlayers : n(body.maxPlayers, 2);
      const mode = String(body.mode || "h2h");
      const minPlayers = Number.isInteger(body.minPlayers) ? body.minPlayers : 2;
      const autostart = !!body.autostart;
      const countdownSeconds = Number.isInteger(body.countdownSeconds) ? body.countdownSeconds : 0;
      const requireMutual = !!body.require_mutual_ready;
      const config = body.config && typeof body.config === "object" ? body.config : {};

      if (!Number.isInteger(maxPlayers) || maxPlayers < 2 || maxPlayers > 10) return sendJson(res, 400, { ok:false, error:"invalid_maxPlayers" });
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
          pushBalance(userId, Number(rBal2.rows[0].balance));
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
           VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
          [rRoom.rows[0].id, userId, false]
        );

        await client.query("COMMIT");
        return sendJson(res, 200, { ok:true, room: rRoom.rows[0] });
      } catch (e) {
        try{ await client.query("ROLLBACK"); }catch{}
        console.error("rooms/create error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------- ROOMS: JOIN ----------
    if (req.method === "POST" && pathname === "/rooms/join") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(body.userId || jwt.sub);
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
          pushBalance(userId, Number(rBal2.rows[0].balance));
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
        try{ await client.query("ROLLBACK"); }catch{}
        console.error("rooms/join error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------- ROOMS: READY (mutual ready & start match) ----------
    if (req.method === "POST" && pathname === "/rooms/ready") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(body.userId || jwt.sub);
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
        try{ await client.query("ROLLBACK"); }catch{}
        console.error("rooms/ready error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------- ROOMS: REJOIN (within grace) ----------
    if (req.method === "POST" && pathname === "/rooms/rejoin") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "").trim();
      const userId = await ensureUserWithWallet(body.userId || jwt.sub);
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
             FROM match_players
            WHERE match_id=$1 AND user_id=$2
            FOR UPDATE`,
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
          `UPDATE match_players
              SET left_at = NULL, grace_until = NULL
            WHERE match_id=$1 AND user_id=$2`,
          [match.id, userId]
        );

        await client.query("COMMIT");
        return sendJson(res, 200, { ok: true, note: "rejoined_success", roomStatus: room.status, matchId: match.id });
      } catch (e) {
        try{ await client.query("ROLLBACK"); }catch{}
        console.error("rooms/rejoin error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------- ROOMS: LEAVE (no refund; 90s grace) ----------
    if (req.method === "POST" && pathname === "/rooms/leave") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId  = String(body.roomId || "").trim();
      const userId  = await ensureUserWithWallet(body.userId || jwt.sub);
      const GRACE_S = 90;
      if (!roomId) return sendJson(res, 400, { ok:false, error:"missing_roomId" });

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

        if (rMatch.rowCount > 0) {
          const match = rMatch.rows[0];
          const rMP = await client.query(
            `SELECT match_id, user_id FROM match_players WHERE match_id=$1 AND user_id=$2 FOR UPDATE`,
            [match.id, userId]
          );
          if (rMP.rowCount === 0) { await client.query("COMMIT"); return sendJson(res, 200, { ok:true, note:"not_in_match", room, matchId: match.id }); }

          const rGrace = await client.query(
            `UPDATE match_players
                SET left_at = NOW(),
                    grace_until = NOW() + make_interval(secs => $1)
              WHERE match_id=$2 AND user_id=$3
              RETURNING left_at, grace_until`,
            [GRACE_S, match.id, userId]
          );

          await client.query("COMMIT");
          return sendJson(res, 200, {
            ok: true,
            note: "left_temporarily_no_refund",
            roomStatus: room.status,
            matchId: match.id,
            grace: rGrace.rows[0]
          });
        }

        // lobby leave, no refund per your policy
        await client.query(`DELETE FROM room_players WHERE room_id=$1 AND user_id=$2`, [room.id, userId]).catch(()=>null);
        await client.query(`UPDATE rooms SET current_players = GREATEST(current_players - 1, 0) WHERE id=$1`, [room.id]);

        await client.query("COMMIT");
        const rAfter = await q(`SELECT * FROM rooms WHERE id=$1`, [room.id]);
        return sendJson(res, 200, { ok:true, note:"left_lobby_no_refund", room: rAfter.rows[0] });
      } catch (e) {
        try{ await client.query("ROLLBACK"); }catch{}
        console.error("rooms/leave error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ---------- ROOMS: LIST ----------
    if (req.method === "GET" && pathname === "/rooms/list") {
      const status = String(query.status || "open");
      const r = await q(
        `SELECT id, host_user_id, game, stake, max_players, current_players, status,
                created_at, mode, min_players, autostart, countdown_seconds, require_mutual_ready, config
           FROM rooms
          WHERE status=$1
          ORDER BY created_at DESC LIMIT 100`,
        [status]
      );
      return sendJson(res, 200, { ok:true, rooms: r.rows });
    }

    // ---------- INTERNAL WALLET SYNC (HMAC) ----------
    if (req.method === "POST" && pathname === "/internal/wallet/sync") {
      // HMAC-SHA256 of raw body using SYNC_SECRET. Clients must send header: x-sync-sig: sha256=<hex>
      let raw = "";
      req.on("data", (c)=> raw += c);
      await new Promise((r)=> req.on("end", r));
      const sig = String(req.headers["x-sync-sig"] || "");
      const want = "sha256=" + crypto.createHmac("sha256", SYNC_SECRET).update(raw).digest("hex");
      if (sig !== want && (req.headers["x-sync-secret"] !== SYNC_SECRET)) {
        return sendJson(res, 401, { ok:false, error:"unauthorized" });
      }
      let body = {};
      try { body = JSON.parse(raw || "{}"); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }

      const uid = await ensureUserWithWallet(body.uid);
      const balance = n(body.balance, NaN);
      if (!Number.isFinite(balance) || balance < 0) return sendJson(res, 400, { ok:false, error:"invalid_balance" });

      const rWal = await q(`UPDATE wallets SET balance=$1, updated_at=NOW() WHERE user_id=$2 RETURNING balance`, [balance, uid]);
      pushBalance(uid, Number(rWal.rows[0].balance));
      return sendJson(res, 200, { ok:true });
    }

    // ---------- MATCHES: FINISH (idempotent) ----------
    if (req.method === "POST" && pathname === "/matches/finish") {
      const idk = String(req.headers["x-idempotency-key"] || "").trim();
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const matchId = String(body.matchId || "").trim();
      const results = Array.isArray(body.results) ? body.results : [];
      if (!matchId || results.length === 0) return sendJson(res, 400, { ok:false, error:"missing_params" });

      // Idempotency fast-path
      if (idk) {
        const idr = await q(`SELECT response FROM idempotency_keys WHERE key=$1`, [idk]);
        if (idr.rowCount > 0) return sendJson(res, 200, idr.rows[0].response);
      }

      const rMatch = await q(`SELECT id, room_id, game, stake, status FROM matches WHERE id=$1`, [matchId]);
      if (rMatch.rowCount === 0) return sendJson(res, 404, { ok:false, error:"match_not_found" });
      const match = rMatch.rows[0];
      if (!["ready","running","live","in_play"].includes(match.status)) {
        return sendJson(res, 400, { ok:false, error:"invalid_state", status: match.status });
      }

      const rPlayers = await q(`SELECT user_id FROM match_players WHERE match_id=$1`, [matchId]);
      const playerIds = new Set(rPlayers.rows.map(r => String(r.user_id)));
      const allowed = new Set(["won","lost","draw","cancelled"]);
      const incoming = new Map(
        results
          .map(r => [String(r.userId || "").trim(), String(r.result || "").trim()])
          .filter(([uid, res]) => uid && allowed.has(res))
      );
      if (incoming.size !== playerIds.size || [...playerIds].some(uid => !incoming.has(uid))) {
        return sendJson(res, 400, { ok:false, error:"results_must_cover_all_players" });
      }

      const winners = [...incoming.entries()].filter(([,res]) => res === "won").map(([uid]) => uid);
      const perPlayerStake = n(match.stake, 0);
      const prizePool = perPlayerStake * playerIds.size;
      const perWinner = winners.length > 0 ? prizePool / winners.length : 0;

      const client = await pool.connect();
      let response;
      try {
        await client.query("BEGIN");

        // update results
        const vals = [...incoming.entries()];
        const placeholders = vals.map((_, i) => `($${i*2+1}::uuid,$${i*2+2}::text)`).join(",");
        await client.query(
          `UPDATE match_players mp
              SET result = v.res
             FROM (VALUES ${placeholders}) AS v(uid,res)
            WHERE mp.match_id = $${vals.length*2+1}::uuid
              AND mp.user_id = v.uid`,
          [...vals.flatMap(([uid,res]) => [uid,res]), matchId]
        );

        // credit winners
        if (perWinner > 0 && winners.length > 0) {
          for (const uid of winners) {
            await client.query(`UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE user_id=$2`, [perWinner, uid]);
            const rBal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [uid]);
            await client.query(
              `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
               VALUES ($1, $2, 'payout', $3, $4)`,
              [uid, perWinner, matchId, rBal.rows[0].balance]
            );
            pushBalance(uid, Number(rBal.rows[0].balance));
          }
        }

        await client.query(`UPDATE matches SET status='finished', ended_at = NOW() WHERE id=$1`, [matchId]);
        if (match.room_id) await client.query(`UPDATE rooms SET status='closed' WHERE id=$1`, [match.room_id]);

        const rDone = await client.query(
          `SELECT id, room_id, game, stake, status, started_at, ended_at FROM matches WHERE id=$1`,
          [matchId]
        );
        response = { ok:true, match: rDone.rows[0], payouts: (perWinner>0 ? { winners, perWinner } : null) };

        await client.query("COMMIT");
      } catch (e) {
        try{ await client.query("ROLLBACK"); }catch{}
        console.error("matches/finish error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }

      if (idk) {
        await q(`INSERT INTO idempotency_keys (key, response) VALUES ($1,$2)
                 ON CONFLICT (key) DO UPDATE SET response = EXCLUDED.response`, [idk, response]);
      }
      return sendJson(res, 200, response);
    }

    // ---------- default 404 ----------
    return sendJson(res, 404, { ok:false, error:"not_found" });

  } catch (err) {
    console.error("HTTP error:", err);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  }
});
// ======= WebSocket upgrade =======
server.on("upgrade", (req, socket, head) => {
  if (req.url === "/ws") {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  } else {
    socket.destroy();
  }
});

// ======= Auto-cancel stale rooms job =======
// Policy: if room is 'open' for > AUTOCANCEL_MINUTES and did not reach min_players,
// close it and refund stake holds to whoever is in room_players + host.
// (We must refund here to avoid stuck funds. Your manual "leave=no refund" rule
// remains for user-initiated leaves; system cancel is different.)
async function autoCancelSweep() {
  try {
    const r = await q(
      `SELECT *
         FROM rooms
        WHERE status='open'
          AND created_at < NOW() - ($1::int || ' minutes')::interval`,
      [AUTOCANCEL_MINUTES]
    );
    for (const room of r.rows) {
      const players = await q(`SELECT user_id FROM room_players WHERE room_id=$1`, [room.id]);
      const allUsers = new Set(players.rows.map(x => String(x.user_id)));
      allUsers.add(String(room.host_user_id));

      if (room.stake > 0) {
        const client = await pool.connect();
        try {
          await client.query("BEGIN");
          for (const uid of allUsers) {
            await client.query(
              `UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE user_id=$2`,
              [room.stake, uid]
            );
            const rBal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [uid]);
            await client.query(
              `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
               VALUES ($1, $2, 'stake_release', NULL, $3)`,
              [uid, room.stake, rBal.rows[0].balance]
            );
            pushBalance(uid, Number(rBal.rows[0].balance));
          }
          await client.query(`UPDATE rooms SET status='closed' WHERE id=$1`, [room.id]);
          await client.query("COMMIT");
          console.log(`Auto-cancelled room ${room.id}`);
        } catch (e) {
          try { await client.query("ROLLBACK"); } catch {}
          console.error("autoCancel sweep error for room", room.id, e);
        } finally { client.release(); }
      } else {
        await q(`UPDATE rooms SET status='closed' WHERE id=$1`, [room.id]);
        console.log(`Auto-closed free room ${room.id}`);
      }
    }
  } catch (e) {
    console.error("autoCancelSweep error", e);
  }
}
setInterval(autoCancelSweep, 60 * 1000); // run every minute

// ======= Boot =======
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