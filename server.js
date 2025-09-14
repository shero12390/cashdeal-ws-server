// server.js — CashDeal HTTP + WebSocket + Postgres (Render-ready)

// ───────────────────────── Imports & Setup
const http = require("http");
const url = require("url");
const crypto = require("crypto");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;
const DB_URL = process.env.DB_URL;
const SYNC_SECRET = process.env.SYNC_SECRET || "superStrongSecret123"; // for /internal/wallet/sync HMAC
const AUTH_JWT_SECRET = process.env.AUTH_JWT_SECRET || "dev_secret";
const DEV_MINT_SECRET = process.env.DEV_MINT_SECRET || "";              // for /auth/dev QA mint
const TOKEN_TTL_SECONDS = Number(process.env.TOKEN_TTL_SECONDS || 3600);

// ───────────────────────── DB
const pool = new Pool({
  connectionString: DB_URL,
  ssl: { rejectUnauthorized: false },
});
const q = (sql, params = []) => pool.query(sql, params);

// ───────────────────────── Small utils
function nowSec() { return Math.floor(Date.now() / 1000); }
function n(v, f = 0) { const x = Number(v); return Number.isFinite(x) ? x : f; }
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
    req.on("data", c => data += c);
    req.on("end", () => {
      if (!data) return resolve({});
      try { resolve(JSON.parse(data)); }
      catch { reject(new Error("invalid_json")); }
    });
    req.on("error", reject);
  });
}
function bearerFrom(req) {
  const h = req.headers["authorization"];
  if (!h) return null;
  const m = /^Bearer\s+(.+)$/i.exec(h);
  return m ? m[1] : null;
}

// ───────────────────────── JWT auth (tiny HS256)
function b64url(b){return Buffer.from(b).toString("base64").replace(/=/g,"").replace(/\+/g,"-").replace(/\//g,"_")}
function signJwt(payload, ttlSec = TOKEN_TTL_SECONDS) {
  const header = { alg:"HS256", typ:"JWT" };
  const now = nowSec();
  const pl = { iat: now, exp: now + ttlSec, ...payload };
  const h = b64url(JSON.stringify(header));
  const p = b64url(JSON.stringify(pl));
  const data = `${h}.${p}`;
  const sig = crypto.createHmac("sha256", AUTH_JWT_SECRET).update(data).digest("base64")
    .replace(/=/g,"").replace(/\+/g,"-").replace(/\//g,"_");
  return `${data}.${sig}`;
}
function verifyJwt(token) {
  try {
    const [h,p,s] = token.split(".");
    if (!h || !p || !s) return null;
    const data = `${h}.${p}`;
    const expect = crypto.createHmac("sha256", AUTH_JWT_SECRET).update(data).digest("base64")
      .replace(/=/g,"").replace(/\+/g,"-").replace(/\//g,"_");
    if (expect !== s) return null;
    const payload = JSON.parse(Buffer.from(p,"base64").toString("utf8"));
    if (payload.exp && nowSec() > payload.exp) return null;
    return payload;
  } catch { return null; }
}
async function requireAuth(req, res) {
  const tok = bearerFrom(req);
  if (!tok) { sendJson(res, 401, { ok:false, error:"unauthorized" }); return null; }
  const payload = verifyJwt(tok);
  if (!payload || !payload.sub) { sendJson(res, 401, { ok:false, error:"unauthorized" }); return null; }
  return payload.sub; // userId
}

// ───────────────────────── Simple in-memory rate limiter (per IP)
const buckets = new Map();
const RATE_LIMIT = Number(process.env.RATE_LIMIT_PER_MIN || 120);
function rateLimit(req, res) {
  const ip = req.headers["x-forwarded-for"] || req.socket.remoteAddress || "na";
  const now = Date.now();
  let b = buckets.get(ip);
  if (!b || now - b.t > 60_000) b = { t: now, n: 0 };
  b.n++;
  buckets.set(ip, b);
  if (b.n > RATE_LIMIT) { sendJson(res, 429, { ok:false, error:"rate_limited" }); return false; }
  return true;
}

// ───────────────────────── Migrations
async function runMigrations() {
  await q(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);
  await q(`CREATE EXTENSION IF NOT EXISTS "pgcrypto";`);

  await q(`CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );`);

  await q(`CREATE TABLE IF NOT EXISTS wallets (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    balance NUMERIC(18,2) NOT NULL DEFAULT 0,
    currency TEXT NOT NULL DEFAULT 'USDT',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );`);

  await q(`CREATE TABLE IF NOT EXISTS rooms (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    host_user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    game TEXT NOT NULL,
    stake NUMERIC(18,2) NOT NULL DEFAULT 0,
    max_players INT NOT NULL CHECK (max_players >= 2 AND max_players <= 10),
    current_players INT NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'open', -- open|locked|in_play|full|closed
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mode TEXT NOT NULL DEFAULT 'h2h',
    min_players INT NOT NULL DEFAULT 2,
    autostart BOOLEAN NOT NULL DEFAULT false,
    countdown_seconds INT NOT NULL DEFAULT 0,
    require_mutual_ready BOOLEAN NOT NULL DEFAULT false,
    config JSONB NOT NULL DEFAULT '{}'::jsonb
  );`);
  await q(`CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);`);

  await q(`CREATE TABLE IF NOT EXISTS room_players (
    room_id UUID NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    ready BOOLEAN NOT NULL DEFAULT false,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (room_id, user_id)
  );`);

  await q(`CREATE TABLE IF NOT EXISTS matches (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    room_id UUID REFERENCES rooms(id) ON DELETE SET NULL,
    game TEXT NOT NULL,
    stake NUMERIC(18,2) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'ready', -- ready|running|live|finished|cancelled
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );`);

  await q(`CREATE TABLE IF NOT EXISTS match_players (
    match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    user_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    seat SMALLINT,
    result TEXT NOT NULL DEFAULT 'pending', -- pending|won|lost|draw|cancelled
    stake_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
    left_at TIMESTAMPTZ,
    grace_until TIMESTAMPTZ,
    PRIMARY KEY (match_id, user_id)
  );`);

  await q(`CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount NUMERIC(18,2) NOT NULL,
    reason TEXT NOT NULL, -- deposit|withdraw|stake_hold|stake_release|payout|refund
    ref_match_id UUID REFERENCES matches(id) ON DELETE SET NULL,
    balance_after NUMERIC(18,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );`);
  await q(`CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id);`);

  // Idempotency keys (for /matches/finish)
  await q(`CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );`);

  console.log("✅ migrations done");
}

// ───────────────────────── User bootstrap
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

// ───────────────────────── WebSocket hub (balance subscriptions)
const wss = new WebSocketServer({ noServer: true });
const subsBalance = new Map(); // userId -> Set(ws)

function subAdd(userId, ws) {
  if (!subsBalance.has(userId)) subsBalance.set(userId, new Set());
  subsBalance.get(userId).add(ws);
}
function subDel(userId, ws) {
  const s = subsBalance.get(userId);
  if (!s) return;
  s.delete(ws);
  if (s.size === 0) subsBalance.delete(userId);
}
async function broadcastBalance(userId) {
  const s = subsBalance.get(userId);
  if (!s || s.size === 0) return;
  const r = await q(`SELECT balance,currency FROM wallets WHERE user_id=$1`, [userId]);
  const bal = r.rowCount ? String(r.rows[0].balance) : "0";
  const cur = r.rowCount ? r.rows[0].currency : "USDT";
  const msg = JSON.stringify({ type:"balance.update", userId, balance: bal, currency: cur });
  for (const ws of s) {
    if (ws.readyState === 1) try { ws.send(msg); } catch {}
  }
}

wss.on("connection", (ws, req) => {
  // Auth via ?token=...
  const { query } = url.parse(req.url, true);
  const tok = query && query.token ? String(query.token) : null;
  const payload = tok ? verifyJwt(tok) : null;
  if (!payload || !payload.sub) { try { ws.close(4401, "unauthorized"); } catch{}; return; }
  ws.userId = payload.sub;

  ws.on("message", async (buf) => {
    let msg = null;
    try { msg = JSON.parse(buf.toString("utf8")); } catch { return; }
    if (!msg || typeof msg !== "object") return;

    if (msg.type === "ping") {
      try { ws.send(JSON.stringify({ type:"pong", ts: msg.ts || Date.now() })); } catch {}
      return;
    }
    if (msg.type === "subscribe.balance") {
      const who = String(msg.userId || ws.userId);
      if (who !== ws.userId) return; // cannot subscribe to others
      subAdd(who, ws);
      try { ws.send(JSON.stringify({ type:"balance.subscribed", userId: who })); } catch {}
      // push immediate snapshot
      broadcastBalance(who);
      return;
    }
  });

  ws.on("close", () => { try { subDel(ws.userId, ws); } catch {} });
});

// HTTP→WS upgrade
const server = http.createServer(asyncHandler);
server.on("upgrade", (req, socket, head) => {
  const { pathname } = url.parse(req.url);
  if (pathname === "/ws") {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  } else {
    socket.destroy();
  }
});

// Kick migrations on boot
runMigrations().catch(err => { console.error("migrations failed", err); process.exit(1); });

// Start
server.listen(PORT, () => console.log(`🚀 CashDeal server listening on :${PORT}`));
// ───────────────────────── HTTP Router
async function asyncHandler(req, res) {
  try {
    // CORS preflight
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET,POST,OPTIONS",
        "access-control-allow-headers": "content-type,authorization,x-idempotency-key,x-dev-secret,x-signature,x-timestamp"
      });
      return res.end();
    }
    if (!rateLimit(req, res)) return;

    const { pathname, query } = url.parse(req.url, true);

    // Health
    if (pathname === "/health") return sendJson(res, 200, { ok:true });
    if (pathname === "/db/health") {
      const r = await q(`SELECT NOW() now`);
      return sendJson(res, 200, { ok:true, dbTime: r.rows[0].now });
    }

    // ── Dev token mint (QA only)
    if (req.method === "POST" && pathname === "/auth/dev") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, {ok:false,error:"invalid_json"}); }
      const devSecret = req.headers["x-dev-secret"] || "";
      if (!DEV_MINT_SECRET || devSecret !== DEV_MINT_SECRET) return sendJson(res, 401, { ok:false, error:"unauthorized" });
      const uid = String(body.uid || "").trim();
      if (!uid) return sendJson(res, 400, { ok:false, error:"missing_uid" });
      const token = signJwt({ sub: uid });
      return sendJson(res, 200, { ok:true, token, expiresIn: TOKEN_TTL_SECONDS });
    }

    // ── Internal wallet sync (Firebase→PG), HMAC: sha256(timestamp.body with SYNC_SECRET)
    if (req.method === "POST" && pathname === "/internal/wallet/sync") {
      const ts = String(req.headers["x-timestamp"] || "");
      const sig = String(req.headers["x-signature"] || "");
      const raw = await new Promise((resolve) => {
        let d = ""; req.on("data", c => d+=c); req.on("end", () => resolve(d));
      });
      const want = crypto.createHmac("sha256", SYNC_SECRET).update(ts + "." + raw).digest("hex");
      if (!ts || !sig || !crypto.timingSafeEqual(Buffer.from(sig), Buffer.from(want)))
        return sendJson(res, 401, { ok:false, error:"unauthorized" });
      let body; try { body = JSON.parse(raw); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }

      const uid = String(body.uid || body.userId || "").trim();
      const balance = n(body.balance, NaN);
      if (!uid || !Number.isFinite(balance)) return sendJson(res, 400, { ok:false, error:"bad_payload" });

      await ensureUserWithWallet(uid);
      await q(`UPDATE wallets SET balance=$1, updated_at=NOW() WHERE user_id=$2`, [balance, uid]);
      await broadcastBalance(uid);
      return sendJson(res, 200, { ok:true });
    }

    // ── Testing deposit (manual)
    if (req.method === "POST" && pathname === "/wallet/deposit") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const userIdIn = String(body.userId || "").trim();
      const amount = Number(body.amount);
      if (!Number.isFinite(amount) || amount <= 0) return sendJson(res, 400, { ok:false, error:"invalid_amount" });

      const userId = await ensureUserWithWallet(userIdIn);
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(`UPDATE wallets SET balance=balance+$1, updated_at=NOW() WHERE user_id=$2`, [amount, userId]);
        const rBal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        await client.query(
          `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
           VALUES ($1,$2,'deposit',NULL,$3)`,
          [userId, amount, rBal.rows[0].balance]
        );
        await client.query("COMMIT");
        await broadcastBalance(userId);
        return sendJson(res, 200, { ok:true, userId, wallet:{ balance:Number(rBal.rows[0].balance), currency:"USDT" } });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("deposit error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ── Rooms: CREATE (auth)
    if (req.method === "POST" && pathname === "/rooms/create") {
      const authed = await requireAuth(req, res); if (!authed) return;
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }

      const userId = await ensureUserWithWallet(authed);
      const game = String(body.game || "ludo");
      const stake = n(body.stake, 0);
      const maxPlayers = Number.isInteger(body.maxPlayers) ? body.maxPlayers : 2;
      const mode = String(body.mode || "h2h");
      const minPlayers = Number.isInteger(body.minPlayers) ? body.minPlayers : 2;
      const autostart = !!body.autostart;
      const countdownSeconds = Number.isInteger(body.countdownSeconds) ? body.countdownSeconds : 0;
      const requireMutual = !!body.require_mutual_ready;
      const config = body.config && typeof body.config === "object" ? body.config : {};

      if (maxPlayers < 2 || maxPlayers > 10) return sendJson(res, 400, { ok:false, error:"invalid_maxPlayers" });
      if (stake < 0) return sendJson(res, 400, { ok:false, error:"invalid_stake" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        if (stake > 0) {
          const rWal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`, [userId]);
          const bal = n(rWal.rows[0]?.balance, 0);
          if (bal < stake) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"insufficient_funds", balance: bal }); }
          await client.query(`UPDATE wallets SET balance=balance-$1, updated_at=NOW() WHERE user_id=$2`, [stake, userId]);
          const r2 = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
          await client.query(
            `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
             VALUES ($1,$2,'stake_hold',NULL,$3)`,
            [userId, stake, r2.rows[0].balance]
          );
        }
        const rRoom = await client.query(
          `INSERT INTO rooms (host_user_id, game, stake, max_players, current_players, status,
                              mode, min_players, autostart, countdown_seconds, require_mutual_ready, config)
           VALUES ($1,$2,$3,$4,1,'open',$5,$6,$7,$8,$9,$10)
           RETURNING *`,
          [userId, game, stake, maxPlayers, mode, minPlayers, autostart, countdownSeconds, requireMutual, JSON.stringify(config)]
        );
        await client.query(`INSERT INTO room_players (room_id, user_id, ready) VALUES ($1,$2,false) ON CONFLICT DO NOTHING`, [rRoom.rows[0].id, userId]);

        await client.query("COMMIT");
        await broadcastBalance(userId);
        return sendJson(res, 200, { ok:true, room: rRoom.rows[0] });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/create error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ── Rooms: JOIN (auth)
    if (req.method === "POST" && pathname === "/rooms/join") {
      const authed = await requireAuth(req, res); if (!authed) return;
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(authed);
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
          await client.query(`UPDATE wallets SET balance=balance-$1, updated_at=NOW() WHERE user_id=$2`, [stake, userId]);
          const r2 = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
          await client.query(
            `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
             VALUES ($1,$2,'stake_hold',NULL,$3)`,
            [userId, stake, r2.rows[0].balance]
          );
        }
        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready)
           VALUES ($1,$2,false) ON CONFLICT DO NOTHING`,
          [room.id, userId]
        );
        const rUpd = await client.query(
          `UPDATE rooms SET current_players=current_players+1
             WHERE id=$1 AND status='open' AND current_players < max_players
           RETURNING *`,
          [room.id]
        );
        if (rUpd.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"became_full_try_again" }); }
        await client.query("COMMIT");
        await broadcastBalance(userId);
        const rBal = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        return sendJson(res, 200, { ok:true, room: rUpd.rows[0], balance: n(rBal.rows[0]?.balance, 0) });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/join error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ── Rooms: READY (auth; may auto-start match if mutual ready)
    if (req.method === "POST" && pathname === "/rooms/ready") {
      const authed = await requireAuth(req, res); if (!authed) return;
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(authed);
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
        const rPlayers = await client.query(`SELECT user_id, ready, joined_at FROM room_players WHERE room_id=$1 ORDER BY joined_at ASC`, [room.id]);

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
               VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`,
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

    // ── Rooms: REJOIN (auth; within grace)
    if (req.method === "POST" && pathname === "/rooms/rejoin") {
      const authed = await requireAuth(req, res); if (!authed) return;
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "").trim();
      const userId = await ensureUserWithWallet(authed);
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
        if (new Date(rNow.rows[0].now) > new Date(mp.grace_until)) {
          await client.query("ROLLBACK"); return sendJson(res, 400, { ok:false, error:"grace_expired" });
        }
        await client.query(`UPDATE match_players SET left_at=NULL, grace_until=NULL WHERE match_id=$1 AND user_id=$2`, [match.id, userId]);
        await client.query("COMMIT");
        return sendJson(res, 200, { ok:true, note:"rejoined_success", roomStatus: room.status, matchId: match.id });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/rejoin error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // ── Rooms: LEAVE (auth; 90s grace if match live; no refund here)
    if (req.method === "POST" && pathname === "/rooms/leave") {
      const authed = await requireAuth(req, res); if (!authed) return;
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId  = String(body.roomId || "").trim();
      const userId  = await ensureUserWithWallet(authed);
      const GRACE_S = 90;
      if (!roomId) return sendJson(res, 400, { ok:false, error:"missing_roomId" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const rRoom = await client.query(`SELECT * FROM rooms WHERE id=$1 FOR UPDATE`, [roomId]);
        if (rRoom.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 404, { ok:false, error:"room_not_found" }); }
        const room = rRoom.rows[0];

        const rMatch = await client.query(
          `SELECT id,status FROM matches WHERE room_id=$1 AND status IN ('ready','running','live','in_play')
           ORDER BY created_at DESC LIMIT 1`, [room.id]
        );

        if (rMatch.rowCount > 0) {
          const match = rMatch.rows[0];
          const rNow = await client.query(`SELECT NOW() AS now`);
          const graceUntil = new Date(new Date(rNow.rows[0].now).getTime() + GRACE_S*1000);
          await client.query(
            `UPDATE match_players SET left_at=NOW(), grace_until=$3 WHERE match_id=$1 AND user_id=$2`,
            [match.id, userId, graceUntil]
          );
        }

        await client.query(`DELETE FROM room_players WHERE room_id=$1 AND user_id=$2`, [room.id, userId]);
        await client.query(`UPDATE rooms SET current_players=GREATEST(current_players-1,0) WHERE id=$1`, [room.id]);

        await client.query("COMMIT");
        return sendJson(res, 200, { ok:true, note:"left_room" });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/leave error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

    // fallthrough
    return sendJson(res, 404, { ok:false, error:"not_found" });

  } catch (e) {
    console.error("http error", e);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  }
}
// (Append to same file, after Part 2 router, before module end)

// ───────────────────────── Matches: FINISH (auth + idempotency)
async function finishMatchHandler(req, res) {
  const authed = await requireAuth(req, res); if (!authed) return;

  let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
  const matchId = String(body.matchId || "");
  const results = body.results; // e.g., [{userId:..., outcome:"won"|... , payout: number}, ...]
  const idemKey = String(req.headers["x-idempotency-key"] || "").trim();
  if (!matchId || !Array.isArray(results) || results.length === 0) {
    return sendJson(res, 400, { ok:false, error:"bad_payload" });
  }

  // Idempotency gate
  if (idemKey) {
    try {
      await q(`INSERT INTO idempotency_keys(key) VALUES ($1)`, [idemKey]);
    } catch (e) {
      // duplicate key -> already processed
      return sendJson(res, 200, { ok:true, idempotent:true });
    }
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Lock match
    const rM = await client.query(`SELECT * FROM matches WHERE id=$1 FOR UPDATE`, [matchId]);
    if (rM.rowCount === 0) { await client.query("ROLLBACK"); return sendJson(res, 404, { ok:false, error:"match_not_found" }); }
    const match = rM.rows[0];
    if (match.status === "finished") { await client.query("ROLLBACK"); return sendJson(res, 200, { ok:true, already:true }); }

    // Payout/refund
    for (const r of results) {
      const userId = String(r.userId || "").trim();
      if (!userId) continue;
      const payout = n(r.payout, 0);
      if (!Number.isFinite(payout)) continue;

      if (payout !== 0) {
        // credit/debit wallet
        await client.query(`UPDATE wallets SET balance=balance+$1, updated_at=NOW() WHERE user_id=$2`, [payout, userId]);
        const rBal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        const reason = payout >= 0 ? 'payout' : 'stake_release'; // negative payout == release/fee
        await client.query(
          `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
           VALUES ($1,$2,$3,$4,$5)`,
          [userId, payout, reason, matchId, rBal.rows[0].balance]
        );
      }
      // Save per-player result
      await client.query(
        `UPDATE match_players SET result=$3 WHERE match_id=$1 AND user_id=$2`,
        [matchId, userId, String(r.outcome || "pending")]
      );
    }

    await client.query(`UPDATE matches SET status='finished', ended_at=NOW() WHERE id=$1`, [matchId]);
    await client.query("COMMIT");

    // Push live balance updates
    for (const r of results) { if (r.userId) broadcastBalance(String(r.userId)); }

    return sendJson(res, 200, { ok:true });

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("matches/finish error", e);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  } finally { client.release(); }
}

// Attach route to router (extend handler)
const _origHandler = asyncHandler;
async function asyncHandler(req, res) {
  const { pathname } = url.parse(req.url, true);

  // monitor endpoints (no auth)
  if (pathname === "/monitor/ping") return sendJson(res, 200, { ok:true, ts: Date.now(), rss: process.memoryUsage().rss });

  if (req.method === "POST" && pathname === "/matches/finish") {
    return finishMatchHandler(req, res);
  }
  return _origHandler(req, res);
}

// ───────────────────────── Auto-cancel stale rooms (cron)
// Every 2 minutes: close rooms open > 30m and refund stake holds to joined players.
const CANCEL_AFTER_MIN = Number(process.env.CANCEL_AFTER_MIN || 30);
setInterval(async () => {
  try {
    const r = await q(
      `SELECT * FROM rooms
       WHERE status='open' AND created_at < NOW() - INTERVAL '${CANCEL_AFTER_MIN} minutes'`
    );
    for (const room of r.rows) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        // refund each joined player's stake (if any)
        const stake = n(room.stake, 0);
        if (stake > 0) {
          const rp = await client.query(`SELECT user_id FROM room_players WHERE room_id=$1`, [room.id]);
          for (const p of rp.rows) {
            await client.query(`UPDATE wallets SET balance=balance+$1, updated_at=NOW() WHERE user_id=$2`, [stake, p.user_id]);
            const rBal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [p.user_id]);
            await client.query(
              `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
               VALUES ($1,$2,'stake_release',NULL,$3)`,
              [p.user_id, stake, rBal.rows[0].balance]
            );
            broadcastBalance(p.user_id);
          }
        }
        await client.query(`UPDATE rooms SET status='closed' WHERE id=$1`, [room.id]);
        await client.query("COMMIT");
        console.log("⏱️ auto-closed room", room.id);
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("auto-cancel room error", room.id, e);
      } finally { client.release(); }
    }
  } catch (e) {
    console.error("auto-cancel scan error", e);
  }
}, 120_000);