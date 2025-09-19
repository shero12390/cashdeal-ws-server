// server.js — CashDeal HTTP + WebSocket + Postgres (Render-ready)

/* -------------------- Imports -------------------- */
const http = require("http");
const Url = require("url");           // don't shadow 'url' var later
const path = require("path");
const crypto = require("crypto");
const express = require("express");
const swaggerUi = require("swagger-ui-express");
const YAML = require("yamljs");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");
const jwt = require("jsonwebtoken");
const admin = require("firebase-admin"); // Firebase Admin

/* -------------------- Config / Env -------------------- */
const PORT = Number(process.env.PORT) || 3000;        // Render sets PORT
const DB_URL = process.env.DB_URL;
const AUTH_JWT_SECRET = process.env.AUTH_JWT_SECRET || "dev_secret_change_me";
const TOKEN_TTL_SECONDS = Number(process.env.TOKEN_TTL_SECONDS || 3600);
// NEW: short-lived guest token TTL (~15 minutes default)
const GUEST_TOKEN_TTL_SECONDS = Number(process.env.GUEST_TOKEN_TTL_SECONDS || 900);

const SYNC_SECRET = process.env.SYNC_SECRET || "superStrongSecret123";   // legacy
const SYNC_HMAC_KEY = process.env.SYNC_HMAC_KEY || "hmac_key_change_me"; // preferred

/* -------------------- Firebase Admin init -------------------- */
// Prefer GOOGLE_APPLICATION_CREDENTIALS on Render, else base64 JSON in FIREBASE_SERVICE_ACCOUNT
if (!admin.apps.length) {
  try {
    if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
      admin.initializeApp(); // uses ADC
    } else if (process.env.FIREBASE_SERVICE_ACCOUNT) {
      const svcJson = JSON.parse(Buffer.from(process.env.FIREBASE_SERVICE_ACCOUNT, "base64").toString("utf8"));
      admin.initializeApp({ credential: admin.credential.cert(svcJson) });
    } else {
      admin.initializeApp(); // last resort (will fail verify if no creds)
    }
    console.log("Firebase Admin initialized");
  } catch (e) {
    console.warn("Firebase Admin init warning:", e.message);
  }
}

/* -------------------- DB Pool -------------------- */
const pool = new Pool({
  connectionString: DB_URL,
  max: 10,
  idleTimeoutMillis: 30_000,
  // REQUIRED on Render’s managed Postgres (external URL requires SSL)
  ssl: { rejectUnauthorized: false }
});
pool.on("error", (err) => console.error("PG pool error:", err));

async function q(text, params = []) {
  const c = await pool.connect();
  try { return await c.query(text, params); }
  finally { c.release(); }
}

/* -------------------- Boot: lightweight migrations -------------------- */
async function migrate() {
  await q(`CREATE TABLE IF NOT EXISTS wallets(
    user_id uuid primary key,
    balance numeric not null default 0,
    currency text not null default 'USDT',
    updated_at timestamptz not null default now()
  )`);

  await q(`CREATE TABLE IF NOT EXISTS rooms(
    id uuid primary key,
    game text not null,
    stake numeric not null,
    max_players int not null default 2,
    status text not null default 'waiting',
    created_at timestamptz not null default now(),
    expires_at timestamptz
  )`);

  await q(`CREATE TABLE IF NOT EXISTS room_players(
    room_id uuid references rooms(id) on delete cascade,
    user_id uuid not null,
    side text,
    primary key (room_id, user_id)
  )`);

  await q(`CREATE TABLE IF NOT EXISTS idempotency_keys(
    key text primary key,
    route text not null,
    created_at timestamptz not null default now(),
    response jsonb
  )`);
}

/* -------------------- Express App + Swagger -------------------- */
const app = express();
app.use(express.json({ limit: "512kb" }));

// Minimal CORS for tests (tighten in prod)
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key, X-Signature, X-Timestamp");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

// Load OpenAPI docs at /docs (openapi.yaml in repo root)
try {
  const swaggerDoc = YAML.load(path.join(__dirname, "openapi.yaml"));
  app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerDoc));
} catch (e) {
  console.warn("Swagger not mounted:", e.message);
}

/* -------------------- Rate limiting (simple in-memory) -------------------- */
const rlMap = new Map(); // ip -> { count, ts }
function rateLimit(limit, windowMs) {
  return (req, res, next) => {
    const ip = req.headers["x-forwarded-for"]?.split(",")[0]?.trim() || req.socket.remoteAddress || "unknown";
    const now = Date.now();
    const rec = rlMap.get(ip) || { count: 0, ts: now };
    if (now - rec.ts > windowMs) { rec.count = 0; rec.ts = now; }
    rec.count += 1; rlMap.set(ip, rec);
    if (rec.count > limit) return res.status(429).json({ ok: false, error: "rate_limited" });
    next();
  };
}

/* -------------------- Auth helpers (JWT) -------------------- */
function mintJwt(payload, ttlSec = TOKEN_TTL_SECONDS) {
  return jwt.sign(payload, AUTH_JWT_SECRET, { expiresIn: ttlSec });
}
function verifyJwtFromReq(req) {
  const hdr = req.headers.authorization || "";
  const tok = hdr.startsWith("Bearer ") ? hdr.slice(7) : null;
  if (!tok) throw new Error("no_token");
  return jwt.verify(tok, AUTH_JWT_SECRET);
}
function requireAuth(req, res, next) {
  try {
    req.user = verifyJwtFromReq(req);
    next();
  } catch (e) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }
}
// NEW: allow either normal server JWT (no scope) or guest JWT with scope=match
function requireMatchAuth(req, res, next) {
  try {
    const user = verifyJwtFromReq(req);
    // Accept tokens minted by /auth/firebase (no scope) OR guest with scope=match
    if (user.scope && user.scope !== "match") {
      return res.status(403).json({ ok: false, error: "forbidden_scope" });
    }
    req.user = user;
    next();
  } catch (e) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }
}

/* -------------------- /auth/firebase (exchange Firebase ID token -> server JWT) -------------------- */
app.post("/auth/firebase", rateLimit(30, 60_000), async (req, res) => {
  try {
    const { idToken, uid, uid8 } = req.body || {};
    if (!idToken) return res.status(400).json({ ok: false, error: "missing_idToken" });

    let decoded;
    try {
      decoded = await admin.auth().verifyIdToken(idToken);
    } catch (e) {
      return res.status(401).json({ ok: false, error: "invalid_firebase_token" });
    }

    if (uid && String(uid) !== String(decoded.uid)) {
      return res.status(401).json({ ok: false, error: "uid_mismatch" });
    }

    const token = mintJwt({ uid: decoded.uid, uid8: uid8 || undefined });
    return res.json({ ok: true, token, ttl: TOKEN_TTL_SECONDS });
  } catch (e) {
    console.error("/auth/firebase error:", e);
    return res.status(500).json({ ok: false, error: "server_error" });
  }
});

/* -------------------- NEW: /auth/guest (short-lived, matchmaking-only JWT) -------------------- */
app.post("/auth/guest", rateLimit(30, 60_000), async (req, res) => {
  try {
    const { uid = "", uid8 = "", device = "", appVer = "" } = req.body || {};
    // Minimal sanity: must provide at least uid8 (your client can compute 8-digit ID)
    if (!uid8 || String(uid8).trim().length === 0) {
      return res.status(400).json({ ok: false, error: "missing_uid8" });
    }
    // Mint guest token restricted to matchmaking scope
    const payload = {
      uid: uid || undefined,
      uid8: String(uid8),
      scope: "match",
      dev: device ? String(device).slice(0, 64) : undefined,
      ver: appVer ? String(appVer).slice(0, 32) : undefined
    };
    const token = mintJwt(payload, GUEST_TOKEN_TTL_SECONDS);
    return res.json({ ok: true, token, ttl: GUEST_TOKEN_TTL_SECONDS, expMs: GUEST_TOKEN_TTL_SECONDS * 1000 });
  } catch (e) {
    console.error("/auth/guest error:", e);
    return res.status(500).json({ ok: false, error: "server_error" });
  }
});

/* -------------------- HMAC helpers for /internal/wallet/sync -------------------- */
function safeTimingEqual(a, b) {
  const ab = Buffer.from(a); const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}
function verifyHmac(req, rawBody) {
  const sig = req.headers["x-signature"];
  const ts = req.headers["x-timestamp"];
  if (sig && ts) {
    const expect = crypto.createHmac("sha256", SYNC_HMAC_KEY)
      .update(String(ts)).update(".").update(rawBody)
      .digest("hex");
    return safeTimingEqual(sig, `sha256=${expect}`);
  }
  const legacy = req.headers["x-sync-secret"];
  return legacy && legacy === SYNC_SECRET;
}

/* -------------------- WS broadcast hook (wired later) -------------------- */
let wsBroadcastBalance = () => {}; // set after WS init

/* -------------------- Utility: unwrap number -------------------- */
function asNumber(n, def = 0) {
  const v = Number(n);
  return Number.isFinite(v) ? v : def;
}

/* -------------------- Health & Dev Tokens -------------------- */
app.get("/healthz", (req, res) => res.json({ ok: true, ts: Date.now() }));

app.post("/auth/dev-mint", rateLimit(20, 60_000), (req, res) => {
  const { userId, name } = req.body || {};
  if (!userId) return res.status(400).json({ ok: false, error: "missing_userId" });
  const token = mintJwt({ uid: userId, name: name || "Guest" });
  res.json({ ok: true, token, ttl: TOKEN_TTL_SECONDS });
});
/* -------------------- Wallet APIs -------------------- */
app.get("/wallet/me", requireAuth, async (req, res) => {
  try {
    const uid = req.user.uid;
    const r = await q(`SELECT balance, currency FROM wallets WHERE user_id=$1`, [uid]);
    if (r.rowCount === 0) return res.json({ ok: true, balance: 0, currency: "USDT" });
    res.json({ ok: true, balance: Number(r.rows[0].balance), currency: r.rows[0].currency || "USDT" });
  } catch (e) {
    console.error(e); res.status(500).json({ ok: false, error: "db_error" });
  }
});

app.post("/internal/wallet/sync",
  express.raw({ type: "*/*" }),
  async (req, res) => {
    const raw = Buffer.isBuffer(req.body)
      ? req.body.toString("utf8")
      : JSON.stringify(req.body || {});
    if (!verifyHmac(req, raw)) return res.status(401).json({ ok: false, error: "unauthorized" });

    let body; try { body = JSON.parse(raw); } catch { body = {}; }
    const uid = body.uid; const bal = asNumber(body.balance, undefined);
    if (!uid || bal == null) return res.status(400).json({ ok: false, error: "bad_payload" });

    try {
      await q(`
        INSERT INTO wallets(user_id, balance, currency, updated_at)
        VALUES($1,$2,'USDT',now())
        ON CONFLICT (user_id) DO UPDATE SET balance=EXCLUDED.balance, updated_at=now()`,
        [uid, bal]);
      try { wsBroadcastBalance(uid, bal); } catch {}
      res.json({ ok: true });
    } catch (e) {
      console.error(e); res.status(500).json({ ok: false, error: "db_error" });
    }
  }
);

/* -------------------- Rooms & Matchmaking -------------------- */
app.post("/rooms/create", requireMatchAuth, rateLimit(30, 60_000), async (req, res) => {
  try {
    const { game, stake, maxPlayers = 2, ttlSeconds = 120 } = req.body || {};
    if (!game) return res.status(400).json({ ok: false, error: "missing_game" });

    const id = crypto.randomUUID();
    const exp = new Date(Date.now() + Math.min(600, Math.max(30, Number(ttlSeconds || 120))) * 1000);

    await q(`INSERT INTO rooms(id, game, stake, max_players, status, expires_at)
             VALUES($1,$2,$3,$4,'waiting',$5)`,
      [id, String(game), asNumber(stake, 0), Number(maxPlayers) || 2, exp]);

    await q(`INSERT INTO room_players(room_id, user_id) VALUES($1,$2)`, [id, req.user.uid]);

    res.json({ ok: true, roomId: id, status: "waiting", expiresAt: exp.toISOString() });
  } catch (e) {
    console.error(e); res.status(500).json({ ok: false, error: "db_error" });
  }
});

app.post("/rooms/join", requireMatchAuth, rateLimit(60, 60_000), async (req, res) => {
  try {
    const { roomId } = req.body || {};
    if (!roomId) return res.status(400).json({ ok: false, error: "missing_roomId" });

    const r = await q(`SELECT max_players, status FROM rooms WHERE id=$1`, [roomId]);
    if (r.rowCount === 0) return res.status(404).json({ ok: false, error: "room_not_found" });
    if (r.rows[0].status !== "waiting") return res.status(400).json({ ok: false, error: "room_closed" });

    await q(`INSERT INTO room_players(room_id, user_id)
             VALUES($1,$2) ON CONFLICT DO NOTHING`, [roomId, req.user.uid]);

    const count = await q(`SELECT count(*)::int AS n FROM room_players WHERE room_id=$1`, [roomId]);
    const nowCount = count.rows[0].n;

    if (nowCount >= r.rows[0].max_players) {
      await q(`UPDATE rooms SET status='ready' WHERE id=$1`, [roomId]);
      return res.json({ ok: true, roomId, status: "ready" });
    }
    res.json({ ok: true, roomId, status: "waiting" });
  } catch (e) {
    console.error(e); res.status(500).json({ ok: false, error: "db_error" });
  }
});

app.post("/rooms/ready", requireMatchAuth, async (req, res) => {
  try {
    const { roomId } = req.body || {};
    if (!roomId) return res.status(400).json({ ok: false, error: "missing_roomId" });
    await q(`UPDATE rooms SET status='ready' WHERE id=$1 AND status='waiting'`, [roomId]);
    res.json({ ok: true });
  } catch (e) {
    console.error(e); res.status(500).json({ ok: false, error: "db_error" });
  }
});

/* -------------------- Matches: finish (idempotent) -------------------- */
async function getOrStoreIdempotent(key, route, responseJson) {
  const found = await q(`SELECT response FROM idempotency_keys WHERE key=$1`, [key]);
  if (found.rowCount) return found.rows[0].response;
  await q(`INSERT INTO idempotency_keys(key, route, response) VALUES($1,$2,$3)`,
    [key, route, responseJson]);
  return responseJson;
}

app.post("/matches/finish", requireMatchAuth, rateLimit(60, 60_000), async (req, res) => {
  const idem = String(req.headers["idempotency-key"] || "");
  if (!idem) return res.status(400).json({ ok: false, error: "missing_idempotency_key" });
  const payload = req.body || {};
  const route = "matches.finish";

  try {
    const prev = await q(`SELECT response FROM idempotency_keys WHERE key=$1`, [idem]);
    if (prev.rowCount) return res.json(prev.rows[0].response);

    const { roomId, results } = payload; // [{ userId, result }]
    if (!roomId || !Array.isArray(results)) {
      return res.status(400).json({ ok: false, error: "bad_payload" });
    }

    const r = await q(`UPDATE rooms SET status='finished' WHERE id=$1 AND status!='finished' RETURNING id`, [roomId]);
    if (r.rowCount === 0) {
      const resp = { ok: true, status: "already_finished" };
      const stored = await getOrStoreIdempotent(idem, route, resp);
      return res.json(stored);
    }

    const roomRow = await q(`SELECT stake FROM rooms WHERE id=$1`, [roomId]);
    const stake = asNumber(roomRow.rows[0]?.stake || 0);

    for (const it of results) {
      const uid = it.userId;
      if (!uid) continue;
      let delta = 0;
      if (it.result === "won") delta = stake;
      else if (it.result === "lost") delta = -stake;

      if (delta !== 0) {
        await q(`INSERT INTO wallets(user_id, balance, currency, updated_at)
                 VALUES($1,$2,'USDT',now())
                 ON CONFLICT (user_id) DO UPDATE
                 SET balance = wallets.balance + EXCLUDED.balance,
                     updated_at = now()`,
          [uid, delta]);

        try { wsBroadcastBalance(uid); } catch {}
      }
    }

    const resp = { ok: true, status: "finished" };
    const stored = await getOrStoreIdempotent(idem, route, resp);
    res.json(stored);
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: "db_error" });
  }
});

/* -------------------- Auto-cancel stale rooms (cron) -------------------- */
const CANCEL_SWEEP_MS = 30_000;
setInterval(async () => {
  try {
    await q(`UPDATE rooms SET status='cancelled'
             WHERE status='waiting' AND expires_at IS NOT NULL AND now() > expires_at`);
  } catch (e) { /* ignore */ }
}, CANCEL_SWEEP_MS);

/* -------------------- WebSocket (attached to Express server) -------------------- */
const server = http.createServer(app);
const wss = new WebSocketServer({ server, perMessageDeflate: false });

// userId -> Set<ws>
const subsByUser = new Map();

function wsSend(ws, obj) {
  try { ws.send(JSON.stringify(obj)); } catch {}
}
async function fetchBalance(uid) {
  const r = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [uid]);
  return Number(r.rows[0]?.balance ?? 0);
}
wsBroadcastBalance = async (userId, forcedBalance) => {
  const subs = subsByUser.get(String(userId));
  if (!subs || subs.size === 0) return;
  const bal = forcedBalance != null ? forcedBalance : await fetchBalance(userId);
  for (const ws of subs) if (ws.readyState === ws.OPEN)
    wsSend(ws, { type: "balance.update", userId: String(userId), balance: bal });
};

wss.on("connection", (ws) => {
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  ws.on("message", async (buf) => {
    let msg = null; try { msg = JSON.parse(String(buf)); } catch { return; }

    if (msg?.type === "ping") {
      return wsSend(ws, { type: "pong", ts: msg.ts || Date.now() });
    }

    if (msg?.type === "subscribe.balance" && msg.userId) {
      const uid = String(msg.userId);
      if (!subsByUser.has(uid)) subsByUser.set(uid, new Set());
      subsByUser.get(uid).add(ws);
      ws.on("close", () => {
        const set = subsByUser.get(uid);
        if (set) { set.delete(ws); if (set.size === 0) subsByUser.delete(uid); }
      });
      wsSend(ws, { type: "balance.subscribed", userId: uid });
      try {
        const bal = await fetchBalance(uid);
        wsSend(ws, { type: "balance.update", userId: uid, balance: bal });
      } catch {}
    }
  });
});

// WS heartbeat
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { try { ws.terminate(); } catch {} continue; }
    ws.isAlive = false; try { ws.ping(); } catch {}
  }
}, 30_000);

/* -------------------- Basic HTTP fallback (optional) -------------------- */
app.get("/", (req, res) => {
  res.type("text/plain").send("CashDeal WS/HTTP server OK. See /docs for API.");
});

/* -------------------- Error handler -------------------- */
app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);
  res.status(500).json({ ok: false, error: "server_error" });
});

/* -------------------- Start Server -------------------- */
(async () => {
  try {
    await migrate();
    server.listen(PORT, () => {
      console.log(`CashDeal server listening on :${PORT}`);
    });
  } catch (e) {
    console.error("Failed to start:", e);
    process.exit(1);
  }
})();