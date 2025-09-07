// server.js — HTTP + WebSocket + Postgres (Render-friendly)

const http = require("http");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;

// =====================
// Postgres connection
// =====================
const pool = new Pool({
  connectionString: process.env.DB_URL,           // Set this in Render env
  ssl: { rejectUnauthorized: false }              // Required on Render PG
});

// Small helper to run a query with automatic client release on error
async function q(text, params = []) {
  return pool.query(text, params);
}

// =====================
// Initial DB setup (migrations-lite)
// =====================
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
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      name TEXT,
      max_players INT NOT NULL DEFAULT 4,
      status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','locked','in_play','closed')),
      stake_mode TEXT NOT NULL DEFAULT 'flexible' CHECK (stake_mode IN ('flexible','fixed')),
      min_stake NUMERIC(18,2) NOT NULL DEFAULT 0,
      max_stake NUMERIC(18,2) NOT NULL DEFAULT 1000000
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS matches (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      room_id UUID REFERENCES rooms(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'waiting' CHECK (status IN ('waiting','live','finished','cancelled')),
      stake_amount NUMERIC(18,2) NOT NULL,
      currency TEXT NOT NULL DEFAULT 'USDT',
      winner_user_id UUID NULL REFERENCES users(id)
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS match_players (
      match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
      user_id  UUID NOT NULL REFERENCES users(id)  ON DELETE CASCADE,
      joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      seat SMALLINT,
      result TEXT NOT NULL DEFAULT 'pending' CHECK (result IN ('pending','won','lost','draw','cancelled')),
      stake_amount NUMERIC(18,2) NOT NULL,
      PRIMARY KEY (match_id, user_id)
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS transactions (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      amount NUMERIC(18,2) NOT NULL,  -- credit = +, debit = -
      reason TEXT NOT NULL CHECK (reason IN ('stake_hold','stake_release','payout','refund','deposit','withdraw')),
      ref_match_id UUID REFERENCES matches(id),
      balance_after NUMERIC(18,2)
    );
  `);

  console.log("✅ DB ready (tables ensured)");
}

// =====================
// Helper functions (escrow-friendly, used later)
// =====================

// Return DB time (for quick health checks)
async function dbNow() {
  const r = await q("SELECT NOW() AS now");
  return r.rows[0].now;
}

// Ensure user & wallet exist; returns { user_id }
async function ensureUser(userId = null) {
  if (!userId) {
    const r = await q("INSERT INTO users DEFAULT VALUES RETURNING id");
    userId = r.rows[0].id;
  } else {
    await q(`INSERT INTO users(id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, [userId]);
  }
  await q(
    `INSERT INTO wallets(user_id, balance) VALUES ($1, 0)
     ON CONFLICT (user_id) DO NOTHING`,
    [userId]
  );
  return { user_id: userId };
}

async function getWallet(userId) {
  const r = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
  return r.rows[0] ? Number(r.rows[0].balance) : 0;
}

// Credit wallet (positive amount), record transaction
async function credit(userId, amount, reason = "deposit", refMatchId = null, client = null) {
  const c = client || await pool.connect();
  let release = false;
  if (!client) release = true;

  try {
    await c.query("BEGIN");
    const prev = await c.query(`SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`, [userId]);
    const oldBal = prev.rows[0] ? Number(prev.rows[0].balance) : 0;
    const newBal = oldBal + Number(amount);
    await c.query(`UPDATE wallets SET balance=$2 WHERE user_id=$1`, [userId, newBal]);
    await c.query(
      `INSERT INTO transactions(user_id, amount, reason, ref_match_id, balance_after)
       VALUES ($1,$2,$3,$4,$5)`,
      [userId, amount, reason, refMatchId, newBal]
    );
    await c.query("COMMIT");
    return newBal;
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    if (release) c.release();
  }
}

// Hold stake (debit), will be released/payout later
async function holdStake(userId, amount, refMatchId) {
  const c = await pool.connect();
  try {
    await c.query("BEGIN");
    const r = await c.query(`SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`, [userId]);
    const bal = r.rows[0] ? Number(r.rows[0].balance) : 0;
    if (bal < amount) throw new Error("insufficient_funds");
    const newBal = bal - Number(amount);
    await c.query(`UPDATE wallets SET balance=$2 WHERE user_id=$1`, [userId, newBal]);
    await c.query(
      `INSERT INTO transactions(user_id, amount, reason, ref_match_id, balance_after)
       VALUES ($1,$2,$3,$4,$5)`,
      [userId, -Number(amount), "stake_hold", refMatchId, newBal]
    );
    await c.query("COMMIT");
    return newBal;
  } catch (e) {
    await c.query("ROLLBACK");
    throw e;
  } finally {
    c.release();
  }
}

// Release a previous hold back to the player (refund)
async function releaseStake(userId, amount, refMatchId) {
  return credit(userId, Number(amount), "stake_release", refMatchId);
}

// Pay the winner (credit winnings). For now we just credit an amount you pass in.
async function payoutWinner(winnerUserId, amount, refMatchId) {
  return credit(winnerUserId, Number(amount), "payout", refMatchId);
}

// =====================
// HTTP server
// =====================
const server = http.createServer(async (req, res) => {
  try {
    if (req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, ts: Date.now() }));
      return;
    }

    if (req.url === "/db-ping") {
      const ts = await dbNow();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, dbTime: ts }));
      return;
    }

    if (req.url === "/db/tables") {
      const r = await q(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        ORDER BY table_name;
      `);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, tables: r.rows.map(x => x.table_name) }));
      return;
    }

    // Tiny in-browser WS tester
    if (req.url === "/test") {
      res.writeHead(200, { "content-type": "text/html" });
      res.end(`<!doctype html>
<html><body style="font:14px system-ui">
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
</script>
</body></html>`);
      return;
    }

    // Default 404
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "not_found" }));
  } catch (err) {
    console.error("HTTP handler error:", err);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "server_error" }));
  }
});

// =====================
// WebSocket — simple echo now (we'll extend later)
// =====================
const wss = new WebSocketServer({ noServer: true });
wss.on("connection", (ws) => {
  ws.on("message", (m) => ws.send(String(m)));
  ws.send("connected");
});

server.on("upgrade", (req, socket, head) => {
  if (req.url === "/ws") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  } else {
    socket.destroy();
  }
});

// =====================
// Boot
// =====================
initDb()
  .then(() => {
    server.listen(PORT, () => console.log(`✅ Server listening on :${PORT}`));
  })
  .catch((err) => {
    console.error("❌ Startup error:", err);
    process.exit(1);
  });