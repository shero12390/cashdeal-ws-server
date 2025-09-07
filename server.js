// server.js — HTTP + WebSocket + Postgres

const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

// ---------- Postgres ----------
const pool = new Pool({
  connectionString: process.env.DB_URL,      // set on Render
  ssl: { rejectUnauthorized: false }         // required on Render
});

async function initDb() {
  await pool.connect();
  console.log("✅ Connected to Postgres");

  // enable UUIDs and create minimal tables
  await pool.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS wallets (
      user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      balance NUMERIC(18,2) NOT NULL DEFAULT 0
    );
  `);

  console.log("✅ DB ready (tables ensured)");
}

// ---------- HTTP server ----------
const server = http.createServer(async (req, res) => {
  try {
    if (req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, ts: Date.now() }));
      return;
    }

    if (req.url === "/db-ping") {
      // quick DB check
      const r = await pool.query("SELECT NOW() AS now");
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, dbTime: r.rows[0].now }));
      return;
    }

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

    // default 404
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "not_found" }));
  } catch (err) {
    console.error("HTTP handler error:", err);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "server_error" }));
  }
});

// ---------- WebSocket (simple echo) ----------
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

// ---------- Start ----------
initDb()
  .then(() => {
    server.listen(PORT, () => {
      console.log(`✅ Server on :${PORT}`);
    });
  })
  .catch((err) => {
    console.error("❌ Startup error:", err);
    process.exit(1);
  });