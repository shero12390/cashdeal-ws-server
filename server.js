// server.js — Cashdeal minimal API (HTTP only) + Postgres
// Tables: users, wallets, rooms, matches, match_players, transactions

const http = require("http");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

// --- Postgres ---------------------------------------------------------------
const pool = new Pool({
  connectionString: process.env.DB_URL, // set in Render Env
  ssl: { rejectUnauthorized: false },   // required on Render
});

async function initDb() {
  await pool.connect();

  // extensions
  await pool.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

  // users
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // wallets
  await pool.query(`
    CREATE TABLE IF NOT EXISTS wallets (
      user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      balance NUMERIC(18,2) NOT NULL DEFAULT 0
    );
  `);

  // transactions (deposit/withdraw/escrow/payout)
  await pool.query(`
    CREATE TYPE IF NOT EXISTS tx_type AS ENUM ('deposit','withdraw','escrow','payout');
  `).catch(() => {}); // enum may already exist

  await pool.query(`
    CREATE TABLE IF NOT EXISTS transactions (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      user_id UUID REFERENCES users(id) ON DELETE SET NULL,
      room_id UUID,
      match_id UUID,
      type tx_type NOT NULL,
      amount NUMERIC(18,2) NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // rooms (a lobby where players gather & choose stake)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS rooms (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      game TEXT NOT NULL,                 -- e.g., 'ludo', 'solitaire'
      stake NUMERIC(18,2) NOT NULL,       -- per-player stake
      max_players INT NOT NULL CHECK (max_players BETWEEN 2 AND 8),
      status TEXT NOT NULL DEFAULT 'open',-- 'open' | 'playing' | 'closed'
      host_id UUID REFERENCES users(id),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // matches (one played game that happens from a room)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS matches (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      room_id UUID NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
      game TEXT NOT NULL,
      stake NUMERIC(18,2) NOT NULL,
      status TEXT NOT NULL DEFAULT 'created', -- 'created' | 'finished' | 'cancelled'
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      finished_at TIMESTAMPTZ
    );
  `);

  // match_players (who played a match; winners flagged later)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS match_players (
      match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      is_winner BOOLEAN NOT NULL DEFAULT FALSE,
      PRIMARY KEY (match_id, user_id)
    );
  `);

  // helpful indexes
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_tx_user ON transactions(user_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_room_status ON rooms(status);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_match_room ON matches(room_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_wallet_balance ON wallets(balance);`);

  console.log("✅ DB ready");
}

// --- helpers ---------------------------------------------------------------
function readBody(req) {
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

async function ensureUser(userId) {
  // if userId provided, upsert; if not, create a new one
  if (userId) {
    await pool.query(
      `INSERT INTO users(id) VALUES($1) ON CONFLICT (id) DO NOTHING;`,
      [userId]
    );
  } else {
    const r = await pool.query(`INSERT INTO users DEFAULT VALUES RETURNING id;`);
    userId = r.rows[0].id;
  }
  await pool.query(
    `INSERT INTO wallets(user_id, balance)
     VALUES($1, 0)
     ON CONFLICT (user_id) DO NOTHING;`,
    [userId]
  );
  return userId;
}

async function getBalance(userId) {
  const r = await pool.query(`SELECT balance FROM wallets WHERE user_id=$1;`, [userId]);
  return r.rows[0] ? Number(r.rows[0].balance) : 0;
}

async function addTx(userId, type, amount, roomId = null, matchId = null) {
  await pool.query(
    `INSERT INTO transactions (user_id, type, amount, room_id, match_id)
     VALUES ($1,$2,$3,$4,$5);`,
    [userId, type, amount, roomId, matchId]
  );
}

// atomic wallet change
async function changeBalance(client, userId, delta) {
  const r = await client.query(
    `UPDATE wallets SET balance = balance + $2 WHERE user_id=$1 RETURNING balance;`,
    [userId, delta]
  );
  if (r.rowCount === 0) throw new Error("wallet_not_found");
  const bal = Number(r.rows[0].balance);
  if (bal < 0) throw new Error("insufficient_funds");
  return bal;
}

// --- HTTP server -----------------------------------------------------------
const server = http.createServer(async (req, res) => {
  try {
    // CORS (simple)
    res.setHeader("access-control-allow-origin", "*");
    res.setHeader("access-control-allow-headers", "content-type");
    if (req.method === "OPTIONS") { res.writeHead(200); res.end(); return; }

    // HEALTH
    if (req.method === "GET" && req.url === "/health") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, ts: Date.now() }));
      return;
    }

    if (req.method === "GET" && req.url === "/db/health") {
      const r = await pool.query("SELECT NOW() AS now");
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, dbTime: r.rows[0].now }));
      return;
    }

    if (req.method === "GET" && req.url === "/db/tables") {
      const r = await pool.query(`
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' ORDER BY table_name;`);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, tables: r.rows.map(x => x.table_name) }));
      return;
    }

    // USERS: upsert or create new
    if (req.method === "POST" && req.url === "/users/upsert") {
      const { userId } = await readBody(req);
      const id = await ensureUser(userId);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, userId: id }));
      return;
    }

    // WALLET: get balance
    if (req.method === "GET" && req.url.startsWith("/wallet/")) {
      const userId = req.url.split("/")[2];
      const bal = await getBalance(userId);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, userId, balance: bal }));
      return;
    }

    // WALLET: deposit / withdraw (test-only)
    if (req.method === "POST" && (req.url === "/wallet/deposit" || req.url === "/wallet/withdraw")) {
      const { userId, amount } = await readBody(req);
      if (!userId || typeof amount !== "number" || amount <= 0) throw new Error("bad_input");

      await ensureUser(userId);
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const delta = req.url.endsWith("deposit") ? amount : -amount;
        const bal = await changeBalance(client, userId, delta);
        await addTx(userId, req.url.endsWith("deposit") ? "deposit" : "withdraw", amount);
        await client.query("COMMIT");
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true, balance: bal }));
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        client.release();
      }
      return;
    }

    // ROOMS: create (host chooses stake/maxPlayers)
    if (req.method === "POST" && req.url === "/rooms/create") {
      const { userId, game, stake, maxPlayers } = await readBody(req);
      if (!game || !stake || !maxPlayers) throw new Error("bad_input");
      const host = await ensureUser(userId);

      const r = await pool.query(
        `INSERT INTO rooms (game, stake, max_players, host_id)
         VALUES ($1,$2,$3,$4) RETURNING *;`,
        [game, stake, maxPlayers, host]
      );
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, room: r.rows[0] }));
      return;
    }

    // ROOMS: join (no escrow yet, escrow happens at match start)
    if (req.method === "POST" && req.url === "/rooms/join") {
      const { userId, roomId } = await readBody(req);
      await ensureUser(userId);
      const room = await pool.query(`SELECT * FROM rooms WHERE id=$1 AND status='open'`, [roomId]);
      if (room.rowCount === 0) throw new Error("room_unavailable");

      // count how many players currently in room by looking at open matches? we keep it simple:
      // We don't track lobby players in a table for now; app can just start match when ready.

      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, roomId }));
      return;
    }

    // MATCHES: start (escrow stake from the provided players)
    if (req.method === "POST" && req.url === "/matches/start") {
      const { roomId, players } = await readBody(req); // players = [userId,...]
      if (!roomId || !Array.isArray(players) || players.length < 2) throw new Error("bad_input");

      const rRoom = await pool.query(`SELECT * FROM rooms WHERE id=$1 AND status='open'`, [roomId]);
      if (rRoom.rowCount === 0) throw new Error("room_unavailable");
      const room = rRoom.rows[0];

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        // create match
        const rMatch = await client.query(
          `INSERT INTO matches (room_id, game, stake, status)
           VALUES ($1,$2,$3,'created') RETURNING *;`,
          [roomId, room.game, room.stake]
        );
        const match = rMatch.rows[0];

        // escrow stake from each player
        for (const uid of players) {
          await ensureUser(uid);
          await changeBalance(client, uid, -Number(room.stake));
          await addTx(uid, "escrow", Number(room.stake), roomId, match.id);
          await client.query(
            `INSERT INTO match_players (match_id, user_id) VALUES ($1,$2)
             ON CONFLICT DO NOTHING;`,
            [match.id, uid]
          );
        }

        // lock room
        await client.query(`UPDATE rooms SET status='playing' WHERE id=$1;`, [roomId]);
        await client.query("COMMIT");

        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true, match }));
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        client.release();
      }
      return;
    }

    // MATCHES: finish (pay winners; if multiple, split pot equally)
    if (req.method === "POST" && req.url === "/matches/finish") {
      const { matchId, winners } = await readBody(req); // winners = [userId,...]
      if (!matchId || !Array.isArray(winners) || winners.length < 1) throw new Error("bad_input");

      const rMatch = await pool.query(`SELECT * FROM matches WHERE id=$1 AND status='created'`, [matchId]);
      if (rMatch.rowCount === 0) throw new Error("match_not_found_or_done");
      const match = rMatch.rows[0];

      const rPlayers = await pool.query(`SELECT COUNT(*)::int AS c FROM match_players WHERE match_id=$1;`, [matchId]);
      const playerCount = rPlayers.rows[0].c;
      const pot = Number(match.stake) * playerCount;
      const share = pot / winners.length;

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        for (const uid of winners) {
          await changeBalance(client, uid, share);
          await addTx(uid, "payout", share, match.room_id, match.id);
          await client.query(
            `UPDATE match_players SET is_winner=TRUE WHERE match_id=$1 AND user_id=$2;`,
            [matchId, uid]
          );
        }

        await client.query(
          `UPDATE matches SET status='finished', finished_at=NOW() WHERE id=$1;`,
          [matchId]
        );
        await client.query(`UPDATE rooms SET status='open' WHERE id=$1;`, [match.room_id]);

        await client.query("COMMIT");
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: true, pot, share }));
      } catch (e) {
        await client.query("ROLLBACK");
        throw e;
      } finally {
        client.release();
      }
      return;
    }

    // default 404
    res.writeHead(404, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "not_found" }));
  } catch (err) {
    console.error("[HTTP ERROR]", err);
    res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: String(err.message || err) }));
  }
});

// --- boot ------------------------------------------------------------------
initDb()
  .then(() => server.listen(PORT, () => console.log(`✅ API on :${PORT}`)))
  .catch((e) => { console.error("❌ Startup", e); process.exit(1); });