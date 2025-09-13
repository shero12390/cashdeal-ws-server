// server.js — HTTP + WebSocket + Postgres (Render-ready)

const http = require("http");
const url = require("url");
const { WebSocketServer } = require("ws");
const { Pool } = require("pg");

const PORT = process.env.PORT || 3000;

/* =========================
   Postgres
   ========================= */
const pool = new Pool({
  connectionString: process.env.DB_URL,
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
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      if (!data) return resolve({});
      try { resolve(JSON.parse(data)); }
      catch { reject(new Error("invalid_json")); }
    });
    req.on("error", reject);
  });
}

function n(v, f = 0) { const x = Number(v); return Number.isFinite(x) ? x : f; }

/* =========================
   Migrations (aligned to your DB)
   ========================= */
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

  /* Rooms with your flexible/fixed style baked into config fields already present */
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
      ended_at TIMESTAMPTZ
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
      PRIMARY KEY (match_id, user_id)
    );
  `);

  /* Your actual transactions schema (no side/meta) */
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
  console.log("✅ migrations done");
}

/* =========================
   Helpers
   ========================= */
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

/* =========================
   HTTP
   ========================= */
const server = http.createServer(async (req, res) => {
  try {
    const { pathname, query } = url.parse(req.url, true);

    // CORS preflight
    if (req.method === "OPTIONS") {
      res.writeHead(204, {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET,POST,OPTIONS",
        "access-control-allow-headers": "content-type",
      });
      return res.end();
    }

    if (pathname === "/health") return sendJson(res, 200, { ok: true });
    if (pathname === "/db/health") {
      const r = await q(`SELECT NOW() now`);
      return sendJson(res, 200, { ok: true, dbTime: r.rows[0].now });
    }

    
// ---------- WALLET: DEPOSIT (for testing) ----------
if (req.method === "POST" && pathname === "/wallet/deposit") {
  let body;
  try { body = await readJsonBody(req); } 
  catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }

  const userIdIn = String(body.userId || "").trim();
  const amt = Number(body.amount);
  if (!Number.isFinite(amt) || amt <= 0) {
    return sendJson(res, 400, { ok:false, error:"invalid_amount" });
  }

  const userId = await ensureUserWithWallet(userIdIn);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // credit wallet
    await client.query(
      `UPDATE wallets 
          SET balance = balance + $1, updated_at = NOW()
        WHERE user_id = $2`,
      [amt, userId]
    );

    // fetch new balance for balance_after column
    const rBal = await client.query(
      `SELECT balance FROM wallets WHERE user_id=$1`,
      [userId]
    );
    const newBal = Number(rBal.rows[0]?.balance ?? 0);

    // transactions table in your DB: id, created_at, user_id, amount, reason, ref_match_id, balance_after
    await client.query(
      `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
       VALUES ($1, $2, 'deposit', NULL, $3)`,
      [userId, amt, newBal]
    );

    await client.query("COMMIT");
    return sendJson(res, 200, { ok:true, userId, wallet: { balance: newBal, currency: 'USDT' } });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("deposit error", e);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  } finally {
    client.release();
  }
}


    /* ---------- ROOMS: CREATE ---------- */
    if (req.method === "POST" && pathname === "/rooms/create") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const userId = await ensureUserWithWallet(body.userId);
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
          // hold host stake
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

        // auto add host into room_players (ready=false by default)
        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready) VALUES ($1,$2,$3)
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

    /* ---------- ROOMS: JOIN ---------- */
    if (req.method === "POST" && pathname === "/rooms/join") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(body.userId);
      if (!roomId) return sendJson(res, 400, { ok:false, error:"missing_roomId" });

      const rRoom = await q(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
      if (rRoom.rowCount === 0) return sendJson(res, 404, { ok:false, error:"room_not_found" });
      const room = rRoom.rows[0];
      if (room.status !== "open") return sendJson(res, 400, { ok:false, error:"room_not_open" });
      if (room.current_players >= room.max_players) return sendJson(res, 400, { ok:false, error:"room_full" });

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        // if stake > 0 – hold stake
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

        // add to room_players
        await client.query(
          `INSERT INTO room_players (room_id, user_id, ready)
           VALUES ($1,$2,false) ON CONFLICT DO NOTHING`,
          [room.id, userId]
        );

        // increment player count atomically
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

    /* ---------- ROOMS: READY (mutual ready & start match) ---------- */
    if (req.method === "POST" && pathname === "/rooms/ready") {
      let body; try { body = await readJsonBody(req); } catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }
      const roomId = String(body.roomId || "");
      const userId = await ensureUserWithWallet(body.userId);
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
          // start match
          await client.query(`UPDATE rooms SET status='in_play' WHERE id=$1`, [room.id]);
          const rMatch = await client.query(
            `INSERT INTO matches (room_id, game, stake, status, started_at)
             VALUES ($1,$2,$3,'ready',NOW()) RETURNING id`,
            [room.id, room.game, room.stake]
          );
          startedMatchId = rMatch.rows[0].id;

          // attach match_players (stake_amount = room.stake)
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
        return sendJson(res, 200, { ok:true, room: { ...room, status: everyoneReady ? "in_play" : room.status }, players: rPlayers.rows, startedMatchId });
      } catch (e) {
        try { await client.query("ROLLBACK"); } catch {}
        console.error("rooms/ready error", e);
        return sendJson(res, 500, { ok:false, error:"server_error" });
      } finally { client.release(); }
    }

/* ---------- ROOMS: REJOIN (only within grace window) ---------- */
if (req.method === "POST" && pathname === "/rooms/rejoin") {
  let body;
  try { body = await readJsonBody(req); }
  catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }

  const roomId = String(body.roomId || "").trim();
  const userId = await ensureUserWithWallet(body.userId);

  if (!roomId || !userId) {
    return sendJson(res, 400, { ok:false, error:"missing_params" });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Lock room
    const rRoom = await client.query(`SELECT * FROM rooms WHERE id=$1 FOR UPDATE`, [roomId]);
    if (rRoom.rowCount === 0) {
      await client.query("ROLLBACK");
      return sendJson(res, 404, { ok:false, error:"room_not_found" });
    }
    const room = rRoom.rows[0];

    // Find latest active match for this room
    const rMatch = await client.query(
      `SELECT id, status FROM matches
        WHERE room_id=$1 AND status IN ('ready','running','live','in_play')
        ORDER BY created_at DESC
        LIMIT 1`,
      [room.id]
    );
    if (rMatch.rowCount === 0) {
      await client.query("ROLLBACK");
      return sendJson(res, 400, { ok:false, error:"no_active_match" });
    }
    const match = rMatch.rows[0];

    // Check this user has a seat and is still within grace
    const rMP = await client.query(
      `SELECT user_id, left_at, grace_until
         FROM match_players
        WHERE match_id=$1 AND user_id=$2
        FOR UPDATE`,
      [match.id, userId]
    );
    if (rMP.rowCount === 0) {
      await client.query("ROLLBACK");
      return sendJson(res, 404, { ok:false, error:"not_in_match" });
    }

    const mp = rMP.rows[0];
    // If no grace set or grace expired, reject
    if (!mp.grace_until) {
      await client.query("ROLLBACK");
      return sendJson(res, 400, { ok:false, error:"no_grace_set" });
    }
    const rNow = await client.query(`SELECT NOW() AS now`);
    const now = new Date(rNow.rows[0].now);
    const graceUntil = new Date(mp.grace_until);
    if (now > graceUntil) {
      await client.query("ROLLBACK");
      return sendJson(res, 400, { ok:false, error:"grace_expired" });
    }

    // Clear leave markers — player is back
    await client.query(
      `UPDATE match_players
          SET left_at = NULL,
              grace_until = NULL
        WHERE match_id=$1 AND user_id=$2`,
      [match.id, userId]
    );

    await client.query("COMMIT");
    return sendJson(res, 200, {
      ok: true,
      note: "rejoined_success",
      roomStatus: room.status,
      matchId: match.id
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("rooms/rejoin error", e);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  } finally {
    client.release();
  }
}

/* ---------- ROOMS: LEAVE (no refund; 90s grace if match is live) ---------- */
if (req.method === "POST" && pathname === "/rooms/leave") {
  let body;
  try { body = await readJsonBody(req); }
  catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }

  const roomId  = String(body.roomId || "").trim();
  const userId  = await ensureUserWithWallet(body.userId);
  const GRACE_S = 90; // 90 seconds grace to rejoin

  if (!roomId) return sendJson(res, 400, { ok:false, error:"missing_roomId" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const rRoom = await client.query(`SELECT * FROM rooms WHERE id=$1 FOR UPDATE`, [roomId]);
    if (rRoom.rowCount === 0) {
      await client.query("ROLLBACK");
      return sendJson(res, 404, { ok:false, error:"room_not_found" });
    }
    const room = rRoom.rows[0];

    const rMatch = await client.query(
      `SELECT id, status, stake FROM matches
        WHERE room_id=$1 AND status IN ('ready','running','live','in_play')
        ORDER BY created_at DESC LIMIT 1`,
      [room.id]
    );

    if (rMatch.rowCount > 0) {
      const match = rMatch.rows[0];

      const rMP = await client.query(
        `SELECT match_id, user_id FROM match_players
          WHERE match_id=$1 AND user_id=$2 FOR UPDATE`,
        [match.id, userId]
      );

      if (rMP.rowCount === 0) {
        await client.query("COMMIT");
        return sendJson(res, 200, { ok:true, note:"not_in_match", room, matchId: match.id });
      }

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

    // No active match yet
    await client.query(
      `DELETE FROM room_players WHERE room_id=$1 AND user_id=$2`,
      [room.id, userId]
    ).catch(() => null);

    await client.query(
      `UPDATE rooms
          SET current_players = GREATEST(current_players - 1, 0)
        WHERE id=$1`,
      [room.id]
    );

    await client.query("COMMIT");
    const rAfter = await q(`SELECT * FROM rooms WHERE id=$1`, [room.id]);
    return sendJson(res, 200, { ok: true, note: "left_lobby_no_refund", room: rAfter.rows[0] });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("rooms/leave error", e);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  } finally {
    client.release();
  }
}

/* ---------- ROOMS: STATUS ---------- */
if (req.method === "GET" && pathname === "/rooms/status") {
  const roomId = String(query.roomId || "").trim();
  if (!roomId) return sendJson(res, 400, { ok:false, error:"missing_roomId" });

  const rRoom = await q(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
  if (rRoom.rowCount === 0) return sendJson(res, 404, { ok:false, error:"room_not_found" });

  const rPlayers = await q(
    `SELECT user_id, ready, joined_at, left_at, grace_until
       FROM match_players WHERE match_id IN
         (SELECT id FROM matches WHERE room_id=$1 ORDER BY created_at DESC LIMIT 1)
       ORDER BY joined_at ASC`,
    [roomId]
  );
  return sendJson(res, 200, { ok:true, room: rRoom.rows[0], players: rPlayers.rows });
}

/* ---------- ROOMS: LIST ---------- */
if (req.method === "GET" && pathname === "/rooms/list") {
  const status = String(query.status || "open");
  const r = await q(
    `SELECT id, host_user_id, game, stake, max_players, current_players, status,
            created_at, mode, min_players, autostart, countdown_seconds,
            require_mutual_ready, config
       FROM rooms
      WHERE status=$1
      ORDER BY created_at DESC
      LIMIT 100`,
    [status]
  );
  return sendJson(res, 200, { ok:true, rooms: r.rows });
}

/* ---------- MATCHES: FINISH ---------- */
if (req.method === "POST" && pathname === "/matches/finish") {
  let body; try { body = await readJsonBody(req); }
  catch { return sendJson(res, 400, { ok:false, error:"invalid_json" }); }

  const matchId = String(body.matchId || "").trim();
  const results = Array.isArray(body.results) ? body.results : [];
  if (!matchId || results.length === 0)
    return sendJson(res, 400, { ok:false, error:"missing_params" });

  const rMatch = await q(`SELECT id, room_id, game, stake, status FROM matches WHERE id=$1`, [matchId]);
  if (rMatch.rowCount === 0) return sendJson(res, 404, { ok:false, error:"match_not_found" });
  const match = rMatch.rows[0];
  if (!["ready","running","live","in_play"].includes(match.status))
    return sendJson(res, 400, { ok:false, error:"invalid_state", status: match.status });

  const rPlayers = await q(`SELECT user_id FROM match_players WHERE match_id=$1`, [matchId]);
  const playerIds = new Set(rPlayers.rows.map(r => String(r.user_id)));

  const allowed = new Set(["won","lost","draw","cancelled"]);
  const incoming = new Map(
    results
      .map(r => [String(r.userId || "").trim(), String(r.result || "").trim()])
      .filter(([uid,res]) => uid && allowed.has(res))
  );
  if (incoming.size !== playerIds.size || [...playerIds].some(uid => !incoming.has(uid)))
    return sendJson(res, 400, { ok:false, error:"results_must_cover_all_players" });

  const winners = [...incoming.entries()].filter(([,res]) => res === "won").map(([uid]) => uid);
  const perPlayerStake = n(match.stake, 0);
  const prizePool = perPlayerStake * playerIds.size;
  const perWinner = winners.length > 0 ? prizePool / winners.length : 0;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const vals = [...incoming.entries()];
    const placeholders = vals.map((_,i)=>`($${i*2+1}::uuid,$${i*2+2}::text)`).join(",");
    await client.query(
      `UPDATE match_players mp
          SET result = v.res
         FROM (VALUES ${placeholders}) AS v(uid,res)
        WHERE mp.match_id = $${vals.length*2+1}::uuid
          AND mp.user_id = v.uid`,
      [...vals.flatMap(([uid,res])=>[uid,res]), matchId]
    );

    if (perWinner > 0 && winners.length > 0) {
      for (const uid of winners) {
        await client.query(`UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE user_id=$2`, [perWinner, uid]);
        const rBal = await client.query(`SELECT balance FROM wallets WHERE user_id=$1`, [uid]);
        await client.query(
          `INSERT INTO transactions (user_id, amount, reason, ref_match_id, balance_after)
           VALUES ($1,$2,'payout',$3,$4)`,
          [uid, perWinner, matchId, rBal.rows[0].balance]
        );
      }
    }

    await client.query(`UPDATE matches SET status='finished', ended_at=NOW() WHERE id=$1`, [matchId]);
    if (match.room_id) await client.query(`UPDATE rooms SET status='closed' WHERE id=$1`, [match.room_id]);

    await client.query("COMMIT");
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("matches/finish error", e);
    return sendJson(res, 500, { ok:false, error:"server_error" });
  } finally { client.release(); }

  const rDone = await q(`SELECT id, room_id, game, stake, status, started_at, ended_at FROM matches WHERE id=$1`, [matchId]);
  return sendJson(res, 200, { ok:true, match: rDone.rows[0], payouts: (perWinner>0 ? { winners, perWinner } : null) });
}