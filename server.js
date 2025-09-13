// server.js — HTTP + WebSocket + Postgres (Render-ready, with new room fields)

const http = require('http');
const url = require('url');
const { WebSocketServer } = require('ws');
const { Pool } = require('pg');

const PORT = process.env.PORT || 3000;

/* =========================
   Postgres
   ========================= */
const pool = new Pool({
  connectionString: process.env.DB_URL,
  ssl: { rejectUnauthorized: false },
});
const q = (sql, params=[]) => pool.query(sql, params);

function sendJson(res, code, obj) {
  res.writeHead(code, {
    'content-type': 'application/json',
    'cache-control': 'no-store',
  });
  res.end(JSON.stringify(obj));
}
function toNumber(n, fallback=0){ const v = Number(n); return Number.isFinite(v) ? v : fallback; }
function isInt(n){ return typeof n === 'number' && Number.isInteger(n); }

async function readJsonBody(req){
  return new Promise((resolve, reject)=>{
    let d=''; req.on('data', c=> d+=c);
    req.on('end', ()=>{ if(!d) return resolve({}); try{ resolve(JSON.parse(d)); }catch(e){ reject({kind:'invalid_json'});} });
    req.on('error', reject);
  });
}

/* =========================
   Migrations (idempotent)
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

  // Base rooms table (aligns with your live schema; then add new columns below)
  await q(`
    CREATE TABLE IF NOT EXISTS rooms (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      -- in your live DB 'host_user_id' exists; ensure it's there
      host_user_id UUID,
      -- keep compatibility with existing columns
      max_players INT NOT NULL DEFAULT 4,
      status TEXT NOT NULL DEFAULT 'open'
    );
  `);

  // Add/ensure the new flexible columns you created
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS game                TEXT;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS mode                TEXT NOT NULL DEFAULT 'h2h';`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS min_players         INT  NOT NULL DEFAULT 2;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS autostart           BOOLEAN NOT NULL DEFAULT FALSE;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS countdown_seconds   INT  NOT NULL DEFAULT 0;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS require_mutual_ready BOOLEAN NOT NULL DEFAULT FALSE;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS config              JSONB NOT NULL DEFAULT '{}'::jsonb;`);
  await q(`ALTER TABLE rooms ADD COLUMN IF NOT EXISTS current_players     INT  NOT NULL DEFAULT 1;`);

  await q(`CREATE INDEX IF NOT EXISTS idx_rooms_status ON rooms(status);`);
  await q(`CREATE INDEX IF NOT EXISTS idx_room_status ON rooms(status);`);

  // Matches use stake_amount (your live table already has these)
  await q(`
    CREATE TABLE IF NOT EXISTS matches (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      room_id UUID REFERENCES rooms(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'waiting',
      stake_amount NUMERIC(18,2) NOT NULL DEFAULT 0,
      currency TEXT NOT NULL DEFAULT 'USDT',
      winner_user_id UUID,
      game TEXT,
      started_at TIMESTAMPTZ,
      ended_at TIMESTAMPTZ
    );
  `);
  await q(`CREATE INDEX IF NOT EXISTS idx_match_room ON matches(room_id);`);

  await q(`
    CREATE TABLE IF NOT EXISTS match_players (
      match_id UUID NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
      user_id  UUID NOT NULL REFERENCES users(id)  ON DELETE CASCADE,
      joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (match_id, user_id)
    );
  `);

  await q(`
    CREATE TABLE IF NOT EXISTS transactions (
      id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
      user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      amount NUMERIC(18,2) NOT NULL,
      side TEXT NOT NULL,           -- debit | credit
      reason TEXT NOT NULL,         -- deposit | withdraw | room_create | room_join | payout | refund | etc.
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  console.log('✅ Migrations complete');
}

/* =========================
   Helpers
   ========================= */
async function ensureUserWithWallet(userIdMaybe){
  let userId = String(userIdMaybe || '').trim();
  if (userId) {
    const r = await q(`SELECT id FROM users WHERE id=$1::uuid`, [userId]).catch(()=>null);
    if (!r || r.rowCount === 0) {
      const ins = await q(`INSERT INTO users(id) VALUES ($1) ON CONFLICT DO NOTHING RETURNING id`, [userId]);
      if (ins.rowCount === 0) { const n = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`); userId = n.rows[0].id; }
    }
  } else {
    const n = await q(`INSERT INTO users DEFAULT VALUES RETURNING id`); userId = n.rows[0].id;
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

    // CORS
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'access-control-allow-origin': '*',
        'access-control-allow-methods': 'GET,POST,OPTIONS',
        'access-control-allow-headers': 'content-type',
      }); return res.end();
    }
    res.setHeader('access-control-allow-origin', '*');

    if (pathname === '/health') return sendJson(res, 200, { ok: true, ts: Date.now() });
    if (pathname === '/db/health') {
      const r = await q(`SELECT NOW() AS now`); return sendJson(res, 200, { ok: true, dbTime: r.rows[0].now });
    }
    if (pathname === '/tables') {
      const r = await q(`SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename`);
      return sendJson(res, 200, { ok: true, tables: r.rows.map(x=>x.tablename) });
    }

    /* -------- Wallet: deposit (testing) -------- */
    if (req.method === 'POST' && pathname === '/wallet/deposit') {
      let b; try { b = await readJsonBody(req) } catch { return sendJson(res, 400, { ok:false, error:'invalid_json' }) }
      const amount = toNumber(b.amount, NaN);
      if (!Number.isFinite(amount) || amount <= 0) return sendJson(res, 400, { ok:false, error:'invalid_amount' });
      const userId = await ensureUserWithWallet(b.userId);

      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        await client.query(`UPDATE wallets SET balance = balance + $1, updated_at=NOW() WHERE user_id=$2`, [amount, userId]);
        await client.query(
          `INSERT INTO transactions (user_id, amount, side, reason, meta)
           VALUES ($1,$2,'credit','deposit','{}'::jsonb)`, [userId, amount]
        );
        await client.query('COMMIT');
        const w = await q(`SELECT balance, currency FROM wallets WHERE user_id=$1`, [userId]);
        return sendJson(res, 200, { ok:true, userId, wallet: w.rows[0] });
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch {}
        console.error('deposit error', e);
        return sendJson(res, 500, { ok:false, error:'server_error' });
      } finally { client.release(); }
    }

    /* -------- Rooms: CREATE (respects all new fields) -------- */
    if (req.method === 'POST' && pathname === '/rooms/create') {
      let b; try { b = await readJsonBody(req) } catch { return sendJson(res, 400, { ok:false, error:'invalid_json' }) }

      const userIdIn = b.userId;
      const game     = (b.game || 'ludo').trim();
      const stake    = toNumber(b.stake, 0);

      const maxPlayers = isInt(b.maxPlayers) ? b.maxPlayers : toNumber(b.maxPlayers, 2);
      const mode       = (b.mode || 'h2h').trim();
      const minPlayers = isInt(b.minPlayers) ? b.minPlayers : 2;
      const autostart  = !!b.autostart;
      const countdownSeconds   = isInt(b.countdownSeconds) ? b.countdownSeconds : 0;
      const requireMutualReady = !!b.require_mutual_ready;
      const config     = typeof b.config === 'object' && b.config ? b.config : {};

      if (!isInt(maxPlayers) || maxPlayers < 2 || maxPlayers > 10)
        return sendJson(res, 400, { ok:false, error:'invalid_maxPlayers' });
      if (!isInt(minPlayers) || minPlayers < 2 || minPlayers > maxPlayers)
        return sendJson(res, 400, { ok:false, error:'invalid_minPlayers' });
      if (stake < 0) return sendJson(res, 400, { ok:false, error:'invalid_stake' });

      const userId = await ensureUserWithWallet(userIdIn);

      // (Optional) require host has balance >= stake
      const w = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
      const bal = toNumber(w.rows[0]?.balance, 0);
      if (bal < stake) return sendJson(res, 400, { ok:false, error:'insufficient_funds', balance: bal, needed: stake });

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Debit host for stake (host buy-in)
        if (stake > 0) {
          await client.query(`UPDATE wallets SET balance = balance - $1, updated_at=NOW() WHERE user_id=$2`, [stake, userId]);
          await client.query(
            `INSERT INTO transactions (user_id, amount, side, reason, meta)
             VALUES ($1,$2,'debit','room_create', jsonb_build_object('game',$3))`,
            [userId, stake, game]
          );
        }

        // Create room using *exactly* what the client sent
        const rRoom = await client.query(
          `INSERT INTO rooms (
              host_user_id, game, max_players, current_players, status,
              mode, min_players, autostart, countdown_seconds, require_mutual_ready, config
           ) VALUES ($1,$2,$3,1,'open',$4,$5,$6,$7,$8,$9::jsonb)
           RETURNING id, created_at, host_user_id, game, max_players, current_players, status,
                     mode, min_players, autostart, countdown_seconds, require_mutual_ready, config`,
          [userId, game, maxPlayers, mode, minPlayers, autostart, countdownSeconds, requireMutualReady, JSON.stringify(config)]
        );
        const room = rRoom.rows[0];

        // Create initial READY match for this room with the stake
        const rMatch = await client.query(
          `INSERT INTO matches (room_id, status, stake_amount, currency, game)
           VALUES ($1,'ready',$2,'USDT',$3)
           RETURNING id, stake_amount`,
          [room.id, stake, game]
        );

        await client.query('COMMIT');

        // API response mirrors what your app expects
        return sendJson(res, 200, {
          ok: true,
          room: {
            id: room.id,
            host_user_id: room.host_user_id,
            game: room.game,
            stake: Number(rMatch.rows[0].stake_amount).toFixed(2),
            max_players: room.max_players,
            current_players: room.current_players,
            status: room.status,
            created_at: room.created_at,
            mode: room.mode,
            min_players: room.min_players,
            autostart: room.autostart,
            countdown_seconds: room.countdown_seconds,
            require_mutual_ready: room.require_mutual_ready,
            config: room.config
          }
        });
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch {}
        console.error('rooms/create error', e);
        return sendJson(res, 500, { ok:false, error:'server_error' });
      } finally { client.release(); }
    }

    /* -------- Rooms: JOIN -------- */
    if (req.method === 'POST' && pathname === '/rooms/join') {
      let b; try { b = await readJsonBody(req) } catch { return sendJson(res, 400, { ok:false, error:'invalid_json' }) }
      const roomId = String(b.roomId || '').trim();
      const userIdIn = b.userId;

      if (!roomId) return sendJson(res, 400, { ok:false, error:'missing_roomId' });

      const rRoom = await q(
        `SELECT id, host_user_id, game, max_players, current_players, status
         FROM rooms WHERE id=$1`, [roomId]
      );
      if (rRoom.rowCount === 0) return sendJson(res, 404, { ok:false, error:'room_not_found' });
      const room = rRoom.rows[0];
      if (room.status !== 'open') return sendJson(res, 400, { ok:false, error:'room_not_open' });
      if (room.current_players >= room.max_players) return sendJson(res, 400, { ok:false, error:'room_full' });

      // Get current stake for this room from the READY match
      const rStake = await q(
        `SELECT stake_amount FROM matches
          WHERE room_id=$1 AND status IN ('ready','waiting')
          ORDER BY created_at DESC LIMIT 1`, [room.id]
      );
      const stakeNum = toNumber(rStake.rows[0]?.stake_amount, 0);

      const userId = await ensureUserWithWallet(userIdIn);

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Hold player stake
        if (stakeNum > 0) {
          const w = await client.query(`SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE`, [userId]);
          const bal = toNumber(w.rows[0]?.balance, 0);
          if (bal < stakeNum) {
            await client.query('ROLLBACK');
            return sendJson(res, 400, { ok:false, error:'insufficient_funds', balance: bal, needed: stakeNum });
          }
          await client.query(`UPDATE wallets SET balance = balance - $1, updated_at=NOW() WHERE user_id=$2`, [stakeNum, userId]);
          await client.query(
            `INSERT INTO transactions (user_id, amount, side, reason, meta)
             VALUES ($1,$2,'debit','room_join', jsonb_build_object('roomId',$3,'game',$4))`,
            [userId, stakeNum, room.id, room.game]
          );
        }

        // increment players
        const rUpd = await client.query(
          `UPDATE rooms
             SET current_players = current_players + 1
           WHERE id=$1 AND current_players < max_players AND status='open'
           RETURNING id, game, max_players, current_players, status`,
          [room.id]
        );
        if (rUpd.rowCount === 0) {
          await client.query('ROLLBACK');
          return sendJson(res, 400, { ok:false, error:'became_full_try_again' });
        }
        const updatedRoom = rUpd.rows[0];

        // If filled, mark full (match stays 'ready' until your game starts it)
        if (updatedRoom.current_players >= updatedRoom.max_players) {
          await client.query(`UPDATE rooms SET status='full' WHERE id=$1`, [room.id]);
        }

        await client.query('COMMIT');

        const rBal = await q(`SELECT balance FROM wallets WHERE user_id=$1`, [userId]);
        return sendJson(res, 200, {
          ok: true,
          room: {
            id: updatedRoom.id,
            game: updatedRoom.game,
            stake: stakeNum.toFixed(2),
            max_players: updatedRoom.max_players,
            current_players: updatedRoom.current_players,
            status: updatedRoom.status
          },
          balance: toNumber(rBal.rows[0]?.balance, 0)
        });
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch {}
        console.error('rooms/join error', e);
        return sendJson(res, 500, { ok:false, error:'server_error' });
      } finally { client.release(); }
    }

    /* -------- Rooms: LEAVE (refund only if still open) -------- */
    if (req.method === 'POST' && pathname === '/rooms/leave') {
      let b; try { b = await readJsonBody(req) } catch { return sendJson(res, 400, { ok:false, error:'invalid_json' }) }
      const roomId = String(b.roomId || '').trim();
      const userId = await ensureUserWithWallet(b.userId);
      if (!roomId) return sendJson(res, 400, { ok:false, error:'missing_roomId' });

      const rRoom = await q(`SELECT * FROM rooms WHERE id=$1`, [roomId]);
      if (rRoom.rowCount === 0) return sendJson(res, 404, { ok:false, error:'room_not_found' });
      const room = rRoom.rows[0];
      if (room.status !== 'open') return sendJson(res, 400, { ok:false, error:'room_not_open' });

      // read stake from current ready/waiting match
      const rStake = await q(
        `SELECT stake_amount FROM matches
         WHERE room_id=$1 AND status IN ('ready','waiting')
         ORDER BY created_at DESC LIMIT 1`, [room.id]
      );
      const stakeNum = toNumber(rStake.rows[0]?.stake_amount, 0);

      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        if (stakeNum > 0) {
          await client.query(`UPDATE wallets SET balance = balance + $1, updated_at=NOW() WHERE user_id=$2`, [stakeNum, userId]);
          await client.query(
            `INSERT INTO transactions (user_id, amount, side, reason, meta)
             VALUES ($1,$2,'credit','refund', jsonb_build_object('roomId',$3))`,
            [userId, stakeNum, room.id]
          );
        }

        await client.query(
          `UPDATE rooms SET current_players = GREATEST(current_players - 1, 1) WHERE id=$1`,
          [room.id]
        );

        await client.query('COMMIT');
        const r2 = await q(`SELECT * FROM rooms WHERE id=$1`, [room.id]);
        return sendJson(res, 200, { ok:true, room: r2.rows[0] });
      } catch (e) {
        try { await client.query('ROLLBACK'); } catch {}
        console.error('rooms/leave error', e);
        return sendJson(res, 500, { ok:false, error:'server_error' });
      } finally { client.release(); }
    }

    /* -------- Rooms: LIST -------- */
    if (req.method === 'GET' && pathname === '/rooms/list') {
      const status = String(query.status || 'open');
      const r = await q(
        `SELECT id, host_user_id, game, max_players, current_players, status,
                mode, min_players, autostart, countdown_seconds, require_mutual_ready, config, created_at
           FROM rooms
          WHERE status=$1
          ORDER BY created_at DESC
          LIMIT 100`, [status]
      );
      return sendJson(res, 200, { ok:true, rooms: r.rows });
    }

    return sendJson(res, 404, { ok:false, error:'not_found' });
  } catch (err) {
    console.error('HTTP error:', err);
    const msg = err?.kind === 'invalid_json' ? 'invalid_json' : 'server_error';
    return sendJson(res, 500, { ok:false, error: msg });
  }
});

/* =========================
   WebSocket
   ========================= */
const wss = new WebSocketServer({ noServer: true });
wss.on('connection', (ws)=>{ ws.on('message', m=> ws.send(String(m))); ws.send('connected'); });
server.on('upgrade', (req, socket, head)=>{
  if (req.url === '/ws') wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  else socket.destroy();
});

/* =========================
   Boot
   ========================= */
(async ()=>{
  try{
    await pool.connect(); console.log('✅ Connected to Postgres');
    await runMigrations();
    server.listen(PORT, ()=> console.log(`✅ Server on :${PORT}`));
  }catch(e){
    console.error('❌ Startup error', e); process.exit(1);
  }
})();