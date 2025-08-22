// server.js
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });

console.log("Cashdeal WS Server running on port", PORT);

// Keep rooms and players
let rooms = {};

wss.on("connection", (ws) => {
  console.log("New client connected");

  ws.on("message", (msg) => {
    try {
      const data = JSON.parse(msg);
      console.log("Received:", data);

      // --- Handle join ---
      if (data.type === "join") {
        const { room, name } = data;
        if (!rooms[room]) rooms[room] = { players: [], state: {} };
        if (!rooms[room].players.includes(name)) {
          rooms[room].players.push(name);
        }
        ws.room = room;
        ws.name = name;

        // Reply only to the joining client
        ws.send(JSON.stringify({
          type: "joined",
          room,
          you: name,
          players: rooms[room].players
        }));

        // Notify others in the room
        broadcast(room, {
          type: "state",
          room,
          state: rooms[room].state
        });
      }

      // --- Handle moves (game actions) ---
      else if (data.type === "move") {
        const { room, payload } = data;
        if (!rooms[room]) {
          return ws.send(JSON.stringify({ type: "error", msg: "join first" }));
        }

        // Save latest move into state
        rooms[room].state[ws.name] = payload;

        // Broadcast new state to all players
        broadcast(room, {
          type: "state",
          room,
          state: rooms[room].state
        });
      }

    } catch (e) {
      console.log("Invalid message", e);
      ws.send(JSON.stringify({ type: "error", msg: "Invalid JSON" }));
    }
  });

  ws.on("close", () => {
    if (ws.room && rooms[ws.room]) {
      rooms[ws.room].players = rooms[ws.room].players.filter(p => p !== ws.name);
      broadcast(ws.room, {
        type: "state",
        room: ws.room,
        state: rooms[ws.room].state
      });

      // Optional: delete empty room
      if (rooms[ws.room].players.length === 0) {
        delete rooms[ws.room];
      }
    }
    console.log("Client disconnected");
  });
});

// Helper: send message to all in a room
function broadcast(room, data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN && c.room === room) {
      c.send(msg);
    }
  });
}
