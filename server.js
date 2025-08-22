// server.js
const WebSocket = require("ws");

const PORT = process.env.PORT || 3000;
const wss = new WebSocket.Server({ port: PORT });

console.log("Cashdeal WS Server running on port " + PORT);

wss.on("connection", (ws) => {
  console.log("New client connected");

  ws.on("message", (message) => {
    console.log("Received:", message.toString());

    // For now: echo the same message back
    ws.send("Server says: " + message);
  });

  ws.on("close", () => {
    console.log("Client disconnected");
  });
});
