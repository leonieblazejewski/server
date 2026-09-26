// server.js
import { WebSocketServer, WebSocket } from "ws";
import http from "node:http";

const TARGET_HOST = "hf.dientuai.online";
const PORT = process.env.PORT || 8080;

const stats = {
  connTotal: 0,
  connActive: 0,
  errors: 0,
  bytesUp: 0,
  bytesDown: 0,
};

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

const httpServer = http.createServer((req, res) => {
  if (req.url === "/__stats") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(stats, null, 2));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ noServer: true });

httpServer.on("upgrade", (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (clientWs) => {
    wss.emit("connection", clientWs, req);
  });
});

wss.on("connection", (clientWs, req) => {
  stats.connTotal++;
  stats.connActive++;
  const id = stats.connTotal;
  const ip = req.socket.remoteAddress;

  const targetUrl = `wss://${TARGET_HOST}${req.url}`;

  const forwardHeaders = { ...req.headers };
  delete forwardHeaders.host;
  delete forwardHeaders.connection;
  delete forwardHeaders.upgrade;
  delete forwardHeaders["sec-websocket-key"];
  delete forwardHeaders["sec-websocket-version"];
  delete forwardHeaders["sec-websocket-extensions"];
  delete forwardHeaders["sec-websocket-protocol"]; // truyền riêng qua `protocols` bên dưới

  // PX1 chỉ chấp nhận subprotocol "polo-px1" — bắt buộc phải có
  const clientProtocols = req.headers["sec-websocket-protocol"]
    ? req.headers["sec-websocket-protocol"].split(",").map((s) => s.trim())
    : [];
  const protocols = clientProtocols.length ? clientProtocols : ["polo-px1"];

  log(`[${id}] client connected from ${ip}, path=${req.url}, protocols=${protocols}, active=${stats.connActive}`);

  const pending = []; // { data, isBinary }
  let upstreamOpen = false;

  const upstreamWs = new WebSocket(targetUrl, protocols, {
    headers: forwardHeaders,
  });

  upstreamWs.on("open", () => {
    upstreamOpen = true;
    log(`[${id}] upstream connected, negotiated protocol=${upstreamWs.protocol}`);
    for (const { data, isBinary } of pending) {
      upstreamWs.send(data, { binary: isBinary });
    }
    pending.length = 0;
  });

  upstreamWs.on("message", (data, isBinary) => {
    stats.bytesDown += data.length;
    if (clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(data, { binary: isBinary });
    }
  });

  upstreamWs.on("close", (code, reason) => {
    log(`[${id}] upstream closed: ${code} ${reason.toString()}`);
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close(code);
  });

  upstreamWs.on("unexpected-response", (req2, res2) => {
    stats.errors++;
    log(`[${id}] upstream unexpected-response: ${res2.statusCode}`);
    clientWs.close(1011, "upstream rejected");
  });

  upstreamWs.on("error", (err) => {
    stats.errors++;
    log(`[${id}] upstream error:`, err.message);
    if (clientWs.readyState === WebSocket.OPEN) clientWs.close();
  });

  clientWs.on("message", (data, isBinary) => {
    stats.bytesUp += data.length;
    if (upstreamOpen && upstreamWs.readyState === WebSocket.OPEN) {
      upstreamWs.send(data, { binary: isBinary });
    } else {
      pending.push({ data, isBinary });
    }
  });

  clientWs.on("close", (code, reason) => {
    stats.connActive--;
    log(`[${id}] client closed: ${code} ${reason.toString()}, active=${stats.connActive}`);
    if (upstreamWs.readyState === WebSocket.OPEN || upstreamWs.readyState === WebSocket.CONNECTING) {
      upstreamWs.close();
    }
  });

  clientWs.on("error", (err) => {
    stats.errors++;
    log(`[${id}] client error:`, err.message);
  });
});

setInterval(() => {
  log(
    "STATS",
    `total=${stats.connTotal}`,
    `active=${stats.connActive}`,
    `errors=${stats.errors}`,
    `up=${stats.bytesUp}B`,
    `down=${stats.bytesDown}B`
  );
}, 30_000);

httpServer.listen(PORT, "0.0.0.0", () => {
  log(`WS bridge listening on :${PORT} -> wss://${TARGET_HOST}`);
});
