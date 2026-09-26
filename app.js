// server.js
import http from "node:http";
import httpProxy from "http-proxy";

const TARGET = "https://node--wss--9fb77cgtfy78.code.run";

const proxy = httpProxy.createProxyServer({
  target: TARGET,
  changeOrigin: true,
  secure: true,
  ws: true,
});

proxy.on("error", (err, req, res) => {
  console.error("Proxy error:", err.message);
  if (res && res.writeHead) {
    res.writeHead(502, { "Content-Type": "text/plain" });
    res.end("Bad gateway");
  }
});

const server = http.createServer((req, res) => {
  proxy.web(req, res);
});

// Xử lý WebSocket upgrade, tương đương check request.headers.get("Upgrade") === "websocket"
server.on("upgrade", (req, socket, head) => {
  proxy.ws(req, socket, head);
});

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Proxy listening on :${PORT} -> ${TARGET}`);
});
