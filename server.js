import http from "node:http";
import WebSocket, { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 10000);
const ENV = String(process.env.CTRADER_ENV || "live").toLowerCase();
const CTID = Number(process.env.CTRADER_ACCOUNT_ID || 0);
const ACCESS_TOKEN = String(process.env.CTRADER_ACCESS_TOKEN || "");
const CLIENT_ID = String(process.env.CTRADER_CLIENT_ID || "");
const CLIENT_SECRET = String(process.env.CTRADER_CLIENT_SECRET || "");
const SYMBOL_IDS = String(process.env.CTRADER_SYMBOL_IDS || "")
  .split(",").map(v => Number(v.trim())).filter(Number.isFinite);
const PERIODS = String(process.env.CTRADER_PERIODS || "1,5,7,8,9")
  .split(",").map(v => Number(v.trim())).filter(Number.isFinite);

const WS_URL = ENV === "demo" ? "wss://demo.ctraderapi.com:5036" : "wss://live.ctraderapi.com:5036";

const state = {
  startedAt: Date.now(),
  connected: false,
  authenticated: false,
  accountAuthenticated: false,
  lastMessageAt: 0,
  lastSpotAt: 0,
  subscriptions: 0,
  lastError: "",
  symbolIds: SYMBOL_IDS,
  periods: PERIODS
};

let socket = null;
let heartbeatTimer = null;
let reconnectTimer = null;
const clients = new Set();

function broadcast(event) {
  const payload = JSON.stringify(event);
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

function send(payloadType, payload = {}) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return false;
  socket.send(JSON.stringify({
    clientMsgId: String(Date.now()) + "-" + Math.random().toString(36).slice(2,8),
    payloadType,
    payload
  }));
  return true;
}

function scheduleReconnect() {
  if (reconnectTimer) return;
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connect();
  }, 5000);
}

function connect() {
  if (!CLIENT_ID || !CLIENT_SECRET || !ACCESS_TOKEN || !CTID) {
    state.lastError = "Missing cTrader credentials/account environment variables.";
    broadcast({type:"status", state});
    return;
  }

  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;

  socket = new WebSocket(WS_URL);

  socket.on("open", () => {
    state.connected = true;
    state.lastError = "";
    broadcast({type:"status", state});

    // cTrader requires application authentication before account authentication.
    send(2100, {clientId: CLIENT_ID, clientSecret: CLIENT_SECRET});
  });

  socket.on("message", raw => {
    state.lastMessageAt = Date.now();

    let message;
    try { message = JSON.parse(raw.toString()); }
    catch { return; }

    const payload = message.payload && typeof message.payload === "object" ? message.payload : message;

    if (message.errorCode) {
      state.lastError = String(message.description || message.errorCode);
      broadcast({type:"ctrader_error", error:state.lastError, message});
      return;
    }

    const type = Number(message.payloadType);

    if (type === 2101) {
      state.authenticated = true;
      send(2102, {ctidTraderAccountId: CTID, accessToken: ACCESS_TOKEN});
      broadcast({type:"status", state});
      return;
    }

    if (type === 2103) {
      state.accountAuthenticated = true;
      broadcast({type:"status", state});

      if (SYMBOL_IDS.length) {
        send(2127, {
          ctidTraderAccountId: CTID,
          symbolId: SYMBOL_IDS,
          subscribeToSpotTimestamp: true
        });

        for (const symbolId of SYMBOL_IDS) {
          for (const period of PERIODS) {
            send(2135, {ctidTraderAccountId: CTID, symbolId, period});
            state.subscriptions++;
          }
        }
      }
      return;
    }

    if (type === 2131) {
      state.lastSpotAt = Date.now();
      broadcast({
        type:"spot",
        receivedAt: Date.now(),
        symbolId: Number(payload.symbolId || 0),
        bid: payload.bid != null ? Number(payload.bid) / 100000 : null,
        ask: payload.ask != null ? Number(payload.ask) / 100000 : null,
        timestamp: payload.timestamp || null,
        trendbars: Array.isArray(payload.trendbar) ? payload.trendbar : []
      });
      return;
    }

    broadcast({type:"ctrader_message", message});
  });

  socket.on("close", () => {
    state.connected = false;
    state.authenticated = false;
    state.accountAuthenticated = false;
    broadcast({type:"status", state});
    scheduleReconnect();
  });

  socket.on("error", err => {
    state.lastError = err?.message || String(err);
    broadcast({type:"status", state});
  });
}

heartbeatTimer = setInterval(() => {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({clientMsgId:"heartbeat-" + Date.now(), payloadType:51, payload:{}}));
  }
}, 9000);

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health") {
    res.writeHead(200, {"content-type":"application/json"});
    res.end(JSON.stringify({
      ok:true,
      service:"volatility-ctrader-stream",
      ctrader:{
        connected:state.connected,
        authenticated:state.authenticated,
        accountAuthenticated:state.accountAuthenticated,
        lastSpotAt:state.lastSpotAt || null,
        lastError:state.lastError || null
      }
    }));
    return;
  }

  if (url.pathname === "/state") {
    res.writeHead(200, {"content-type":"application/json"});
    res.end(JSON.stringify(state));
    return;
  }

  if (url.pathname === "/") {
    res.writeHead(200, {"content-type":"text/plain"});
    res.end("Volatility cTrader persistent stream service");
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({server, path:"/stream"});
wss.on("connection", client => {
  clients.add(client);
  client.send(JSON.stringify({type:"status", state}));
  client.on("close", () => clients.delete(client));
  client.on("error", () => clients.delete(client));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`cTrader stream service listening on ${PORT}; endpoint=${WS_URL}`);
  connect();
});

process.on("SIGTERM", () => {
  clearInterval(heartbeatTimer);
  if (socket) socket.close();
  server.close(() => process.exit(0));
});
