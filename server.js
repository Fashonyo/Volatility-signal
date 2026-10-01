import http from "node:http";
import WebSocket, { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT || 10000);
const ENV = String(process.env.CTRADER_ENV || "live").toLowerCase();
let CTID = Number(process.env.CTRADER_ACCOUNT_ID || 0);
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
  accountId: CTID || null,
  authorizedAccountCount: 0,
  stage: "starting",
  spotEvents: 0,
  trendbarEvents: 0,
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
    clientMsgId: String(Date.now()) + "-" + Math.random().toString(36).slice(2, 8),
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

function authenticateAccount(accountId) {
  CTID = Number(accountId || 0);
  if (!CTID) {
    state.lastError = "cTrader returned no authorized account.";
    broadcast({ type: "status", state });
    return;
  }

  state.accountId = CTID;
  send(2102, {
    ctidTraderAccountId: CTID,
    accessToken: ACCESS_TOKEN
  });
}

function subscribeMarketData() {
  state.accountAuthenticated = true;
  broadcast({ type: "status", state });

  if (SYMBOL_IDS.length) {
    // Subscribe to spots first. cTrader requires the spot subscription
    // before live trendbar events can be delivered.
    send(2127, {
      ctidTraderAccountId: CTID,
      symbolId: SYMBOL_IDS,
      subscribeToSpotTimestamp: true
    });

    for (const symbolId of SYMBOL_IDS) {
      for (const period of PERIODS) {
        send(2135, {
          ctidTraderAccountId: CTID,
          symbolId,
          period
        });
        state.subscriptions++;
      }
    }
  }
}

function connect() {
  state.stage = "checking_credentials";
  console.log(`[diagnostic] connect(): clientId=${Boolean(CLIENT_ID)} clientSecret=${Boolean(CLIENT_SECRET)} accessToken=${Boolean(ACCESS_TOKEN)} accountIdConfigured=${Boolean(CTID)}`);
  if (!CLIENT_ID || !CLIENT_SECRET || !ACCESS_TOKEN) {
    state.stage = "missing_credentials";
    state.lastError = "Missing cTrader credentials environment variables.";
    broadcast({ type: "status", state });
    return;
  }

  if (socket && (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING)) return;

  state.stage = "connecting_ctrader";
  console.log(`[diagnostic] opening cTrader WebSocket: ${WS_URL}`);
  socket = new WebSocket(WS_URL);

  socket.on("open", () => {
    state.connected = true;
    state.stage = "app_authenticating";
    state.lastError = "";
    console.log("[diagnostic] cTrader WebSocket open; sending application auth.");
    broadcast({ type: "status", state });

    // cTrader requires application authentication before account authentication.
    send(2100, {
      clientId: CLIENT_ID,
      clientSecret: CLIENT_SECRET
    });
  });

  socket.on("message", raw => {
    state.lastMessageAt = Date.now();

    let message;
    try {
      message = JSON.parse(raw.toString());
    } catch {
      return;
    }

    const payload = message.payload && typeof message.payload === "object" ? message.payload : message;

    console.log(`[diagnostic] received payloadType=${message.payloadType ?? "unknown"} bytes=${raw.length}`);

    if (message.errorCode) {
      state.stage = "ctrader_error";
      state.lastError = String(message.description || message.errorCode);
      console.error(`[diagnostic] cTrader error: ${state.lastError}`);
      broadcast({ type: "ctrader_error", error: state.lastError, message });
      return;
    }

    const type = Number(message.payloadType);

    if (type === 2101) {
      state.authenticated = true;
      state.stage = CTID ? "account_authenticating" : "discovering_accounts";
      console.log(`[diagnostic] application authenticated; ${CTID ? "authenticating configured account" : "discovering accounts from access token"}.`);

      // If an account ID was explicitly configured, preserve that behavior.
      // Otherwise discover the accounts granted to this access token.
      if (CTID) {
        authenticateAccount(CTID);
      } else {
        send(2149, { accessToken: ACCESS_TOKEN });
      }

      broadcast({ type: "status", state });
      return;
    }

    if (type === 2150) {
      state.stage = "accounts_discovered";
      const accounts = Array.isArray(payload.ctidTraderAccount)
        ? payload.ctidTraderAccount
        : [];

      state.authorizedAccountCount = accounts.length;
      console.log(`[diagnostic] account discovery returned ${accounts.length} account(s).`);

      if (!accounts.length) {
        state.lastError = "cTrader access token has no authorized trading accounts.";
        broadcast({ type: "status", state });
        return;
      }

      // Prefer a live account when running against the live endpoint.
      // Otherwise use the first account returned by cTrader.
      const wantedLive = ENV !== "demo";
      const selected = accounts.find(account => Boolean(account.isLive) === wantedLive) || accounts[0];
      const discoveredId = Number(selected.ctidTraderAccountId || 0);

      if (!discoveredId) {
        state.lastError = "cTrader returned an account without a valid ctidTraderAccountId.";
        broadcast({ type: "status", state });
        return;
      }

      state.accountId = discoveredId;
      CTID = discoveredId;
      broadcast({
        type: "account_discovered",
        accountId: discoveredId,
        accountCount: accounts.length,
        isLive: Boolean(selected.isLive),
        brokerTitle: selected.brokerTitleShort || null
      });

      authenticateAccount(discoveredId);
      return;
    }

    if (type === 2103) {
      state.stage = "subscribing_market_data";
      console.log(`[diagnostic] account authenticated: ${CTID}. Starting spot/trendbar subscriptions.`);
      subscribeMarketData();
      return;
    }

    if (type === 2131) {
      state.lastSpotAt = Date.now();
      state.spotEvents++;
      state.trendbarEvents += Array.isArray(payload.trendbar) ? payload.trendbar.length : 0;
      state.stage = "streaming_market_data";
      broadcast({
        type: "spot",
        receivedAt: Date.now(),
        symbolId: Number(payload.symbolId || 0),
        bid: payload.bid != null ? Number(payload.bid) / 100000 : null,
        ask: payload.ask != null ? Number(payload.ask) / 100000 : null,
        timestamp: payload.timestamp || null,
        trendbars: Array.isArray(payload.trendbar) ? payload.trendbar : []
      });
      return;
    }

    broadcast({ type: "ctrader_message", message });
  });

  socket.on("close", () => {
    console.log("[diagnostic] cTrader WebSocket closed; scheduling reconnect.");
    state.stage = "reconnecting";
    state.connected = false;
    state.authenticated = false;
    state.accountAuthenticated = false;
    state.subscriptions = 0;
    broadcast({ type: "status", state });
    scheduleReconnect();
  });

  socket.on("error", err => {
    state.stage = "socket_error";
    state.lastError = err?.message || String(err);
    console.error(`[diagnostic] WebSocket error: ${state.lastError}`);
    broadcast({ type: "status", state });
  });
}

heartbeatTimer = setInterval(() => {
  if (socket?.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify({
      clientMsgId: "heartbeat-" + Date.now(),
      payloadType: 51,
      payload: {}
    }));
  }
}, 9000);

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);

  if (url.pathname === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      service: "volatility-ctrader-stream",
      ctrader: {
        connected: state.connected,
        authenticated: state.authenticated,
        accountAuthenticated: state.accountAuthenticated,
        accountId: state.accountId,
        authorizedAccountCount: state.authorizedAccountCount,
        lastSpotAt: state.lastSpotAt || null,
        lastError: state.lastError || null
      }
    }));
    return;
  }

  if (url.pathname === "/diagnostic") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      ok: true,
      stage: state.stage,
      connected: state.connected,
      authenticated: state.authenticated,
      accountAuthenticated: state.accountAuthenticated,
      accountId: state.accountId,
      authorizedAccountCount: state.authorizedAccountCount,
      spotEvents: state.spotEvents,
      trendbarEvents: state.trendbarEvents,
      subscriptions: state.subscriptions,
      lastMessageAt: state.lastMessageAt || null,
      lastSpotAt: state.lastSpotAt || null,
      lastError: state.lastError || null
    }));
    return;
  }

  if (url.pathname === "/state") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(state));
    return;
  }

  if (url.pathname === "/") {
    res.writeHead(200, { "content-type": "text/plain" });
    res.end("Volatility cTrader persistent stream service");
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

const wss = new WebSocketServer({ server, path: "/stream" });
wss.on("connection", client => {
  clients.add(client);
  client.send(JSON.stringify({ type: "status", state }));
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
