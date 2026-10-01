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
  trendbarDiagnosticLogged: false,
  historicalBarsLoaded: 0,
  historicalRequests: 0,
  historicalQueueRemaining: 0,
  symbolDiscoveryCount: 0,
  symbols: [],
  latest: {},
  symbolIds: SYMBOL_IDS,
  periods: PERIODS
};

let socket = null;
let heartbeatTimer = null;
let reconnectTimer = null;
const clients = new Set();
let historicalQueue = [];
let historicalInFlight = false;

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

  const symbolIds = Array.from(new Set(state.symbolIds)).filter(Number.isFinite);
  if (!symbolIds.length) {
    state.stage = "no_symbols_to_subscribe";
    state.lastError = "No symbol IDs available for market-data subscription.";
    broadcast({ type: "status", state });
    return;
  }

  // Subscribe to spots first. cTrader requires the spot subscription
  // before live trendbar events can be delivered.
  send(2127, {
    ctidTraderAccountId: CTID,
    symbolId: symbolIds,
    subscribeToSpotTimestamp: true
  });

  // cTrader allows up to 50 non-historical requests/sec per connection.
  // Queue trendbar subscriptions in small batches instead of firing 100+
  // requests at once.
  const requests = [];
  for (const symbolId of symbolIds) {
    for (const period of PERIODS) {
      requests.push({ symbolId, period });
    }
  }

  let offset = 0;
  const batchSize = 40;
  const sendBatch = () => {
    const batch = requests.slice(offset, offset + batchSize);
    for (const { symbolId, period } of batch) {
      if (send(2135, {
        ctidTraderAccountId: CTID,
        symbolId,
        period
      })) {
        state.subscriptions++;
      }
    }
    offset += batch.length;
    if (offset < requests.length) {
      setTimeout(sendBatch, 1000);
    } else {
      console.log(`[diagnostic] queued ${requests.length} live trendbar subscriptions across ${symbolIds.length} symbols and ${PERIODS.length} periods.`);
      queueHistoricalMarketData(symbolIds);
    }
  };

  sendBatch();
}

function candleFromTrendbar(bar, liveBid = null) {
  const period = Number(bar.period || 0);
  if (!period) return null;
  const low = bar.low != null ? Number(bar.low) / 100000 : null;
  if (low == null) return null;
  const open = bar.deltaOpen != null ? low + Number(bar.deltaOpen) / 100000 : null;
  const close = bar.deltaClose != null ? low + Number(bar.deltaClose) / 100000 : (liveBid != null ? liveBid : null);
  const high = bar.deltaHigh != null ? low + Number(bar.deltaHigh) / 100000 : null;
  const timestamp = bar.utcTimestampInMinutes != null
    ? Number(bar.utcTimestampInMinutes) * 60000
    : (bar.utcTimestamp != null ? Number(bar.utcTimestamp) : null);
  if (![open, high, low, close, timestamp].every(Number.isFinite)) return null;
  return {
    period,
    open,
    high,
    low,
    close,
    volume: bar.volume != null ? Number(bar.volume) : null,
    timestamp,
    raw: bar
  };
}

function ensureLatestSeries(symbolId) {
  if (!state.latest[symbolId]) {
    state.latest[symbolId] = {
      symbolId,
      symbolName: state.symbols.find(s => s.symbolId === symbolId)?.symbolName || null,
      digits: Number(state.symbols.find(s => s.symbolId === symbolId)?.digits ?? 5),
      bid: null,
      ask: null,
      timestamp: null,
      receivedAt: Date.now(),
      candles: {},
      series: {}
    };
  }
  if (!state.latest[symbolId].series) state.latest[symbolId].series = {};
  return state.latest[symbolId];
}

function mergeCandleIntoSeries(symbolId, candle) {
  const latest = ensureLatestSeries(symbolId);
  const period = candle.period;
  const series = Array.isArray(latest.series[period]) ? latest.series[period] : [];
  const index = series.findIndex(item => item.timestamp === candle.timestamp);
  if (index >= 0) series[index] = { ...series[index], ...candle };
  else series.push(candle);
  series.sort((a, b) => a.timestamp - b.timestamp);
  latest.series[period] = series.slice(-240);
  latest.candles[period] = latest.series[period][latest.series[period].length - 1];
}

function requestNextHistorical() {
  if (historicalInFlight || !historicalQueue.length || !socket || socket.readyState !== WebSocket.OPEN) {
    state.historicalQueueRemaining = historicalQueue.length;
    return;
  }
  const item = historicalQueue.shift();
  historicalInFlight = true;
  state.historicalQueueRemaining = historicalQueue.length;
  state.historicalRequests++;
  const toTimestamp = Date.now();
  const fromTimestamp = toTimestamp - 1000 * 60 * 60 * 24 * 14;
  send(2137, {
    ctidTraderAccountId: CTID,
    symbolId: item.symbolId,
    period: item.period,
    count: 220,
    fromTimestamp,
    toTimestamp
  });
}

function queueHistoricalMarketData(symbolIds) {
  historicalQueue = [];
  for (const symbolId of symbolIds) {
    for (const period of PERIODS) historicalQueue.push({ symbolId, period });
  }
  historicalInFlight = false;
  state.historicalQueueRemaining = historicalQueue.length;
  requestNextHistorical();
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
      if (SYMBOL_IDS.length) {
        state.stage = "subscribing_market_data";
        console.log(`[diagnostic] account authenticated: ${CTID}. Using configured symbol IDs.`);
        subscribeMarketData();
      } else {
        state.stage = "discovering_symbols";
        console.log(`[diagnostic] account authenticated: ${CTID}. Requesting symbol list.`);
        send(2114, { ctidTraderAccountId: CTID, includeArchivedSymbols: false });
      }
      return;
    }

    if (type === 2115) {
      const allSymbols = Array.isArray(payload.symbol) ? payload.symbol : [];
      const volatilitySymbols = allSymbols.filter(symbol =>
        /volatility/i.test(String(symbol.symbolName || ""))
      );
      state.symbolDiscoveryCount = allSymbols.length;
      state.symbols = volatilitySymbols.map(symbol => ({
        symbolId: Number(symbol.symbolId || 0),
        symbolName: symbol.symbolName || null,
        enabled: symbol.enabled !== false,
        description: symbol.description || null,
        digits: Number(symbol.digits || 5)
      })).filter(symbol => symbol.symbolId);
      state.symbolIds = state.symbols.map(symbol => symbol.symbolId);
      console.log(`[diagnostic] symbol discovery: ${allSymbols.length} total, ${state.symbols.length} volatility symbol(s).`);
      broadcast({ type: "symbols_discovered", symbols: state.symbols, totalSymbols: allSymbols.length });
      if (!state.symbolIds.length) {
        state.stage = "no_volatility_symbols_found";
        state.lastError = "No symbols containing 'Volatility' were returned for this cTrader account.";
        broadcast({ type: "status", state });
        return;
      }
      state.stage = "subscribing_market_data";
      subscribeMarketData();
      return;
    }

    if (type === 2138) {
      const symbolId = Number(payload.symbolId || 0);
      const period = Number(payload.period || 0);
      const bars = Array.isArray(payload.trendbar) ? payload.trendbar : [];
      let loaded = 0;
      for (const bar of bars) {
        const candle = candleFromTrendbar(bar);
        if (!candle) continue;
        mergeCandleIntoSeries(symbolId, candle);
        loaded++;
      }
      state.historicalBarsLoaded += loaded;
      historicalInFlight = false;
      state.stage = historicalQueue.length ? "loading_historical_data" : "streaming_market_data";
      state.historicalQueueRemaining = historicalQueue.length;
      if (loaded) {
        const latest = ensureLatestSeries(symbolId);
        latest.receivedAt = Date.now();
        broadcast({ type: "market", receivedAt: Date.now(), data: latest });
      }
      if (loaded) console.log(`[diagnostic] HISTORICAL BARS: symbol=${symbolId} period=${period} count=${loaded}`);
      setTimeout(requestNextHistorical, 250);
      return;
    }

    if (type === 2131) {
      state.lastSpotAt = Date.now();
      state.spotEvents++;
      const incomingTrendbars = Array.isArray(payload.trendbar) ? payload.trendbar.length : 0;
      state.trendbarEvents += incomingTrendbars;
      if (incomingTrendbars > 0 && !state.trendbarDiagnosticLogged) {
        state.trendbarDiagnosticLogged = true;
        console.log(`[diagnostic] LIVE TRENDBARS CONFIRMED: symbol=${Number(payload.symbolId || 0)} count=${incomingTrendbars}`);
      }
      state.stage = "streaming_market_data";
      const symbolId = Number(payload.symbolId || 0);
      const symbolMeta = state.symbols.find(s => s.symbolId === symbolId);
      const digits = Number(symbolMeta?.digits ?? 5);
      // cTrader Open API encodes spot/trendbar prices in 1/100000
      // regardless of the symbol's displayed digits.
      const scale = 100000;
      const bid = payload.bid != null ? Number(payload.bid) / scale : null;
      const ask = payload.ask != null ? Number(payload.ask) / scale : null;
      const trendbars = Array.isArray(payload.trendbar) ? payload.trendbar : [];

      const candles = {};
      const latest = ensureLatestSeries(symbolId);
      for (const bar of trendbars) {
        const period = Number(bar.period || 0);
        if (!period) continue;

        const previous = state.latest[symbolId]?.candles?.[period] || null;
        const low = bar.low != null
          ? Number(bar.low) / scale
          : (previous?.low ?? null);

        const open = bar.deltaOpen != null && low != null
          ? low + Number(bar.deltaOpen) / scale
          : (bar.open != null ? Number(bar.open) / scale : (previous?.open ?? null));

        // Live spot events can carry the current trendbar before cTrader
        // supplies deltaClose. In that case the current bid is the live bar's
        // effective close; never leave the signal feed without a usable price.
        const close = bar.deltaClose != null && low != null
          ? low + Number(bar.deltaClose) / scale
          : (bar.close != null
            ? Number(bar.close) / scale
            : (bid != null ? bid : (previous?.close ?? null)));

        const high = bar.deltaHigh != null && low != null
          ? low + Number(bar.deltaHigh) / scale
          : (bar.high != null ? Number(bar.high) / scale : (previous?.high ?? null));

        // utcTimestampInMinutes is Unix time in minutes and marks the bar open.
        const timestamp = bar.utcTimestampInMinutes != null
          ? Number(bar.utcTimestampInMinutes) * 60000
          : (bar.utcTimestamp != null
            ? Number(bar.utcTimestamp)
            : (bar.timestamp != null ? Number(bar.timestamp) : (previous?.timestamp ?? null)));

        const candle = {
          period,
          open,
          high,
          low,
          close,
          volume: bar.volume != null ? Number(bar.volume) : (previous?.volume ?? null),
          timestamp,
          raw: bar
        };
        candles[period] = candle;
        mergeCandleIntoSeries(symbolId, candle);
      }

      latest.symbolName = symbolMeta?.symbolName || null;
      latest.digits = digits;
      latest.bid = bid;
      latest.ask = ask;
      latest.timestamp = payload.timestamp || null;
      latest.receivedAt = Date.now();
      latest.candles = { ...latest.candles, ...candles };
      state.latest[symbolId] = latest;

      broadcast({
        type: "market",
        receivedAt: Date.now(),
        data: state.latest[symbolId]
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

function publicSnapshot() {
  return {
    ok: true,
    stage: state.stage,
    connected: state.connected,
    authenticated: state.authenticated,
    accountAuthenticated: state.accountAuthenticated,
    symbolCount: state.symbols.length,
    symbols: state.symbols,
    latest: state.latest,
    spotEvents: state.spotEvents,
    trendbarEvents: state.trendbarEvents,
    historicalBarsLoaded: state.historicalBarsLoaded,
    historicalRequests: state.historicalRequests,
    historicalQueueRemaining: state.historicalQueueRemaining,
    subscriptions: state.subscriptions,
    lastMessageAt: state.lastMessageAt || null,
    lastSpotAt: state.lastSpotAt || null,
    lastError: state.lastError || null
  };
}

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

  if (url.pathname === "/feed") {
    res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    res.end(JSON.stringify(publicSnapshot()));
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
      symbolDiscoveryCount: state.symbolDiscoveryCount,
      symbols: state.symbols,
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
  // Immediately hydrate a newly connected browser with the persistent snapshot.
  // Without this, a client that connects after historical loading would wait for
  // the next market event before it received any candles.
  for (const latest of Object.values(state.latest)) {
    if (latest && typeof latest === "object") {
      client.send(JSON.stringify({ type: "market", receivedAt: Date.now(), data: latest }));
    }
  }
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
