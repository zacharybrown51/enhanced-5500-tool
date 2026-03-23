// Cloudflare Pages Function: /api/market-indices
// Yahoo Finance v8 chart endpoint (no auth required)
// Uses range=1d for daily change + range=5d for sparklines
// Market open/closed determined from trading period timestamps, NOT marketState string

const SYMBOLS = [
  { symbol: '%5EGSPC', label: 'S&P 500', display: '^GSPC' },
  { symbol: '%5EDJI', label: 'Dow', display: '^DJI' },
  { symbol: '%5EIXIC', label: 'Nasdaq', display: '^IXIC' }
];

const YF_HEADERS = { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' };

function jsonResp(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Cache-Control': 'no-cache, no-store, must-revalidate'
    }
  });
}

function normalizeSymbol(sym) {
  var map = {};
  SYMBOLS.forEach(function(s) {
    map[s.symbol] = s.display;
    map[decodeURIComponent(s.symbol)] = s.display;
  });
  return map[sym] || decodeURIComponent(sym);
}

// Determine if market is currently open from trading period timestamps
function getMarketStatus(meta) {
  var now = Math.floor(Date.now() / 1000);

  // Yahoo provides currentTradingPeriod with unix timestamps for regular/pre/post hours
  var tp = meta.currentTradingPeriod;
  if (tp && tp.regular) {
    var regStart = tp.regular.start;
    var regEnd = tp.regular.end;

    if (now >= regStart && now <= regEnd) {
      return 'open';
    }

    // Check pre-market
    if (tp.pre && now >= tp.pre.start && now < regStart) {
      return 'prepost';
    }

    // Check post-market
    if (tp.post && now > regEnd && now <= tp.post.end) {
      return 'prepost';
    }
  }

  // Also check the marketState string as secondary signal
  var state = (meta.marketState || '').toUpperCase();
  if (state === 'REGULAR') return 'open';
  if (state === 'PRE' || state === 'POST' || state === 'POSTPOST') return 'prepost';

  return 'closed';
}

async function fetchAllData() {
  var results = [];
  var marketStatus = 'closed';
  var lastTs = null;
  var debugInfo = {};

  for (var i = 0; i < SYMBOLS.length; i++) {
    var sym = SYMBOLS[i].symbol;
    var display = SYMBOLS[i].display;

    try {
      // Fetch 1d chart (for accurate daily change) and 5d chart (for sparkline) in parallel
      var [resp1d, resp5d] = await Promise.all([
        fetch('https://query2.finance.yahoo.com/v8/finance/chart/' + sym + '?interval=5m&range=1d', { headers: YF_HEADERS }),
        fetch('https://query2.finance.yahoo.com/v8/finance/chart/' + sym + '?interval=1d&range=5d', { headers: YF_HEADERS })
      ]);

      // Parse 1d data (for price, daily change, market status)
      var chart1d = null;
      if (resp1d.ok) {
        var data1d = await resp1d.json();
        chart1d = data1d && data1d.chart && data1d.chart.result && data1d.chart.result[0];
      }

      // Parse 5d data (for sparkline)
      var chart5d = null;
      if (resp5d.ok) {
        var data5d = await resp5d.json();
        chart5d = data5d && data5d.chart && data5d.chart.result && data5d.chart.result[0];
      }

      if (!chart1d && !chart5d) continue;

      var meta = (chart1d && chart1d.meta) || (chart5d && chart5d.meta) || {};
      var price = Number(meta.regularMarketPrice);

      // For 1d range, chartPreviousClose = yesterday's close (exactly what we want)
      var meta1d = chart1d ? chart1d.meta : {};
      var prevClose = Number(meta1d.chartPreviousClose || meta1d.previousClose);
      if (!isFinite(prevClose) || prevClose <= 0) {
        prevClose = Number(meta.previousClose || meta.chartPreviousClose);
      }

      var change = null;
      var changePct = null;
      if (isFinite(price) && isFinite(prevClose) && prevClose > 0) {
        change = Math.round((price - prevClose) * 100) / 100;
        changePct = ((price - prevClose) / prevClose) * 100;
      }

      // Build 5-day sparkline from the 5d chart
      var sparkline = [];
      if (chart5d) {
        var closes = (chart5d.indicators && chart5d.indicators.quote && chart5d.indicators.quote[0] && chart5d.indicators.quote[0].close) || [];
        for (var j = 0; j < closes.length; j++) {
          if (closes[j] != null && isFinite(closes[j])) {
            sparkline.push(Math.round(closes[j] * 100) / 100);
          }
        }
      }
      // Add current price as last point
      if (isFinite(price) && (sparkline.length === 0 || sparkline[sparkline.length - 1] !== Math.round(price * 100) / 100)) {
        sparkline.push(Math.round(price * 100) / 100);
      }

      // Determine market status from the first symbol's trading period
      if (i === 0) {
        marketStatus = getMarketStatus(meta);
        lastTs = meta.regularMarketTime || null;

        debugInfo = {
          marketStateString: meta.marketState || 'missing',
          computedStatus: marketStatus,
          regularMarketTime: meta.regularMarketTime,
          price: price,
          prevClose1d: Number(meta1d.chartPreviousClose),
          prevCloseField: Number(meta.previousClose),
          change: change,
          changePct: changePct,
          tradingPeriod: meta.currentTradingPeriod ? {
            preStart: meta.currentTradingPeriod.pre ? meta.currentTradingPeriod.pre.start : null,
            regStart: meta.currentTradingPeriod.regular ? meta.currentTradingPeriod.regular.start : null,
            regEnd: meta.currentTradingPeriod.regular ? meta.currentTradingPeriod.regular.end : null,
            postEnd: meta.currentTradingPeriod.post ? meta.currentTradingPeriod.post.end : null
          } : 'missing',
          serverTimeNow: Math.floor(Date.now() / 1000),
          fetchedAt: new Date().toISOString()
        };
      }

      results.push({
        symbol: display,
        price: isFinite(price) ? price : null,
        previousClose: isFinite(prevClose) ? prevClose : null,
        changesPercentage: isFinite(changePct) ? changePct : null,
        change: isFinite(change) ? change : null,
        sparkline: sparkline,
        marketState: (meta.marketState || '').toUpperCase(),
        lastTradeTimestamp: meta.regularMarketTime || null,
        exchange: meta.exchangeName || null
      });
    } catch (e) { /* skip failed symbol */ }
  }

  if (!results.length) throw new Error('No data returned from Yahoo');

  return {
    results: results,
    marketStatus: marketStatus,
    lastTs: lastTs,
    debug: debugInfo
  };
}

export async function onRequestOptions() {
  return new Response('', {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'GET, OPTIONS'
    }
  });
}

export async function onRequestGet() {
  try {
    var data = await fetchAllData();

    return jsonResp(200, {
      ok: true,
      quotes: data.results,
      marketStatus: data.marketStatus,
      lastUpdated: data.lastTs,
      source: 'yahoo-v8-chart',
      _debug: data.debug
    });
  } catch (err) {
    return jsonResp(200, { ok: false, error: err.message || 'Failed.', quotes: [], _debug: { error: String(err) } });
  }
}
