// Cloudflare Pages Function: /api/news
// Fetches Yahoo Finance market news via RSS (no auth required)

function jsonResp(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Cache-Control': 'public, max-age=300'
    }
  });
}

function extractItems(xml) {
  var items = [];
  var parts = xml.split('<item>');
  for (var i = 1; i < parts.length && items.length < 8; i++) {
    var titleMatch = parts[i].match(/<title><!\[CDATA\[(.*?)\]\]><\/title>|<title>(.*?)<\/title>/);
    var linkMatch = parts[i].match(/<link>(.*?)<\/link>/);
    var pubMatch = parts[i].match(/<pubDate>(.*?)<\/pubDate>/);
    var title = titleMatch ? (titleMatch[1] || titleMatch[2] || '').trim() : '';
    var link = linkMatch ? linkMatch[1].trim() : '';
    var pubDate = pubMatch ? pubMatch[1].trim() : '';
    if (title && link) {
      items.push({ title: title, link: link, pubDate: pubDate });
    }
  }
  return items;
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
    var url = 'https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC,%5EDJI,%5EIXIC&region=US&lang=en-US';
    var resp = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
    });
    if (!resp.ok) throw new Error('RSS fetch failed: ' + resp.status);
    var xml = await resp.text();
    var items = extractItems(xml);
    if (!items.length) throw new Error('No news items parsed');
    return jsonResp(200, { ok: true, items: items });
  } catch (err) {
    // Fallback: try Google News RSS for stock market
    try {
      var gUrl = 'https://news.google.com/rss/search?q=stock+market+today&hl=en-US&gl=US&ceid=US:en';
      var gResp = await fetch(gUrl, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
      });
      if (!gResp.ok) throw new Error('Google RSS failed');
      var gXml = await gResp.text();
      var gItems = extractItems(gXml);
      if (gItems.length) {
        return jsonResp(200, { ok: true, items: gItems, source: 'google' });
      }
    } catch (e2) { /* fall through */ }

    return jsonResp(200, { ok: false, error: err.message || 'Failed', items: [] });
  }
}
