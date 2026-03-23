// Cloudflare Pages Function: /api/analyze
// Set ANTHROPIC_API_KEY in Cloudflare dashboard → Settings → Environment Variables

const EXTRACTION_SYSTEM_PROMPT = `You are a Form 5500 retirement plan filing analyst.

Extract useful retirement-plan data from a Form 5500 PDF into one valid JSON object.

Rules:
- Return exactly one valid JSON object
- No markdown, no backticks, no commentary
- If a field is not found, use null
- Prefer accuracy over completeness
- Do not invent values
- Keep arrays concise: investments up to 20, serviceProviders up to 15
- Do not repeat items

Very important: "participantLoans" should only reflect loans/notes receivable from participants as a distinct line item. Do NOT use total assets or broad investment totals. If unclear, use null.

Return this JSON shape:
{
  "planName": string|null,
  "sponsor": string|null,
  "ein": string|null,
  "planNumber": string|null,
  "planYear": string|null,
  "planType": string|null,
  "filingType": string|null,
  "participants": {
    "beginningOfYear": number|null,
    "activeEndOfYear": number|null,
    "totalEndOfYear": number|null,
    "retired": number|null,
    "separated": number|null,
    "deceased": number|null,
    "withAccountBalances": number|null,
    "terminatedUnvested": number|null
  },
  "financials": {
    "totalAssetsBOY": number|null,
    "totalAssetsEOY": number|null,
    "netAssets": number|null,
    "totalContributions": number|null,
    "employerContributions": number|null,
    "participantContributions": number|null,
    "rollovers": number|null,
    "benefitsPaid": number|null,
    "totalIncome": number|null,
    "totalExpenses": number|null,
    "adminExpenses": number|null,
    "investmentGainLoss": number|null,
    "netIncome": number|null,
    "participantLoans": number|null,
    "employerSecurities": number|null
  },
  "assetAllocation": [{"category": string, "beginningValue": number|null, "endValue": number|null}],
  "investments": [{"name": string, "value": number|null, "type": string|null}],
  "serviceProviders": [{"name": string, "ein": string|null, "role": string|null, "serviceCodes": string|null, "directCompensation": number|null, "indirectCompensation": number|null, "relationship": string|null}],
  "planFeatures": [{"code": string, "description": string}],
  "compliance": {
    "lateContributions": boolean|null,
    "lateContributionAmount": number|null,
    "prohibitedTransactions": boolean|null,
    "loansInDefault": boolean|null,
    "fidelityBond": boolean|null,
    "fidelityBondAmount": number|null,
    "blackoutPeriod": boolean|null,
    "failedToPayBenefits": boolean|null,
    "assetsHeldForInvestment": boolean|null,
    "planTerminating": boolean|null
  },
  "auditor": {"name": string|null, "ein": string|null, "opinionType": string|null},
  "fundingInfo": {"minimumRequired": number|null, "actualContribution": number|null, "fundingShortfall": number|null},
  "notes": string|null
}`;

function jsonResp(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'POST, OPTIONS'
    }
  });
}

function toNum(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number') return isFinite(v) ? v : null;
  var s = String(v).replace(/[$,%\s,()]/g, '').trim();
  if (!s) return null;
  var n = Number(s);
  return isFinite(n) ? (/^\(.*\)$/.test(String(v).trim()) ? -n : n) : null;
}
function toBool(v) {
  if (v == null) return null;
  if (typeof v === 'boolean') return v;
  var s = String(v).trim().toLowerCase();
  if (s === 'true' || s === 'yes' || s === '1') return true;
  if (s === 'false' || s === 'no' || s === '0') return false;
  return null;
}
function toStr(v) { if (v == null) return null; var s = String(v).trim(); return s || null; }

function uniqueBy(arr, fn, max) {
  if (!Array.isArray(arr)) return [];
  var seen = {}, out = [];
  for (var i = 0; i < Math.min(arr.length, max || 50); i++) {
    var k = String(fn(arr[i]) || '').toLowerCase();
    if (!k || seen[k]) continue;
    seen[k] = true;
    out.push(arr[i]);
  }
  return out;
}

function normalize(r) {
  var p = r.participants || {}, f = r.financials || {}, c = r.compliance || {}, a = r.auditor || {}, fu = r.fundingInfo || {};
  return {
    planName: toStr(r.planName), sponsor: toStr(r.sponsor),
    ein: toStr(r.ein) ? toStr(r.ein).replace(/[^\d-]/g, '') : null,
    planNumber: toStr(r.planNumber), planYear: toStr(r.planYear),
    planType: toStr(r.planType), filingType: toStr(r.filingType),
    participants: { beginningOfYear: toNum(p.beginningOfYear), activeEndOfYear: toNum(p.activeEndOfYear), totalEndOfYear: toNum(p.totalEndOfYear), retired: toNum(p.retired), separated: toNum(p.separated), deceased: toNum(p.deceased), withAccountBalances: toNum(p.withAccountBalances), terminatedUnvested: toNum(p.terminatedUnvested) },
    financials: { totalAssetsBOY: toNum(f.totalAssetsBOY), totalAssetsEOY: toNum(f.totalAssetsEOY), netAssets: toNum(f.netAssets), totalContributions: toNum(f.totalContributions), employerContributions: toNum(f.employerContributions), participantContributions: toNum(f.participantContributions), rollovers: toNum(f.rollovers), benefitsPaid: toNum(f.benefitsPaid), totalIncome: toNum(f.totalIncome), totalExpenses: toNum(f.totalExpenses), adminExpenses: toNum(f.adminExpenses), investmentGainLoss: toNum(f.investmentGainLoss), netIncome: toNum(f.netIncome), participantLoans: toNum(f.participantLoans), employerSecurities: toNum(f.employerSecurities) },
    assetAllocation: uniqueBy((r.assetAllocation || []).map(x => ({ category: toStr(x.category) || 'Unspecified', beginningValue: toNum(x.beginningValue), endValue: toNum(x.endValue) })), x => x.category + '|' + x.endValue, 20),
    investments: uniqueBy((r.investments || []).map(x => ({ name: toStr(x.name) || 'Unnamed', value: toNum(x.value), type: toStr(x.type) })), x => x.name + '|' + x.value, 20),
    serviceProviders: uniqueBy((r.serviceProviders || []).map(x => ({ name: toStr(x.name) || 'Unnamed', ein: toStr(x.ein), role: toStr(x.role), serviceCodes: toStr(x.serviceCodes), directCompensation: toNum(x.directCompensation), indirectCompensation: toNum(x.indirectCompensation), relationship: toStr(x.relationship) })), x => x.name + '|' + x.role, 15),
    planFeatures: uniqueBy((r.planFeatures || []).map(x => ({ code: toStr(x.code) || '', description: toStr(x.description) || '' })).filter(x => x.code || x.description), x => x.code + '|' + x.description, 20),
    compliance: { lateContributions: toBool(c.lateContributions), lateContributionAmount: toNum(c.lateContributionAmount), prohibitedTransactions: toBool(c.prohibitedTransactions), loansInDefault: toBool(c.loansInDefault), fidelityBond: toBool(c.fidelityBond), fidelityBondAmount: toNum(c.fidelityBondAmount), blackoutPeriod: toBool(c.blackoutPeriod), failedToPayBenefits: toBool(c.failedToPayBenefits), assetsHeldForInvestment: toBool(c.assetsHeldForInvestment), planTerminating: toBool(c.planTerminating) },
    auditor: { name: toStr(a.name), ein: toStr(a.ein), opinionType: toStr(a.opinionType) },
    fundingInfo: { minimumRequired: toNum(fu.minimumRequired), actualContribution: toNum(fu.actualContribution), fundingShortfall: toNum(fu.fundingShortfall) },
    notes: toStr(r.notes)
  };
}

function buildInsights(parsed) {
  var f = parsed.financials || {}, p = parsed.participants || {}, c = parsed.compliance || {};
  var assets = f.totalAssetsEOY, assetsBOY = f.totalAssetsBOY;
  var participants = p.withAccountBalances || p.totalEndOfYear || p.activeEndOfYear;
  var employer = f.employerContributions, employee = f.participantContributions;
  var adminExpenses = f.adminExpenses, loans = f.participantLoans;

  var provText = (parsed.serviceProviders || []).map(x => (x.name || '') + ' ' + (x.role || '')).join(' | ').toLowerCase();
  var likelyRK = null;
  [['fidelity','Fidelity'],['empower','Empower'],['great-west','Empower'],['principal','Principal'],['voya','Voya'],['john hancock','John Hancock'],['ascensus','Ascensus'],['schwab','Schwab'],['adp','ADP'],['transamerica','Transamerica'],['lincoln','Lincoln'],['paychex','Paychex']].forEach(function(m) { if (!likelyRK && provText.indexOf(m[0]) >= 0) likelyRK = m[1]; });

  var featureText = (parsed.planFeatures || []).map(x => (x.code || '') + ' ' + (x.description || '')).join(' | ').toLowerCase();
  var hasRoth = featureText.indexOf('roth') >= 0;
  var hasMatch = employer != null && employer > 0;
  var reasonable = participants != null && participants >= 5;
  var assetsPerP = (assets != null && reasonable) ? Math.round(assets / participants) : null;
  var growthPct = (assets != null && assetsBOY != null && assetsBOY !== 0) ? (assets - assetsBOY) / assetsBOY : null;
  var adminRate = (adminExpenses != null && assets > 0) ? adminExpenses / assets : null;
  var loanRate = (loans != null && assets > 0) ? loans / assets : null;
  var segment = participants != null ? (participants < 25 ? 'Micro' : participants < 100 ? 'Small' : participants < 500 ? 'Mid' : 'Large') : assets != null ? (assets < 1e6 ? 'Micro' : assets < 1e7 ? 'Small' : assets < 5e7 ? 'Mid' : 'Large') : null;

  var score = 5;
  if (assets >= 5e7) score += 2; else if (assets >= 1e7) score += 1; else if (assets < 1e6) score -= 1;
  if (reasonable && participants >= 50) score += 1; if (reasonable && participants >= 200) score += 1;
  if (c.lateContributions) score += 1; if (c.prohibitedTransactions) score += 1;
  if (!hasRoth) score += 1; if (!hasMatch && employee > 0) score += 1;
  score = Math.max(1, Math.min(10, Math.round(score)));

  var whatStandsOut = [], discussionPoints = [], questionsToValidate = [];
  if (growthPct > 0.15) whatStandsOut.push('Strong asset growth year over year.');
  else if (growthPct < -0.05) whatStandsOut.push('Assets declined year over year.');
  if (loanRate > 0.08) whatStandsOut.push('Participant loan balances are meaningful relative to assets.');
  if (!hasRoth) whatStandsOut.push('No clear Roth feature identified.');
  if (!hasMatch && employee > 0) whatStandsOut.push('No employer contribution was clearly shown.');
  if (c.lateContributions) whatStandsOut.push('Late contributions flagged.');
  if (c.prohibitedTransactions) whatStandsOut.push('Prohibited transactions flagged.');
  if (likelyRK) discussionPoints.push('Whether ' + likelyRK + ' is still the right fit.');
  else discussionPoints.push('Whether the current provider setup is still the right fit.');
  if (!hasRoth) discussionPoints.push('Whether Roth is available and being used.');
  if (adminRate > 0.0075) discussionPoints.push('Whether fees have been benchmarked recently.');
  if (assetsPerP > 100000) discussionPoints.push('Balance profile supports a deeper fee review.');
  questionsToValidate.push('Is Roth currently available?');
  if (!hasMatch) questionsToValidate.push('Is the lack of employer contribution intentional?');
  if (adminRate != null) questionsToValidate.push('When was the last fee review?');
  if (likelyRK) questionsToValidate.push('How satisfied is the sponsor with ' + likelyRK + '?');

  var summary = (segment ? 'This appears to be a ' + segment.toLowerCase() + '-market plan' : 'This is a defined contribution plan');
  if (assets != null) summary += ' with roughly ' + (assets >= 1e9 ? '$' + (assets/1e9).toFixed(1) + 'B' : assets >= 1e6 ? '$' + (assets/1e6).toFixed(1) + 'M' : '$' + Math.round(assets/1e3) + 'K') + ' in assets';
  if (reasonable) summary += ' and about ' + participants.toLocaleString() + ' participants';
  summary += '. ' + (c.lateContributions || c.prohibitedTransactions ? 'The angle is governance and process.' : 'A plan to validate and benchmark.');

  return {
    subjectiveSummary: summary, score, confidence: (parsed.planName && parsed.ein && assets != null) ? 'high' : 'medium',
    growthCommentary: growthPct != null ? (growthPct > 0.15 ? 'Strong growth.' : growthPct > 0.03 ? 'Modest growth.' : growthPct < -0.05 ? 'Assets declined.' : 'Assets flat.') : null,
    whatStandsOut: whatStandsOut.slice(0, 4), discussionPoints: discussionPoints.slice(0, 4), questionsToValidate: questionsToValidate.slice(0, 5),
    meta: { likelyRecordkeeper: likelyRK, planSegment: segment, assetsPerParticipant: assetsPerP, hasRoth, hasLoans: (loans > 0) || featureText.indexOf('loan') >= 0, hasMatch, assetGrowth: (assets != null && assetsBOY != null) ? Math.round(assets - assetsBOY) : null, assetGrowthPct: growthPct, adminExpenseRate: adminRate, loanRate }
  };
}

export async function onRequestOptions() {
  return new Response('', {
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'Content-Type',
      'Access-Control-Allow-Methods': 'POST, OPTIONS'
    }
  });
}

export async function onRequestPost(context) {
  const apiKey = context.env.ANTHROPIC_API_KEY;
  if (!apiKey) return jsonResp(500, { ok: false, error: 'Missing ANTHROPIC_API_KEY in Cloudflare environment.' });

  try {
    const body = await context.request.json();
    if (body.mode !== 'extract_5500') return jsonResp(400, { ok: false, error: 'Unsupported mode.' });
    if (!body.base64 || typeof body.base64 !== 'string') return jsonResp(400, { ok: false, error: 'Missing base64 PDF.' });

    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514',
        max_tokens: 4096,
        temperature: 0,
        system: EXTRACTION_SYSTEM_PROMPT,
        messages: [{ role: 'user', content: [
          { type: 'document', source: { type: 'base64', media_type: 'application/pdf', data: body.base64 } },
          { type: 'text', text: 'Analyze this Form 5500 and return only the required JSON object.' }
        ]}]
      })
    });

    const data = await resp.json();
    if (!resp.ok) throw new Error((data.error && data.error.message) || 'Anthropic error ' + resp.status);

    var rawText = '';
    for (var i = 0; i < (data.content || []).length; i++) { if (data.content[i].type === 'text') rawText += data.content[i].text; }
    rawText = rawText.trim().replace(/```json\s*/gi, '').replace(/```\s*/g, '');

    // Extract balanced JSON
    var start = rawText.indexOf('{');
    if (start === -1) throw new Error('No JSON in response.');
    var depth = 0, inStr = false, esc = false, end = start;
    for (var i = start; i < rawText.length; i++) {
      var ch = rawText[i];
      if (inStr) { if (esc) esc = false; else if (ch === '\\') esc = true; else if (ch === '"') inStr = false; }
      else { if (ch === '"') inStr = true; else if (ch === '{') depth++; else if (ch === '}') { depth--; if (depth === 0) { end = i; break; } } }
    }

    var parsed = normalize(JSON.parse(rawText.slice(start, end + 1)));
    var insights = buildInsights(parsed);

    return jsonResp(200, { ok: true, parsed, partial: false, repaired: false, insights, derived: insights.meta });
  } catch (err) {
    return jsonResp(200, { ok: false, error: err.message || 'Function failed.' });
  }
}
