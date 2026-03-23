#!/usr/bin/env python3
"""Build dataset.json plus validation artifacts for the 5500 site.

Modes:
1. bootstrap mode: extract the current embedded arrays from index.html into dataset.json
2. pipeline mode: read a local ZIP or extracted DOL directory, process filings, and write dataset.json

Design goals:
- preserve the site's 38-slot row contract
- keep the Claude deep-analysis feature untouched
- use filing-type-aware selection rules
- keep mappings conservative where source ambiguity exists
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import re
import statistics
import textwrap
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
INDEX_HTML = PROJECT_ROOT / 'index.html'
DATASET_JSON = PROJECT_ROOT / 'dataset.json'
VALIDATION_JSON = PROJECT_ROOT / 'validation_report.json'
SUMMARY_TXT = PROJECT_ROOT / 'summary_report.txt'
CONFIG_PATH = SCRIPT_DIR / 'config.json'

PROVIDER_ALIASES = [
    ('Empower', r'\b(empower|great[- ]?west|gwrs|prudential retirement)\b'),
    ('Fidelity Investments', r'\b(fidelity|fmr)\b'),
    ('Vanguard', r'\b(vanguard)\b'),
    ('John Hancock', r'\b(john hancock|jhancock|jh trust)\b'),
    ('Principal', r'\b(principal)\b'),
    ('Transamerica', r'\b(transamerica)\b'),
    ('MassMutual', r'\b(massmutual|mass mutual)\b'),
    ('Nationwide', r'\b(nationwide)\b'),
    ('T. Rowe Price', r'\b(t\.?\s?rowe)\b'),
    ('ADP', r'\b(adp)\b'),
    ('Paychex', r'\b(paychex)\b'),
    ('Guideline', r'\b(guideline)\b'),
    ('Human Interest', r'\b(human interest)\b'),
    ('Lincoln Financial', r'\b(lincoln financial|lincoln national)\b'),
    ('Voya', r'\b(voya)\b'),
    ('Merrill', r'\b(merrill|mlpf)\b'),
    ('Charles Schwab', r'\b(schwab)\b'),
    ('Morgan Stanley', r'\b(morgan stanley)\b'),
    ('Wells Fargo', r'\b(wells fargo)\b'),
    ('Ascensus', r'\b(ascensus)\b'),
    ('OneDigital', r'\b(onedigital)\b'),
    ('MMA Securities', r'\b(mma securities)\b'),
]

ROLE_KEYWORDS = {
    'RK': [r'record\s*keeper', r'retirement\s*services', r'trust\s*company', r'retirement\s*plan\s*services'],
    'TPA': [r'third\s*party\s*administrator', r'plan\s*administrator', r'pension\s*consult', r'benefit\s*services'],
    'IA': [r'investment\s*advisor', r'advis', r'wealth', r'capital', r'securities', r'financial'],
    'CUS': [r'custod', r'trustee'],
    'AUD': [r'audit', r'cpa', r'account'],
    'INS': [r'insurance'],
}

CITY_COORDS = {
    'San Diego': (32.7157, -117.1611), 'Carlsbad': (33.1581, -117.3506), 'La Jolla': (32.8328, -117.2713),
    'Escondido': (33.1192, -117.0864), 'Poway': (32.9628, -117.0359), 'El Cajon': (32.7948, -116.9625),
    'Vista': (33.2, -117.2428), 'San Marcos': (33.1434, -117.1661), 'Encinitas': (33.0369, -117.2919),
    'Solana Beach': (32.9912, -117.2714), 'Oceanside': (33.1959, -117.3795), 'Chula Vista': (32.6401, -117.0842),
}


def load_config() -> dict:
    return json.loads(CONFIG_PATH.read_text(encoding='utf-8')) if CONFIG_PATH.exists() else {
        'region_name': 'San Diego County', 'filing_year': '2024', 'zip_prefixes': ['919', '920', '921']
    }


def clean_provider_name(name: str) -> str:
    if not name:
        return ''
    name = str(name).strip()
    name = re.sub(r'\s+', ' ', name)
    for canon, pattern in PROVIDER_ALIASES:
        if re.search(pattern, name, re.I):
            return canon
    return name.title() if name.isupper() and len(name) > 4 else name


def safe_float(val: Any, default: float | None = 0.0):
    try:
        num = float(str(val).replace(',', '').replace('$', '').replace('%', '').strip())
        return num if math.isfinite(num) else default
    except Exception:
        return default


def safe_int(val: Any, default: int = 0):
    try:
        return int(float(str(val).replace(',', '').strip()))
    except Exception:
        return default


def pct_change(end_val: Any, start_val: Any):
    end_num = safe_float(end_val, None)
    start_num = safe_float(start_val, None)
    if end_num is None or start_num is None or start_num <= 0:
        return 0
    return round(((end_num - start_num) / start_num) * 100, 1)


def parse_date(val: Any) -> str:
    raw = str(val or '').strip()
    if not raw:
        return ''
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%Y%m%d', '%m/%d/%y'):
        try:
            return datetime.strptime(raw[:10], fmt).strftime('%Y-%m-%d')
        except Exception:
            pass
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', raw)
    if m:
        return raw[:10]
    return raw


def parse_year(row: dict, *names: str) -> int:
    for name in names:
        value = get_field(row, name)
        if not value:
            continue
        m = re.search(r'(20\d{2}|19\d{2})', value)
        if m:
            return int(m.group(1))
    return 0


def get_field(row: dict, *names, default=''):
    for name in names:
        if name in row and str(row[name]).strip():
            return str(row[name]).strip()
        upper = name.upper()
        for key, value in row.items():
            if key.upper() == upper and str(value).strip():
                return str(value).strip()
    return default


def read_csv_from_zip(zf: zipfile.ZipFile, pattern: str):
    for name in zf.namelist():
        if re.search(pattern, name, re.I):
            with zf.open(name) as f:
                text = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
                return list(csv.DictReader(text))
    return []


def read_csv_from_dir(dirpath: Path, pattern: str):
    for fname in os.listdir(dirpath):
        if re.search(pattern, fname, re.I):
            with open(Path(dirpath) / fname, encoding='utf-8', errors='replace') as f:
                return list(csv.DictReader(f))
    return []


def extract_current_embedded_data(index_path: Path = INDEX_HTML):
    html = index_path.read_text(encoding='utf-8', errors='ignore')
    m = re.search(r"var D=(\[.*?\]);\s*var CD=(\[.*?\]);\s*var TP=(\[.*?\]);", html, re.S)
    if not m:
        raise RuntimeError('Could not find embedded D/CD/TP arrays in index.html')
    return json.loads(m.group(1)), json.loads(m.group(2)), json.loads(m.group(3))


def ensure_index_uses_json(index_path: Path = INDEX_HTML):
    html = index_path.read_text(encoding='utf-8', errors='ignore')
    if 'fetch("dataset.json")' in html or "fetch('dataset.json')" in html:
        return
    html = re.sub(
        r"var\s+D\s*=\s*\[.*?var\s+CD\s*=\s*\[.*?var\s+TP\s*=\s*\[.*?;",
        "var D=[]; var CD=[]; var TP=[];\nfetch('dataset.json').then(function(r){return r.json()}).then(function(ds){D=ds.plans||[]; CD=ds.cities||[]; TP=ds.top_providers||[]; if(typeof boot==='function')boot();});",
        html,
        flags=re.S,
    )
    index_path.write_text(html, encoding='utf-8')




def normalize_site_row(row: list) -> list:
    row = list(row) if isinstance(row, list) else []
    if len(row) < 38:
        row = row + [''] * (38 - len(row))
    # Preserve expected numeric blanks as zeros for trailing derived slots when bootstrapping old 32-slot data.
    for idx in (32, 33, 34, 35, 37):
        if row[idx] in ('', None):
            row[idx] = 0
    if row[36] in (None,):
        row[36] = ''
    return row[:38]

def build_city_data(plans: list[list]):
    cities = defaultdict(lambda: {'plans': 0, 'assets': 0, 'participants': 0})
    for p in plans:
        city = str(p[2] or '').strip() or 'Unknown'
        cities[city]['plans'] += 1
        cities[city]['assets'] += safe_float(p[4])
        cities[city]['participants'] += safe_int(p[7])
    out = []
    for city_name, data in sorted(cities.items(), key=lambda x: (-x[1]['assets'], x[0])):
        lat, lon = CITY_COORDS.get(city_name, (32.7157, -117.1611))
        out.append({'n': city_name, 'la': lat, 'lo': lon, 'p': data['plans'], 'a': round(data['assets']), 'pt': data['participants']})
    return out


def build_top_providers(plans: list[list], limit: int = 15):
    counts = defaultdict(lambda: {'plans': 0, 'assets': 0})
    for p in plans:
        rk = clean_provider_name(p[18] if len(p) > 18 else '')
        if not rk:
            continue
        counts[rk]['plans'] += 1
        counts[rk]['assets'] += safe_float(p[4])
    return [[name, v['plans'], round(v['assets'] / 1_000_000)] for name, v in sorted(counts.items(), key=lambda kv: (-kv[1]['plans'], -kv[1]['assets'], kv[0]))[:limit]]


def infer_plan_type(chars: str) -> str:
    codes = {c.strip().upper() for c in str(chars or '').replace('|', ',').split(',') if c.strip()}
    if '2E' in codes:
        return '401(k) plan'
    if '2A' in codes:
        return 'Profit-sharing plan'
    return 'Retirement plan'


def infer_union_flag(row: dict, chars: str) -> int:
    explicit = str(get_field(row, 'COLL_BARGAIN_IND', 'COLLECTIVE_BARGAINING_IND', 'CB_ARRANGEMENT_IND')).strip().upper()
    if explicit in {'1', 'Y', 'YES', 'TRUE'}:
        return 1
    if explicit in {'0', 'N', 'NO', 'FALSE'}:
        return 0
    # Be conservative: only treat an explicit union/collective-bargaining characteristic as true.
    codes = {c.strip().upper() for c in str(chars or '').replace('|', ',').split(',') if c.strip()}
    return 1 if {'1X', '1C', '1B'} & codes else 0


def classify_filing_variant(source_variant: str, row: dict) -> str:
    if source_variant == '5500_SF':
        return '5500_SF'
    large_ind = str(get_field(row, 'LARGE_PLAN_FILER_IND', 'SMALL_PLAN_FILER_IND', 'PLAN_SIZE_IND')).upper()
    if large_ind in {'1', 'Y', 'YES', 'LARGE'}:
        return '5500_LARGE'
    if large_ind in {'0', 'N', 'NO', 'SMALL'}:
        return '5500_SMALL'
    participants = safe_int(get_field(row, 'TOT_PARTCP_EOY_CNT', 'PARTCP_ACCOUNT_BAL_CNT', 'PARTICIPANTS_EOY'))
    return '5500_LARGE' if participants >= 100 else '5500_SMALL'


def role_from_keywords(name: str) -> tuple[str, float]:
    hay = str(name or '').lower()
    best_role, best_score = '', 0.0
    for role, patterns in ROLE_KEYWORDS.items():
        hits = sum(1 for pattern in patterns if re.search(pattern, hay, re.I))
        if hits > best_score:
            best_role, best_score = role, float(hits)
    if best_score <= 0:
        return '', 0.0
    return best_role, min(0.6, 0.2 * best_score)


def collapse_provider_rows(rows: list[list]) -> list[dict]:
    grouped: dict[str, dict[str, Any]] = {}
    for raw_name, role, comp, source in rows:
        name = clean_provider_name(raw_name)
        if not name:
            continue
        key = name.lower()
        item = grouped.setdefault(key, {
            'name': name,
            'roles': Counter(),
            'comp': 0.0,
            'sources': set(),
            'explicit_role_hits': 0,
            'raw_names': set(),
        })
        item['roles'][role] += 1
        item['comp'] += safe_float(comp, 0.0) or 0.0
        item['sources'].add(source)
        item['raw_names'].add(raw_name)
        if source == 'service_code':
            item['explicit_role_hits'] += 1
    out = []
    for item in grouped.values():
        explicit_roles = [r for r, c in item['roles'].items() if r != 'OTH' and c > 0]
        role = ''
        confidence = 0.0
        role_source = ''
        if explicit_roles:
            role = sorted(explicit_roles, key=lambda r: (-item['roles'][r], r))[0]
            role_source = 'service_code'
            confidence = 0.95 if item['explicit_role_hits'] else 0.75
        else:
            role, confidence = role_from_keywords(item['name'])
            role_source = 'name_heuristic' if role else 'unclassified'
        out.append({
            'name': item['name'],
            'role': role or 'OTH',
            'role_source': role_source,
            'confidence': round(confidence, 2),
            'comp': round(item['comp']),
        })
    return sorted(out, key=lambda d: (-d['confidence'], -d['comp'], d['name']))


def provider_role_summary(raw_provider_rows: list[list], filing_variant: str) -> tuple[str, str, str, int, list[list], int]:
    collapsed = collapse_provider_rows(raw_provider_rows)

    def pick(role: str, min_conf: float) -> str:
        for item in collapsed:
            if item['role'] == role and item['confidence'] >= min_conf:
                return item['name']
        return ''

    rk = pick('RK', 0.75)
    tpa = pick('TPA', 0.75)
    advisor = pick('IA', 0.75) or pick('BD', 0.85)
    bundled_status = 2 if rk and tpa and rk == tpa else (1 if rk and tpa else 0)
    provider_confidence = 2 if any(item['confidence'] >= 0.9 for item in collapsed) else (1 if collapsed else 0)

    if filing_variant == '5500_SF' and not collapsed:
        bundled_status = 0

    exported = [[item['name'], item['role'], item['comp'], item['role_source'], item['confidence']] for item in collapsed]
    return rk, tpa, advisor, bundled_status, exported, provider_confidence


def score_plan_row(plan: list):
    assets = safe_float(plan[4])
    assets_boy = safe_float(plan[5])
    parts = safe_int(plan[7])
    parts_boy = safe_int(plan[17] if len(plan) > 17 else 0)
    avg_balance = safe_float(plan[23] if len(plan) > 23 else 0) or (round(assets / parts) if assets > 0 and parts > 0 else 0)
    growth = pct_change(assets, assets_boy) if assets_boy else safe_float(plan[6] if len(plan) > 6 else 0)
    chars = str(plan[8] if len(plan) > 8 else '')
    schedule_text = str(plan[36] if len(plan) > 36 else '')
    provider_confidence = safe_int(plan[37] if len(plan) > 37 else 0)
    signals = []
    score = 30

    if 1_000_000 <= assets <= 50_000_000:
        score += 15
    elif assets > 50_000_000:
        score += 10

    if 10 <= parts <= 300:
        score += 8
    elif parts > 300:
        score += 5

    if growth > 0:
        score += 10
    if growth >= 20:
        score += 5
        signals.append('HGR')
    if growth <= -5:
        score -= 3
        signals.append('DEC')
    if avg_balance >= 100_000:
        score += 5
        signals.append('HBL')
    if '2T' not in chars and '2S' not in chars:
        score += 5
        signals.append('NSH')

    # Only award provider-opportunity points when confidence is decent.
    if not (plan[20] if len(plan) > 20 else '') and provider_confidence >= 1:
        score += 10
        signals.append('NAD')
    elif not (plan[20] if len(plan) > 20 else '') and 'SF' not in schedule_text.upper():
        score += 6
        signals.append('NAD')

    if parts_boy and parts < parts_boy:
        signals.append('PSH')
    if len(plan) > 26 and safe_int(plan[26]):
        score -= 5
        signals.append('LTC')
    if len(plan) > 27 and safe_float(plan[27]) > 0:
        score -= 3
        signals.append('ESC')
    if 'C' in schedule_text.upper():
        signals.append('RSD')

    plan[6] = round(growth, 1)
    plan[11] = ','.join(dict.fromkeys([s for s in signals if s]))
    plan[12] = max(0, min(100, int(round(score))))
    if len(plan) > 23 and not plan[23] and avg_balance:
        plan[23] = int(avg_balance)
    return plan


def schedule_flags_for_row(filing_variant: str, row: dict, has_provider_schedule: bool, has_financial_schedule: bool) -> str:
    flags = set()
    if has_provider_schedule:
        flags.add('C')
    if filing_variant == '5500_SF':
        flags.add('I')
    elif has_financial_schedule:
        flags.add('H')

    insurance_ind = str(get_field(row, 'INSURANCE_IND', 'INSURANCE_CONTRACT_IND', 'FUNDING_INSURANCE_IND')).upper()
    if insurance_ind in {'1', 'Y', 'YES', 'TRUE'}:
        flags.add('A')
    return ','.join(sorted(flags))


def approximate_pdf_url(row: dict) -> str:
    explicit = get_field(row, 'FILING_PDF_URL', 'PUBLIC_FILE_URL', 'PUBLIC_DOCUMENT_URL')
    if explicit:
        return explicit
    # Deliberately do not fabricate a DOL URL from ACK_ID alone. Leave blank unless explicitly present.
    return ''


def completeness_score_for_candidate(candidate: dict) -> int:
    filing_variant = candidate['filing_variant']
    plan = candidate['row']
    score = 0
    for idx in (0, 1, 13, 14, 15, 16):
        if plan[idx] not in ('', None, 0):
            score += 3
    for idx in (4, 5, 7, 17):
        if safe_float(plan[idx], 0) > 0:
            score += 4
    if safe_int(plan[32], 0) > 0:
        score += 2
    if safe_float(plan[29], 0) > 0:
        score += 2
    if safe_float(plan[30], 0) > 0:
        score += 2
    if safe_float(plan[35], 0) > 0:
        score += 1
    if plan[18] or plan[19] or plan[20]:
        score += 2
    if filing_variant != '5500_SF' and 'C' in str(plan[36]):
        score += 1
    return score


def process_dol_data(data: dict, config: dict):
    provider_roles = config.get('provider_role_codes', {'16': 'RK', '15': 'TPA', '13': 'IA', '19': 'AUD', '14': 'CUS'})
    zip_prefixes = tuple(config.get('zip_prefixes', ['919', '920', '921']))
    providers_by_ack: dict[str, list[list]] = defaultdict(list)
    for row in data.get('sch_c_item1', []) + data.get('sch_c_item2', []):
        ack = get_field(row, 'ACK_ID', 'ack_id')
        if not ack:
            continue
        name = clean_provider_name(get_field(row, 'SERVICE_PROVIDER_NAME', 'SVP_NAME', 'NAME', 'service_provider_name'))
        if not name:
            continue
        explicit_role = provider_roles.get(get_field(row, 'SERVICE_CODE', 'SERVICE_CD', 'svc_cd'), 'OTH')
        comp = safe_float(get_field(row, 'COMPENSATION', 'DIRECT_COMPENSATION_AMT', 'INDIRECT_COMPENSATION_AMT'), 0.0)
        source = 'service_code' if explicit_role != 'OTH' else 'raw_provider'
        providers_by_ack[ack].append([name, explicit_role, round(comp), source])

    financials_by_ack: dict[str, dict[str, Any]] = {}
    financial_schedule_presence: dict[str, str] = {}
    for schedule_name, rows in (('H', data.get('sch_h', [])), ('I', data.get('sch_i', []))):
        for row in rows:
            ack = get_field(row, 'ACK_ID', 'ack_id')
            if not ack:
                continue
            base = financials_by_ack.setdefault(ack, {})
            base.update({
                'admin_expenses': safe_float(get_field(row, 'TOT_ADMIN_EXP', 'ADMIN_EXPENSES', 'TOT_PLAN_ADMIN_EXPENSES_AMT'), 0.0),
                'total_contributions': safe_float(get_field(row, 'TOT_CONTRIB', 'TOT_CONTRIB_AMT', 'TOT_EMPLR_EMPLE_CONT_AMT'), 0.0),
                'participant_loans': safe_float(get_field(row, 'PARTICIPANT_LOANS', 'PARTICIPANT_LOANS_AMT', 'LOANS_TO_PARTICIPANTS'), 0.0),
                'employer_securities': safe_float(get_field(row, 'EMPLR_SEC', 'EMPLOYER_SEC_AMT', 'EMPLR_SECURITIES_AMT'), 0.0),
                'total_expenses': safe_float(get_field(row, 'TOT_EXPENSES', 'TOT_EXPENSE_AMT'), 0.0),
                'employer_contributions': safe_float(get_field(row, 'EMPLR_CONTRIB', 'EMPLR_CONTRIB_AMT'), 0.0),
                'employee_contributions': safe_float(get_field(row, 'EMPLE_CONTRIB', 'PARTICP_CONTRIB_AMT', 'EMPLEE_CONTRIB_AMT'), 0.0),
            })
            financial_schedule_presence[ack] = schedule_name

    filing_rows = [('5500', r) for r in data.get('f5500', [])] + [('5500_SF', r) for r in data.get('f5500sf', [])]
    candidates = []
    selection_stats = Counter()
    variant_counts = Counter()

    for source_variant, row in filing_rows:
        zip_code = str(get_field(row, 'SPONS_DFE_MAIL_US_ZIP', 'SPONS_DFE_LOC_US_ZIP', 'ZIP_CODE'))
        if zip_prefixes and not zip_code.startswith(zip_prefixes):
            continue

        chars = get_field(row, 'PLAN_CHAR_CODES', 'PLAN_CHARACTERISTICS', 'PLAN_CHAR_FEATURES')
        char_codes = [c.strip().upper() for c in str(chars).replace('|', ',').split(',') if c.strip()]
        if any(code.startswith('4') for code in char_codes):
            continue

        ack = get_field(row, 'ACK_ID', 'ack_id')
        sponsor_ein = re.sub(r'\D', '', get_field(row, 'SPONS_DFE_EIN', 'EIN', 'SPONSOR_EIN'))
        plan_number_raw = get_field(row, 'PLAN_NUM', 'PLAN_NUMBER', 'PN')
        plan_number = str(plan_number_raw).strip().zfill(3) if plan_number_raw else ''
        plan_name = get_field(row, 'PLAN_NAME', 'PLAN_NAME_TXT')
        sponsor_name = get_field(row, 'SPONS_DFE_NAME', 'SPONSOR_NAME')
        city = get_field(row, 'SPONS_DFE_MAIL_US_CITY', 'SPONS_DFE_LOC_US_CITY', 'CITY')
        street = get_field(row, 'SPONS_DFE_MAIL_US_ADDRESS1', 'SPONS_DFE_LOC_US_ADDRESS1', 'ADDRESS1')
        assets_eoy = safe_float(get_field(row, 'TOT_ASSETS_EOY_AMT', 'TOT_NET_ASSETS_EOY_AMT', 'TOT_ASSETS_END'), 0.0)
        assets_boy = safe_float(get_field(row, 'TOT_ASSETS_BOY_AMT', 'TOT_NET_ASSETS_BOY_AMT', 'TOT_ASSETS_BEGIN'), 0.0)
        parts_eoy = safe_int(get_field(row, 'TOT_PARTCP_EOY_CNT', 'PARTCP_ACCOUNT_BAL_CNT', 'PARTICIPANTS_EOY'))
        parts_boy = safe_int(get_field(row, 'TOT_PARTCP_BOY_CNT', 'PARTICIPANTS_BOY'))
        eligible = safe_int(get_field(row, 'TOT_ELIGIBLE_PARTICIPANTS', 'ELIGIBLE_PARTICIPANTS_CNT', 'ELIG_PARTICIPANTS_CNT'))
        filing_date = parse_date(get_field(row, 'ACK_DATE', 'RECEIVED_DATE', 'PROCESSING_DATE'))
        pdf_url = approximate_pdf_url(row)
        filing_variant = classify_filing_variant(source_variant, row)
        variant_counts[filing_variant] += 1

        raw_providers = providers_by_ack.get(ack, [])
        rk, tpa, advisor, bundled_status, providers, provider_confidence = provider_role_summary(raw_providers, filing_variant)
        fin = financials_by_ack.get(ack, {})
        decoded = []
        for code in char_codes:
            feat = config.get('plan_code_features', {}).get(code)
            if feat and feat not in decoded:
                decoded.append(feat)
        avg_balance = int(round(assets_eoy / parts_eoy)) if assets_eoy > 0 and parts_eoy > 0 else 0
        expense_ratio = round(fin.get('admin_expenses', 0) / assets_eoy, 4) if assets_eoy > 0 and fin.get('admin_expenses', 0) > 0 else 0
        schedule_text = schedule_flags_for_row(
            filing_variant,
            row,
            has_provider_schedule=bool(raw_providers),
            has_financial_schedule=ack in financial_schedule_presence,
        )

        insurance_indicator = 1 if 'A' in schedule_text else 0
        plan = [
            plan_name, sponsor_name, city, zip_code[:5], round(assets_eoy), round(assets_boy), pct_change(assets_eoy, assets_boy),
            parts_eoy, ','.join(char_codes), infer_plan_type(chars), infer_union_flag(row, chars), '', 0, pdf_url, sponsor_ein,
            filing_date, street, parts_boy, rk, tpa, advisor, providers, '|'.join(decoded), avg_balance, bundled_status,
            0, 0, round(fin.get('employer_securities', 0)), round(fin.get('participant_loans', 0)), round(fin.get('admin_expenses', 0)),
            round(fin.get('total_contributions', 0)), expense_ratio, eligible, round(fin.get('employer_contributions', 0)),
            round(fin.get('employee_contributions', 0)), round(fin.get('total_expenses', 0)), schedule_text, insurance_indicator
        ]

        if plan_name and sponsor_name and sponsor_ein and plan_number:
            plan = score_plan_row(plan)
            plan_year = parse_year(row, 'PLAN_YEAR_BEGIN_DATE', 'PLAN_YEAR', 'PLAN_YEAR_BEGIN')
            amended_flag = str(get_field(row, 'AMENDED_RETURN_IND', 'AMENDED_IND')).upper() in {'1', 'Y', 'YES', 'TRUE'}
            candidate = {
                'dedupe_key': f"{sponsor_ein}::{plan_number}",
                'plan_key': f"{sponsor_ein}::{plan_number}::{plan_year or 0}",
                'plan_number': plan_number,
                'plan_year': plan_year,
                'filing_date': filing_date or '',
                'filing_variant': filing_variant,
                'amended_flag': amended_flag,
                'provider_confidence': provider_confidence,
                'row': plan,
            }
            candidate['completeness'] = completeness_score_for_candidate(candidate)
            candidates.append(candidate)

    by_dedupe = defaultdict(list)
    for candidate in candidates:
        by_dedupe[candidate['dedupe_key']].append(candidate)

    deduped = {}
    selection_reasons = Counter()
    for key, group in by_dedupe.items():
        def candidate_rank(c: dict):
            return (
                c['plan_year'],
                1 if c['amended_flag'] else 0,
                c['completeness'],
                c['filing_date'],
                safe_int(c['row'][12]),
                safe_float(c['row'][4]),
                c['plan_key'],
            )

        best = sorted(group, key=candidate_rank, reverse=True)[0]
        deduped[key] = best
        if len(group) > 1:
            selection_stats['multi_filing_plans'] += 1
            years = {g['plan_year'] for g in group}
            if len(years) > 1:
                selection_reasons['newer_plan_year'] += 1
            elif any(g['amended_flag'] for g in group) and best['amended_flag']:
                selection_reasons['amended_preferred'] += 1
            else:
                selection_reasons['completeness_or_recency'] += 1

    plans = sorted([c['row'] for c in deduped.values()], key=lambda p: (-safe_int(p[12]), -safe_float(p[4]), p[0]))
    process_stats = {
        'input_candidate_count': len(candidates),
        'selected_plan_count': len(plans),
        'variant_counts': dict(variant_counts),
        'selection_summary': dict(selection_stats),
        'selection_reasons': dict(selection_reasons),
        'deduped_away_count': len(candidates) - len(plans),
    }
    return plans, process_stats


def validate_dataset(dataset: dict) -> dict:
    plans = dataset.get('plans', [])
    warnings = []
    errors = []
    scores = [p[12] for p in plans if isinstance(p, list) and len(p) > 12 and isinstance(p[12], (int, float))]
    if not plans:
        errors.append({'type': 'empty_dataset', 'message': 'No plan rows were produced.'})
    bad_rows = [i for i, p in enumerate(plans) if not isinstance(p, list) or len(p) != 38]
    if bad_rows:
        errors.append({'type': 'row_length', 'count': len(bad_rows), 'expected': 38})

    duplicate_keys = set()
    seen = set()
    for p in plans:
        if not isinstance(p, list) or len(p) < 15:
            continue
        key = f"{p[14]}::{p[0]}::{p[15]}"
        if key in seen:
            duplicate_keys.add(key)
        seen.add(key)
    if duplicate_keys:
        errors.append({'type': 'duplicate_export_keys', 'count': len(duplicate_keys)})

    variant_counts = Counter()
    missing_pdf = 0
    missing_rk = 0
    extreme_growth = 0
    negative_assets = 0
    zero_parts_with_assets = 0
    high_expense_ratio = 0
    sf_missing_providers = 0
    full_missing_providers = 0
    signals_counter = Counter()

    for p in plans:
        schedule_text = str(p[36] or '').upper()
        variant = '5500_SF' if 'I' in schedule_text and 'H' not in schedule_text else ('5500_FULL' if 'H' in schedule_text else 'UNKNOWN')
        variant_counts[variant] += 1
        if not p[13]:
            missing_pdf += 1
        if not p[18]:
            missing_rk += 1
            if variant == '5500_SF':
                sf_missing_providers += 1
            elif variant == '5500_FULL':
                full_missing_providers += 1
        if isinstance(p[6], (int, float)) and (p[6] > 200 or p[6] < -80):
            extreme_growth += 1
        if safe_float(p[4], 0) < 0:
            negative_assets += 1
        if safe_float(p[4], 0) > 0 and safe_int(p[7], 0) == 0:
            zero_parts_with_assets += 1
        ratio = safe_float(p[31], 0)
        if ratio > 0.05:
            high_expense_ratio += 1
        for sig in [s for s in str(p[11] or '').split(',') if s]:
            signals_counter[sig] += 1

    if missing_pdf:
        warnings.append({'type': 'missing_pdf_url', 'count': missing_pdf})
    if missing_rk:
        warnings.append({'type': 'missing_recordkeeper', 'count': missing_rk, 'sf_count': sf_missing_providers, 'full_count': full_missing_providers})
    if extreme_growth:
        warnings.append({'type': 'extreme_asset_growth', 'count': extreme_growth})
    if high_expense_ratio:
        warnings.append({'type': 'elevated_admin_expense_ratio_rows', 'count': high_expense_ratio})
    if negative_assets:
        errors.append({'type': 'negative_assets_eoy', 'count': negative_assets})
    if zero_parts_with_assets:
        errors.append({'type': 'zero_participants_with_assets', 'count': zero_parts_with_assets})

    process_stats = dataset.get('meta', {}).get('process_stats', {})
    return {
        'row_count': len(plans),
        'city_count': len(dataset.get('cities', [])),
        'variant_counts': dict(variant_counts),
        'errors': errors,
        'warnings': warnings,
        'dataset_schema': {
            'site_row_length_min': min((len(p) for p in plans), default=0),
            'site_row_length_max': max((len(p) for p in plans), default=0),
        },
        'score_distribution': {
            'min': min(scores) if scores else None,
            'max': max(scores) if scores else None,
            'mean': round(statistics.fmean(scores), 2) if scores else None,
            'median': statistics.median(scores) if scores else None,
        },
        'selection_summary': process_stats.get('selection_summary', {}),
        'selection_reasons': process_stats.get('selection_reasons', {}),
        'deduped_away_count': process_stats.get('deduped_away_count', 0),
        'signal_frequency_top10': dict(signals_counter.most_common(10)),
    }


def write_outputs(dataset: dict, validation: dict):
    DATASET_JSON.write_text(json.dumps(dataset, separators=(',', ':')), encoding='utf-8')
    VALIDATION_JSON.write_text(json.dumps(validation, indent=2), encoding='utf-8')
    status = 'PASS' if not validation.get('errors') else 'FAIL'
    SUMMARY_TXT.write_text(textwrap.dedent(f"""\
    RUN SUMMARY
    -----------
    Status: {status}
    Rows: {validation.get('row_count', 0):,}
    Cities: {validation.get('city_count', 0):,}

    Filing Mix:
    - 5500 full-like rows: {validation.get('variant_counts', {}).get('5500_FULL', 0):,}
    - 5500-SF-like rows: {validation.get('variant_counts', {}).get('5500_SF', 0):,}
    - Unknown mix: {validation.get('variant_counts', {}).get('UNKNOWN', 0):,}

    Selection:
    - Deduped-away candidates: {validation.get('deduped_away_count', 0):,}
    - Multi-filing plans: {validation.get('selection_summary', {}).get('multi_filing_plans', 0):,}
    - Newer plan year wins: {validation.get('selection_reasons', {}).get('newer_plan_year', 0):,}
    - Amended filing wins: {validation.get('selection_reasons', {}).get('amended_preferred', 0):,}
    - Completeness/recency wins: {validation.get('selection_reasons', {}).get('completeness_or_recency', 0):,}

    Validation:
    - Hard errors: {len(validation.get('errors', []))}
    - Warnings: {len(validation.get('warnings', []))}

    Score Distribution:
    - Min: {validation.get('score_distribution', {}).get('min')}
    - Mean: {validation.get('score_distribution', {}).get('mean')}
    - Median: {validation.get('score_distribution', {}).get('median')}
    - Max: {validation.get('score_distribution', {}).get('max')}
    """), encoding='utf-8')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-from-index', type=Path, nargs='?', const=INDEX_HTML, default=None)
    parser.add_argument('--zip', type=Path)
    parser.add_argument('--dir', type=Path)
    args = parser.parse_args()
    config = load_config()

    if args.bootstrap_from_index is not None:
        plans, cities, tp = extract_current_embedded_data(args.bootstrap_from_index)
        if not plans and DATASET_JSON.exists():
            existing = json.loads(DATASET_JSON.read_text(encoding='utf-8'))
            plans = existing.get('plans', [])
            cities = existing.get('cities', [])
            tp = existing.get('top_providers', [])
        plans = [normalize_site_row(p) for p in plans]
        dataset = {
            'meta': {
                'region_name': config.get('region_name', 'San Diego County'),
                'filing_year': config.get('filing_year', '2024'),
                'plan_count': len(plans),
                'city_count': len(cities),
                'generated_from': 'index_embedded_dataset',
                'row_schema_version': 'v2-json-export',
                'process_stats': {
                    'input_candidate_count': len(plans),
                    'selected_plan_count': len(plans),
                    'variant_counts': {},
                    'selection_summary': {},
                    'selection_reasons': {},
                    'deduped_away_count': 0,
                },
            },
            'plans': plans,
            'cities': cities,
            'top_providers': tp,
        }
    else:
        ensure_index_uses_json()
        if args.zip:
            zf = zipfile.ZipFile(args.zip)
            reader = lambda pattern: read_csv_from_zip(zf, pattern)
        elif args.dir:
            base = Path(args.dir)
            reader = lambda pattern: read_csv_from_dir(base, pattern)
        else:
            raise SystemExit('Provide --bootstrap-from-index, --zip, or --dir')
        year = config.get('filing_year', '2024')
        raw = {
            'f5500': reader(rf'f_5500_{year}') or reader(r'f_5500'),
            'f5500sf': reader(rf'f_5500_sf_{year}') or reader(r'f_5500_sf'),
            'sch_c_item1': reader(rf'f_sch_c_part1_item1_{year}') or reader(r'f_sch_c_part1_item1'),
            'sch_c_item2': reader(rf'f_sch_c_part1_item2_{year}') or reader(r'f_sch_c_part1_item2'),
            'sch_h': reader(rf'f_sch_h_{year}') or reader(r'f_sch_h'),
            'sch_i': reader(rf'f_sch_i_{year}') or reader(r'f_sch_i'),
        }
        plans, process_stats = process_dol_data(raw, config)
        plans = [normalize_site_row(p) for p in plans]
        plans = [normalize_site_row(p) for p in plans]
        dataset = {
            'meta': {
                'region_name': config.get('region_name', 'San Diego County'),
                'filing_year': year,
                'plan_count': len(plans),
                'city_count': 0,
                'generated_from': 'dol_pipeline',
                'row_schema_version': 'v2-json-export',
                'process_stats': process_stats,
            },
            'plans': plans,
            'cities': build_city_data(plans),
            'top_providers': build_top_providers(plans),
        }
        dataset['meta']['city_count'] = len(dataset['cities'])

    validation = validate_dataset(dataset)
    write_outputs(dataset, validation)
    print(f'Wrote {DATASET_JSON.name}, {VALIDATION_JSON.name}, and {SUMMARY_TXT.name}')


if __name__ == '__main__':
    main()
