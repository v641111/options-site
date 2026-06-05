#!/usr/bin/env python3
"""
Fetch live A-share ETF option chain data from:
  - Eastmoney slist API: contract list (codes, strikes)
  - Sina hq API:        bid/ask/OI/volume + Greeks per contract

Writes per-underlying JSON to data/<symbol>.json for the frontend to read.

Sina CON_OP_<code> field positions (51 fields):
  [1]=bid, [2]=last, [3]=ask, [5]=OI, [7]=strike,
  [32]=quoteTime, [37]=name, [41]=volume,
  [45]=type(P/C), [46]=expiryDate(YYYY-MM-DD), [47]=daysToExpiry

Sina CON_SO_<code> field positions (17 fields):
  [5]=delta, [6]=gamma, [7]=theta, [8]=vega
"""
import json, re, sys, time, math, os
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data"

# Beijing time
BJ = timezone(timedelta(hours=8))

UNDERLYINGS = [
    {"code": "510050", "secid": "1.510050", "name": "上证50ETF华夏",       "short": "50ETF"},
    {"code": "510300", "secid": "1.510300", "name": "沪深300ETF华泰柏瑞",   "short": "300ETF沪"},
    {"code": "510500", "secid": "1.510500", "name": "中证500ETF南方",       "short": "500ETF沪"},
    {"code": "588000", "secid": "1.588000", "name": "科创50ETF华夏",        "short": "科创50华夏"},
    {"code": "588080", "secid": "1.588080", "name": "科创50ETF易方达",      "short": "科创50易"},
    {"code": "159901", "secid": "0.159901", "name": "深证100ETF易方达",     "short": "深100ETF"},
    {"code": "159915", "secid": "0.159915", "name": "创业板ETF易方达",       "short": "创业板ETF"},
    {"code": "159919", "secid": "0.159919", "name": "沪深300ETF嘉实",       "short": "300ETF深"},
    {"code": "159922", "secid": "0.159922", "name": "中证500ETF嘉实",       "short": "500ETF深"},
]

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
SINA_HEADERS = {"User-Agent": UA, "Referer": "https://finance.sina.com.cn"}
EM_HEADERS = {"User-Agent": UA, "Referer": "https://quote.eastmoney.com/"}

RISK_FREE_RATE = 0.018  # 1Y CGB approx


# ─── Black-Scholes IV (bisection) ──────────────────────────
def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_price(S, K, T, r, sigma, is_put):
    if T <= 0 or sigma <= 0:
        return max(K - S, 0) if is_put else max(S - K, 0)
    sqrt_t = math.sqrt(T)
    d1 = (math.log(S / K) + (r + sigma * sigma / 2) * T) / (sigma * sqrt_t)
    d2 = d1 - sigma * sqrt_t
    if is_put:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)
    return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)


def implied_vol(market, S, K, T, r, is_put):
    if not (market > 0 and S > 0 and K > 0 and T > 0):
        return None
    intrinsic = max(K - S, 0) if is_put else max(S - K, 0)
    if market < intrinsic - 0.001:
        return None
    lo, hi = 0.001, 5.0
    for _ in range(60):
        mid = (lo + hi) / 2
        p = bs_price(S, K, T, r, mid, is_put)
        if abs(p - market) < 1e-5:
            return mid
        if p < market:
            lo = mid
        else:
            hi = mid
    iv = (lo + hi) / 2
    if iv < 0.005 or iv > 4.99:
        return None
    return iv


# ─── Fetch contract chain from Eastmoney ───────────────────
def fetch_chain(secid, spt):
    """spt=9 calls, spt=10 puts. Returns list of contract codes/names/strikes."""
    url = (
        f"https://push2.eastmoney.com/api/qt/slist/get"
        f"?secid={secid}&pi=0&pz=400&po=0&spt={spt}&fltt=2&invt=2"
        f"&fid=f161&fields=f12,f14,f161,f250,f334&_={int(time.time() * 1000)}"
    )
    req = urllib.request.Request(url, headers=EM_HEADERS)
    with urllib.request.urlopen(req, timeout=15) as resp:
        j = json.loads(resp.read())
    diff = (j.get("data") or {}).get("diff") or {}
    out = []
    for _, v in diff.items():
        code = v.get("f12")
        if not code:
            continue
        out.append({
            "code": code,
            "name": v.get("f14"),
            "strike": v.get("f161"),
            "moneyness": v.get("f250"),
            "underlying_px": v.get("f334"),
        })
    return out


# ─── Fetch live quotes + greeks from Sina ──────────────────
SINA_OP_RE = re.compile(r'hq_str_CON_OP_(\d+)="([^"]+)"')
SINA_SO_RE = re.compile(r'hq_str_CON_SO_(\d+)="([^"]+)"')


def fetch_sina_batch(codes):
    """Batch fetch CON_OP and CON_SO for multiple contracts."""
    syms = []
    for c in codes:
        syms.append(f"CON_OP_{c}")
        syms.append(f"CON_SO_{c}")
    # Sina rejects very long URLs; chunk by 80 contracts (160 syms) per request
    out_op = {}
    out_so = {}
    CHUNK = 80
    for i in range(0, len(codes), CHUNK):
        sub = syms[i * 2 : (i + CHUNK) * 2]
        url = "https://hq.sinajs.cn/list=" + ",".join(sub)
        req = urllib.request.Request(url, headers=SINA_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                text = resp.read().decode("gbk", errors="replace")
        except Exception as e:
            print(f"  sina batch chunk {i} ERR: {e}")
            continue
        for m in SINA_OP_RE.finditer(text):
            out_op[m.group(1)] = m.group(2).split(",")
        for m in SINA_SO_RE.finditer(text):
            out_so[m.group(1)] = m.group(2).split(",")
    return out_op, out_so


def parse_op(parts):
    """Parse CON_OP_<code> response."""
    try:
        return {
            "bidSize": int(parts[0]) if parts[0] else 0,
            "bid":     float(parts[1]) if parts[1] else 0,
            "last":    float(parts[2]) if parts[2] else 0,
            "ask":     float(parts[3]) if parts[3] else 0,
            "askSize": int(parts[4]) if parts[4] else 0,
            "openInterest": int(parts[5]) if parts[5] else 0,
            "changePct":   float(parts[6]) if parts[6] else 0,
            "strike":  float(parts[7]) if parts[7] else 0,
            "prevSettle": float(parts[8]) if parts[8] else 0,
            "prevClose":  float(parts[9]) if parts[9] else 0,
            "open":    float(parts[10]) if parts[10] else 0,
            "change":  float(parts[11]) if parts[11] else 0,
            "quoteTime": parts[32] if len(parts) > 32 else "",
            "underlyingCode": parts[36] if len(parts) > 36 else "",
            "name":    parts[37] if len(parts) > 37 else "",
            "amplitude": float(parts[38]) if len(parts) > 38 and parts[38] else 0,
            "high":    float(parts[39]) if len(parts) > 39 and parts[39] else 0,
            "low":     float(parts[40]) if len(parts) > 40 and parts[40] else 0,
            "volume":  int(parts[41]) if len(parts) > 41 and parts[41] else 0,
            "amount":  float(parts[42]) if len(parts) > 42 and parts[42] else 0,
            "optType": parts[45] if len(parts) > 45 else "",   # P/C
            "expiry":  parts[46] if len(parts) > 46 else "",   # YYYY-MM-DD
            "days":    int(parts[47]) if len(parts) > 47 and parts[47] else 0,
        }
    except (ValueError, IndexError) as e:
        return None


def parse_so(parts):
    """Parse CON_SO_<code> response: Greeks."""
    try:
        def f(i):
            if i < len(parts) and parts[i]:
                try: return float(parts[i])
                except: return None
            return None
        return {
            "delta": f(5),
            "gamma": f(6),
            "theta": f(7),
            "vega":  f(8),
            # field 9 unreliable as IV — we self-compute
        }
    except Exception:
        return None


# ─── Fetch underlying ETF spot ─────────────────────────────
def fetch_underlying_spot(code):
    """Sina: var hq_str_sz159901='...,price,...,...'"""
    prefix = "sh" if code.startswith("5") else "sz"
    url = f"https://hq.sinajs.cn/list={prefix}{code}"
    req = urllib.request.Request(url, headers=SINA_HEADERS)
    with urllib.request.urlopen(req, timeout=10) as resp:
        text = resp.read().decode("gbk", errors="replace")
    m = re.search(r'"([^"]+)"', text)
    if not m:
        return None
    parts = m.group(1).split(",")
    # ETF format: name,open,prevclose,now,high,low,...,date,time
    if len(parts) < 4:
        return None
    try:
        return {
            "price": float(parts[3]),
            "prevClose": float(parts[2]),
            "open": float(parts[1]),
            "high": float(parts[4]) if len(parts) > 4 else 0,
            "low":  float(parts[5]) if len(parts) > 5 else 0,
            "name": parts[0],
        }
    except (ValueError, IndexError):
        return None


# ─── Main per-underlying scrape ────────────────────────────
def scrape_underlying(under):
    print(f"\n── {under['code']} {under['name']} ──")

    # 1. Chain
    try:
        calls = fetch_chain(under["secid"], 9)
        puts  = fetch_chain(under["secid"], 10)
    except Exception as e:
        print(f"  chain ERR: {e}")
        return None
    contracts = calls + puts
    print(f"  chain: {len(calls)} calls, {len(puts)} puts ({len(contracts)} total)")
    if not contracts:
        return None

    # 2. Sina batch quote + greeks
    codes = [c["code"] for c in contracts]
    op_data, so_data = fetch_sina_batch(codes)
    print(f"  sina: {len(op_data)} OP quotes, {len(so_data)} SO greeks")

    # 3. Underlying spot
    spot = fetch_underlying_spot(under["code"])
    if spot:
        print(f"  spot: {spot['price']} ({spot['name']})")
    else:
        print("  spot fetch failed")
        spot = {"price": 0, "name": under["name"]}

    S = spot["price"]

    # 4. Compose contract list with computed IV
    result = []
    for c in contracts:
        op = op_data.get(c["code"])
        so = so_data.get(c["code"])
        if not op:
            continue  # No quote — skip
        parsed_op = parse_op(op)
        if not parsed_op:
            continue
        parsed_so = parse_so(so) if so else None

        # Determine optType: prefer Sina's field
        opt_type = parsed_op.get("optType") or ""
        is_put = (opt_type == "P") or ("沽" in (parsed_op.get("name") or ""))

        # Compute IV using BS (use mid if both sides, else last)
        bid = parsed_op["bid"]; ask = parsed_op["ask"]; last = parsed_op["last"]
        iv_input_px = (bid + ask) / 2 if (bid > 0 and ask > 0) else (last if last > 0 else None)
        days = parsed_op["days"]
        iv = None
        if iv_input_px and days > 0 and S > 0:
            iv = implied_vol(iv_input_px, S, parsed_op["strike"], days / 365, RISK_FREE_RATE, is_put)

        # Liquidity flag: illiquid if bid or ask is 0
        illiq = (bid <= 0 or ask <= 0)

        result.append({
            "code": c["code"],
            "name": parsed_op["name"] or c["name"],
            "strike": parsed_op["strike"] or c["strike"],
            "type": "P" if is_put else "C",
            "expiry": parsed_op["expiry"],
            "days": days,
            "bid": bid, "ask": ask, "last": last,
            "bidSize": parsed_op["bidSize"],
            "askSize": parsed_op["askSize"],
            "volume": parsed_op["volume"],
            "openInterest": parsed_op["openInterest"],
            "changePct": parsed_op["changePct"],
            "high": parsed_op["high"], "low": parsed_op["low"],
            "iv": round(iv, 4) if iv is not None else None,
            "delta": parsed_so["delta"] if parsed_so else None,
            "gamma": parsed_so["gamma"] if parsed_so else None,
            "theta": parsed_so["theta"] if parsed_so else None,
            "vega":  parsed_so["vega"] if parsed_so else None,
            "illiq": illiq,
            "quoteTime": parsed_op["quoteTime"],
        })

    n_puts = sum(1 for r in result if r["type"] == "P")
    n_calls = sum(1 for r in result if r["type"] == "C")
    n_iv_ok = sum(1 for r in result if r["iv"] is not None)
    n_illiq = sum(1 for r in result if r["illiq"])
    print(f"  composed: {n_calls} calls, {n_puts} puts, IV ok {n_iv_ok}, illiq {n_illiq}")

    return {
        "_meta": {
            "underlying": under,
            "spot": spot,
            "updated": datetime.now(BJ).isoformat(timespec="seconds"),
            "riskFree": RISK_FREE_RATE,
            "counts": {
                "total": len(result),
                "calls": n_calls,
                "puts":  n_puts,
                "iv_computed": n_iv_ok,
                "illiquid": n_illiq,
            },
        },
        "contracts": result,
    }


# ─── Main ──────────────────────────────────────────────────
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    index_meta = []

    for under in UNDERLYINGS:
        data = scrape_underlying(under)
        if not data:
            continue
        out_path = OUT_DIR / f"{under['code']}.json"
        out_path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        print(f"  -> {out_path.name} ({out_path.stat().st_size} bytes)")
        index_meta.append({
            "code": under["code"],
            "name": under["name"],
            "short": under["short"],
            "secid": under["secid"],
            "spot": data["_meta"]["spot"],
            "counts": data["_meta"]["counts"],
        })

    # Write index manifest
    idx_path = OUT_DIR / "index.json"
    idx_path.write_text(
        json.dumps({
            "updated": datetime.now(BJ).isoformat(timespec="seconds"),
            "underlyings": index_meta,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n✓ index.json: {len(index_meta)} underlyings")


if __name__ == "__main__":
    main()
