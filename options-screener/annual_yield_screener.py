"""
Cash-secured put screener: last completed US session as-of date,
annualized writer yield (premium / strike) * (365 / DTE) in [8%, 18%],
optional minimum estimated win rate (default 85%),
and option premium from contract daily **close** on that session (Yahoo history),
with chain mid fallback when no bar exists.

Combines ideas from:
- Discount-band OTM puts (QQQ&SPY style)
- Mid-quote pricing, optional IV + assignment-proximity "win rate" (my_screener style)

Outputs a self-contained HTML report (sortable table) and optional CSV.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import webbrowser
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.optimize import brentq
from scipy.stats import norm

R_FREE_DEFAULT = 0.042


def _ny_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="America/New_York")


def last_completed_session_date(ny: Optional[pd.Timestamp] = None) -> date:
    """
    Calendar date of the last *finished* regular session (4pm ET cutoff).
    Weekends/holidays: pandas BDay steps back from session end; holidays are
    approximated (no exchange calendar) — good enough for retail yfinance use.
    """
    ny = ny or _ny_now()
    day = ny.normalize()
    if ny.time() < time(16, 0):
        asof = (day - pd.tseries.offsets.BDay(1)).date()
    else:
        asof = day.date()
    # If asof lands on weekend (rare with BDay), step back.
    while asof.weekday() >= 5:
        asof = (pd.Timestamp(asof) - pd.tseries.offsets.BDay(1)).date()
    return asof


def close_on_or_before(ticker: yf.Ticker, session: date) -> float:
    """Daily close on `session` if present; else last available close on/before."""
    start = pd.Timestamp(session) - pd.Timedelta(days=20)
    end = pd.Timestamp(session) + pd.Timedelta(days=5)
    hist = ticker.history(start=start, end=end, auto_adjust=True, raise_errors=False)
    if hist is None or hist.empty:
        hist = ticker.history(period="1y", auto_adjust=True, raise_errors=False)
    if hist is None or hist.empty:
        raise RuntimeError("no price history")
    idx = pd.DatetimeIndex(pd.to_datetime(hist.index, utc=False))
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    idx = idx.normalize()
    h = hist.copy()
    h.index = idx.date
    target = session
    if target in h.index:
        return float(h.loc[target, "Close"])
    prior = h[h.index <= target]
    if prior.empty:
        return float(h["Close"].iloc[0])
    return float(prior["Close"].iloc[-1])


def dte_days(expiry: date, asof: date) -> int:
    return (expiry - asof).days


def option_session_close(contract: str, session: date) -> Optional[float]:
    """
    Prior-session option close: daily bar Close for `session` from Yahoo's
    contract-level history (yf.Ticker(OSI symbol).history). Returns None if
    missing or zero (caller may fall back to chain mid).
    """
    c = (contract or "").strip()
    if not c:
        return None
    start = session - timedelta(days=35)
    end = session + timedelta(days=4)
    kwargs = {"start": str(start), "end": str(end), "auto_adjust": True}
    try:
        h = yf.Ticker(c).history(**kwargs, raise_errors=False)
    except TypeError:
        h = yf.Ticker(c).history(**kwargs)
    except Exception:
        return None
    if h is None or h.empty or "Close" not in h.columns:
        return None
    idx = pd.DatetimeIndex(pd.to_datetime(h.index, utc=False))
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC").tz_localize(None)
    idx = idx.normalize()
    hh = h.copy()
    hh.index = idx.date
    if session in hh.index:
        px = float(hh.loc[session, "Close"])
    else:
        sub = hh[hh.index <= session]
        if sub.empty:
            return None
        px = float(sub["Close"].iloc[-1])
    if px <= 0 or (isinstance(px, float) and np.isnan(px)):
        return None
    return px


def _fetch_option_closes(
    contracts: list[str], session: date, max_workers: int
) -> dict[str, Optional[float]]:
    uniq = list(dict.fromkeys(c for c in contracts if c and str(c).strip()))
    if not uniq:
        return {}

    def job(sym: str) -> tuple[str, Optional[float]]:
        return sym, option_session_close(sym, session)

    out: dict[str, Optional[float]] = {}
    workers = max(1, int(max_workers))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for sym, px in ex.map(job, uniq):
            out[sym] = px
    return out


def _safe_int(x: object) -> int:
    try:
        v = float(x)  # type: ignore[arg-type]
        if np.isnan(v):
            return 0
        return int(v)
    except (TypeError, ValueError):
        return 0


def implied_vol_put(price: float, S: float, K: float, T: float, r: float) -> float:
    """European put IV via Brentq; robust fallback."""
    if price <= 0 or S <= 0 or K <= 0 or T <= 1e-6:
        return 0.25

    def f(sig: float) -> float:
        sig = max(sig, 1e-6)
        d1 = (np.log(S / K) + (r + 0.5 * sig**2) * T) / (sig * np.sqrt(T))
        d2 = d1 - sig * np.sqrt(T)
        put = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        return put - price

    try:
        return float(brentq(f, 1e-4, 5.0, maxiter=80))
    except ValueError:
        try:
            return float(brentq(f, 1e-4, 8.0, maxiter=120))
        except Exception:
            return 0.25


def otm_put_win_rate(S: float, K: float, T: float, sigma: float, r: float) -> float:
    """
    Same GBM approximation as my_screener: risk-neutral prob. mass associated
    with finishing at/above strike for an OTM put (K < S). Used only for ranking.
    """
    if T <= 1e-6 or sigma <= 1e-6:
        return 0.5
    return float(
        norm.cdf(
            (np.log(S / K) + (r - 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        )
    )


@dataclass
class ScreenConfig:
    tickers: list[str]
    ann_low: float
    ann_high: float
    dte_min: int
    dte_max: int
    min_discount: float  # min (1 - K/S), put OTM depth; 0 disables
    max_discount: float  # max (1 - K/S); set high to disable upper cap
    min_premium: float
    min_open_interest: int
    min_volume: int
    r_free: float
    include_iv: bool
    min_win_est: float  # 0 disables; else require otm_put_win_rate >= this
    option_price_mode: str  # "session_close" | "chain_mid"
    option_price_workers: int


def _parse_tickers(s: str) -> list[str]:
    parts = re.split(r"[\s,;]+", s.strip())
    return [p.upper() for p in parts if p]


def screen_puts(cfg: ScreenConfig, asof: date) -> pd.DataFrame:
    rows: list[dict] = []

    for sym in cfg.tickers:
        tk = yf.Ticker(sym)
        try:
            S = close_on_or_before(tk, asof)
        except Exception:
            continue

        try:
            expiries: Iterable[str] = tk.options or []
        except Exception:
            continue

        for exp in expiries:
            try:
                exp_d = datetime.strptime(exp, "%Y-%m-%d").date()
            except ValueError:
                continue
            dte = dte_days(exp_d, asof)
            if dte < cfg.dte_min or dte > cfg.dte_max:
                continue

            try:
                puts = tk.option_chain(exp).puts
            except Exception:
                continue
            if puts is None or puts.empty:
                continue

            p = puts.copy()
            strike = p["strike"].astype(float)
            disc = 1.0 - (strike / S)
            mask = strike < S
            if cfg.min_discount > 0:
                mask &= disc >= cfg.min_discount
            if cfg.max_discount < 0.99:
                mask &= disc <= cfg.max_discount
            p = p[mask]
            if p.empty:
                continue

            oi = pd.to_numeric(p.get("openInterest", 0), errors="coerce").fillna(0)
            vol = pd.to_numeric(p.get("volume", 0), errors="coerce").fillna(0)
            p = p[(oi >= cfg.min_open_interest) & (vol >= cfg.min_volume)]
            if p.empty:
                continue

            bid = pd.to_numeric(p.get("bid", 0), errors="coerce").fillna(0.0)
            ask = pd.to_numeric(p.get("ask", 0), errors="coerce").fillna(0.0)
            last = pd.to_numeric(p.get("lastPrice", 0), errors="coerce").fillna(0.0)
            chain_mid = np.where((bid > 0) & (ask > 0), (bid + ask) / 2.0, last)

            if cfg.option_price_mode == "chain_mid":
                p["premium"] = chain_mid
                p["price_source"] = "chain_mid"
            else:
                sym_col = p.get("contractSymbol")
                if sym_col is None:
                    continue
                contracts = sym_col.astype(str).tolist()
                close_map = _fetch_option_closes(
                    contracts, asof, cfg.option_price_workers
                )
                hist_px = pd.to_numeric(
                    pd.Series(contracts, index=p.index, dtype=object).map(close_map),
                    errors="coerce",
                )
                hist_ok = hist_px.notna() & (hist_px > 0)
                chain_s = pd.Series(chain_mid, index=p.index, dtype=float)
                p["premium"] = hist_px.where(hist_ok, chain_s)
                p["price_source"] = np.where(
                    hist_ok.to_numpy(),
                    "上一交易日收盘",
                    "chain(无Bar回退)",
                )

            p = p[pd.to_numeric(p["premium"], errors="coerce").fillna(0) >= cfg.min_premium]
            if p.empty:
                continue

            T = dte / 365.0
            strike_f = p["strike"].astype(float)
            ann = (p["premium"] / strike_f) * (365.0 / dte)
            keep = (ann >= cfg.ann_low) & (ann <= cfg.ann_high)
            p = p[keep]
            if p.empty:
                continue

            need_metrics = cfg.include_iv or cfg.min_win_est > 0
            for _, row in p.iterrows():
                K = float(row["strike"])
                prem = float(row["premium"])
                iv: Optional[float] = None
                win_est: Optional[float] = None
                if need_metrics and T > 0:
                    iv_f = implied_vol_put(prem, float(S), K, float(T), cfg.r_free)
                    win_f = otm_put_win_rate(float(S), K, float(T), iv_f, cfg.r_free)
                    if cfg.min_win_est > 0 and win_f < cfg.min_win_est:
                        continue
                    iv = round(iv_f, 4)
                    win_est = float(win_f)
                elif cfg.min_win_est > 0:
                    continue

                show_metrics = cfg.include_iv or cfg.min_win_est > 0
                rows.append(
                    {
                        "ticker": sym,
                        "asof": asof.isoformat(),
                        "expiry": exp,
                        "dte": int(dte),
                        "spot": round(float(S), 4),
                        "strike": round(K, 4),
                        "discount": float(1.0 - K / S),
                        "premium": round(prem, 4),
                        "annualized": float((prem / K) * (365.0 / dte)),
                        "iv": iv if show_metrics else None,
                        "win_est": win_est if show_metrics else None,
                        "price_source": str(row.get("price_source", "")),
                        "openInterest": _safe_int(row.get("openInterest")),
                        "volume": _safe_int(row.get("volume")),
                        "contract": str(row.get("contractSymbol", "")),
                    }
                )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df.sort_values(
        by=["ticker", "annualized", "openInterest"],
        ascending=[True, False, False],
        inplace=True,
    )
    df.reset_index(drop=True, inplace=True)
    return df


def _fmt_pct(x: float, digits: int = 2) -> str:
    return f"{100.0 * x:.{digits}f}%"


def render_html(df: pd.DataFrame, cfg: ScreenConfig, asof: date) -> str:
    meta = {
        "asof": asof.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "ann_band": [cfg.ann_low, cfg.ann_high],
        "dte": [cfg.dte_min, cfg.dte_max],
        "tickers": cfg.tickers,
        "min_win_est": cfg.min_win_est,
        "option_price_mode": cfg.option_price_mode,
    }
    records = df.to_dict(orient="records")
    for r in records:
        if r.get("iv") is not None:
            r["iv"] = round(float(r["iv"]), 4)
        if r.get("win_est") is not None:
            r["win_est"] = round(float(r["win_est"]), 4)

    style = """
    :root {
      --bg: #0b1020;
      --panel: #121a33;
      --muted: #8b93b0;
      --text: #e8ecff;
      --accent: #6c8cff;
      --good: #3dd68c;
      --warn: #f5c84c;
      --line: #242c4d;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      background: radial-gradient(1200px 600px at 20% -10%, #1a2550 0%, var(--bg) 55%);
      color: var(--text);
      line-height: 1.45;
    }
    .wrap { max-width: 1280px; margin: 0 auto; padding: 28px 20px 48px; }
    header {
      display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-end;
      justify-content: space-between; margin-bottom: 22px;
    }
    h1 { font-size: 1.45rem; font-weight: 650; letter-spacing: 0.02em; margin: 0; }
    .sub { color: var(--muted); font-size: 0.95rem; margin-top: 6px; }
    .pills { display: flex; flex-wrap: wrap; gap: 8px; }
    .pill {
      border: 1px solid var(--line); background: rgba(255,255,255,0.03);
      padding: 6px 10px; border-radius: 999px; font-size: 0.82rem; color: var(--muted);
    }
    .pill strong { color: var(--text); font-weight: 600; }
    .card {
      background: linear-gradient(180deg, rgba(255,255,255,0.04), rgba(255,255,255,0.02));
      border: 1px solid var(--line); border-radius: 14px; overflow: hidden;
      box-shadow: 0 18px 50px rgba(0,0,0,0.35);
    }
    .toolbar {
      display: flex; flex-wrap: wrap; gap: 10px; align-items: center; justify-content: space-between;
      padding: 12px 14px; border-bottom: 1px solid var(--line); background: rgba(0,0,0,0.15);
    }
    input[type="search"] {
      width: min(420px, 100%); padding: 10px 12px; border-radius: 10px; border: 1px solid var(--line);
      background: rgba(0,0,0,0.25); color: var(--text); outline: none;
    }
    .hint { color: var(--muted); font-size: 0.85rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    thead th {
      text-align: left; padding: 10px 10px; color: var(--muted); font-weight: 600;
      border-bottom: 1px solid var(--line); cursor: pointer; user-select: none; white-space: nowrap;
    }
    tbody td { padding: 9px 10px; border-bottom: 1px solid rgba(255,255,255,0.06); vertical-align: top; }
    tbody tr:hover { background: rgba(108, 140, 255, 0.08); }
    .num { font-variant-numeric: tabular-nums; }
    .tag {
      display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 0.78rem;
      border: 1px solid var(--line); color: var(--muted);
    }
    .yield { color: var(--good); font-weight: 650; }
    .foot { margin-top: 14px; color: var(--muted); font-size: 0.82rem; }
    code { color: #cfe0ff; }
    """

    script = """
    const DATA = __DATA__;
    const META = __META__;

    function fmtPct(x, d=2) {
      if (x === null || x === undefined || Number.isNaN(x)) return "—";
      return (100 * x).toFixed(d) + "%";
    }
    function fmtNum(x, d=2) {
      if (x === null || x === undefined || Number.isNaN(x)) return "—";
      return Number(x).toFixed(d);
    }

    let sortKey = "annualized";
    let sortDir = -1;

    function renderRows(rows) {
      const tb = document.querySelector("#tb");
      tb.innerHTML = "";
      for (const r of rows) {
        const tr = document.createElement("tr");
        const win = (r.win_est === null || r.win_est === undefined) ? "—" : fmtPct(r.win_est, 2);
        const iv = (r.iv === null || r.iv === undefined) ? "—" : fmtNum(r.iv, 4);
        tr.innerHTML = `
          <td><span class="tag">${r.ticker}</span></td>
          <td class="num">${r.expiry}</td>
          <td class="num">${r.dte}</td>
          <td class="num">${fmtNum(r.spot, 2)}</td>
          <td class="num">${fmtNum(r.strike, 2)}</td>
          <td class="num">${fmtPct(r.discount, 2)}</td>
          <td class="num">${fmtNum(r.premium, 2)}</td>
          <td><span class="tag">${r.price_source || "—"}</span></td>
          <td class="num yield">${fmtPct(r.annualized, 2)}</td>
          <td class="num">${iv}</td>
          <td class="num">${win}</td>
          <td class="num">${r.openInterest}</td>
          <td class="num">${r.volume}</td>
          <td><code>${r.contract || ""}</code></td>
        `;
        tb.appendChild(tr);
      }
    }

    function sortRows(rows, key, dir) {
      const copy = [...rows];
      copy.sort((a,b) => {
        const va = a[key], vb = b[key];
        if (va === vb) return 0;
        if (va === null || va === undefined) return 1;
        if (vb === null || vb === undefined) return -1;
        return (va < vb ? -1 : 1) * dir;
      });
      return copy;
    }

    function applyFilter(q) {
      q = q.trim().toLowerCase();
      if (!q) return DATA;
      return DATA.filter(r =>
        [r.ticker, r.expiry, r.contract, r.price_source].some(x => String(x || "").toLowerCase().includes(q))
      );
    }

    function resort() {
      let rows = applyFilter(document.querySelector("#q").value);
      rows = sortRows(rows, sortKey, sortDir);
      renderRows(rows);
    }

    document.querySelector("#q").addEventListener("input", resort);

    document.querySelectorAll("th[data-k]").forEach(th => {
      th.addEventListener("click", () => {
        const k = th.getAttribute("data-k");
        if (sortKey === k) sortDir *= -1;
        else { sortKey = k; sortDir = -1; }
        document.querySelectorAll("th[data-k]").forEach(x => x.removeAttribute("aria-sort"));
        th.setAttribute("aria-sort", sortDir === 1 ? "ascending" : "descending");
        resort();
      });
    });

    const winLine = (META.min_win_est > 0)
      ? ` · 估算胜率≥${fmtPct(META.min_win_est, 0)}`
      : "";
    const pxLine = META.option_price_mode === "chain_mid"
      ? " · 期权价: 链上中间价"
      : " · 期权价: 基准日合约收盘（无Bar则链价）";
    document.querySelector("#meta").textContent =
      `基准日（上一完整交易日）: ${META.asof} · 年化区间 ${fmtPct(META.ann_band[0], 0)}–${fmtPct(META.ann_band[1], 0)} · DTE ${META.dte[0]}–${META.dte[1]} · 标的数 ${META.tickers.length}` + winLine + pxLine;

    renderRows(sortRows(DATA, sortKey, sortDir));
    """

    data_json = json.dumps(records, ensure_ascii=False)
    meta_json = json.dumps(meta, ensure_ascii=False)
    script = script.replace("__DATA__", data_json).replace("__META__", meta_json)

    win_pill = (
        f'<div class="pill">估算胜率 <strong>≥ {_fmt_pct(cfg.min_win_est, 0)}</strong></div>'
        if cfg.min_win_est > 0
        else ""
    )
    if cfg.option_price_mode == "chain_mid":
        price_mode_pill = '<div class="pill">期权价 <strong>链上中间价</strong></div>'
    else:
        price_mode_pill = (
            '<div class="pill">期权价 <strong>基准日收盘</strong>（无日K→链中间价）</div>'
        )

    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>期权年化筛选 · {asof.isoformat()}</title>
  <style>{style}</style>
</head>
<body>
  <div class="wrap">
    <header>
      <div>
        <h1>现金担保看跌 · 年化收益筛选</h1>
        <div class="sub" id="meta"></div>
      </div>
      <div class="pills">
        <div class="pill">公式 <strong>(权利金 / 行权价) × (365 / DTE)</strong></div>
        {price_mode_pill}
        {win_pill}
      </div>
    </header>
    <div class="card">
      <div class="toolbar">
        <input id="q" type="search" placeholder="筛选：代码 / 到期日 / 合约…" />
        <div class="hint">点击表头排序 · 数据来自 Yahoo Finance，期权非历史回放；节假日未建模。</div>
      </div>
      <div style="overflow:auto;">
        <table>
          <thead>
            <tr>
              <th data-k="ticker">标的</th>
              <th data-k="expiry">到期日</th>
              <th data-k="dte">DTE</th>
              <th data-k="spot">现货</th>
              <th data-k="strike">行权价</th>
              <th data-k="discount">折价(OTM深度)</th>
              <th data-k="premium">权利金</th>
              <th data-k="price_source">定价来源</th>
              <th data-k="annualized">年化</th>
              <th data-k="iv">IV</th>
              <th data-k="win_est">估算胜率</th>
              <th data-k="openInterest">OI</th>
              <th data-k="volume">成交量</th>
              <th>合约</th>
            </tr>
          </thead>
          <tbody id="tb"></tbody>
        </table>
      </div>
    </div>
    <div class="foot">
      说明：默认期权权利金取 <strong>基准日（上一完整交易日）日K收盘价</strong>（`yf.Ticker(合约).history`）；若该日无成交/无Bar则回退为链上中间价。年化默认 8%–18%；默认估算胜率 ≥85%（`--min-win 0` 关闭）。IV / 估算胜率为 Black–Scholes 反推与 GBM 近似，仅供筛选与对照。
    </div>
  </div>
  <script>{script}</script>
</body>
</html>"""


def main() -> int:
    p = argparse.ArgumentParser(description="Cash-secured put annual yield screener")
    p.add_argument(
        "--tickers",
        type=str,
        default="QQQ,SPY,PDD",
        help="comma/space-separated symbols",
    )
    p.add_argument("--ann-low", type=float, default=0.08)
    p.add_argument("--ann-high", type=float, default=0.18)
    p.add_argument("--dte-min", type=int, default=14)
    p.add_argument("--dte-max", type=int, default=180)
    p.add_argument(
        "--min-discount",
        type=float,
        default=0.0,
        help="min OTM depth (1 - K/S); 0 disables lower bound",
    )
    p.add_argument(
        "--max-discount",
        type=float,
        default=0.35,
        help="max OTM depth; raise to relax",
    )
    p.add_argument("--min-premium", type=float, default=0.05)
    p.add_argument("--min-oi", type=int, default=10)
    p.add_argument("--min-vol", type=int, default=0)
    p.add_argument("--r-free", type=float, default=R_FREE_DEFAULT)
    p.add_argument(
        "--min-win",
        type=float,
        default=0.85,
        help="min estimated win rate (GBM approx); 0 disables",
    )
    p.add_argument(
        "--no-iv",
        action="store_true",
        help="skip IV/胜率 unless --min-win>0 (then still computed for filtering)",
    )
    p.add_argument(
        "--option-price",
        choices=("session_close", "chain_mid"),
        default="session_close",
        help="session_close: contract daily close on as-of date (Yahoo history); "
        "chain_mid: bid/ask mid or last from chain",
    )
    p.add_argument(
        "--option-workers",
        type=int,
        default=6,
        help="parallel Yahoo requests per expiry batch (session_close mode)",
    )
    p.add_argument(
        "--out",
        type=str,
        default="",
        help="output HTML path (default: options-screener/out/screen-ASOF.html)",
    )
    p.add_argument("--csv", type=str, default="", help="optional CSV path")
    p.add_argument("--open", action="store_true", help="open HTML in default browser")

    args = p.parse_args()
    asof = last_completed_session_date()
    tickers = _parse_tickers(args.tickers)
    cfg = ScreenConfig(
        tickers=tickers,
        ann_low=args.ann_low,
        ann_high=args.ann_high,
        dte_min=args.dte_min,
        dte_max=args.dte_max,
        min_discount=args.min_discount,
        max_discount=args.max_discount,
        min_premium=args.min_premium,
        min_open_interest=args.min_oi,
        min_volume=args.min_vol,
        r_free=args.r_free,
        include_iv=not args.no_iv,
        min_win_est=max(0.0, float(args.min_win)),
        option_price_mode=str(args.option_price),
        option_price_workers=max(1, int(args.option_workers)),
    )

    df = screen_puts(cfg, asof)
    out_dir = Path(__file__).resolve().parent / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_html = Path(args.out) if args.out else out_dir / f"annual-yield-screen-{asof.isoformat()}.html"
    html = render_html(df, cfg, asof)
    out_html.write_text(html, encoding="utf-8")

    if args.csv:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.csv, index=False, encoding="utf-8-sig")

    print(f"asof_session_date={asof.isoformat()}")
    print(f"matches={len(df)}")
    print(f"html={out_html}")
    if args.open:
        webbrowser.open(out_html.as_uri())

    if df.empty:
        print(
            "No rows. Widen --ann-low/--ann-high, lower --min-win (0 disables), "
            "relax --min-oi / --min-discount, or add tickers.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
