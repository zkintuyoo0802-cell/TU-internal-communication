# -*- coding: utf-8 -*-
"""
证券展示名：优先雅虎财经（yfinance → Yahoo Finance），并与代码配对为「名称（代码）」。

- 美股 / 港股：Yahoo `shortName` / `longName`。
- A 股：Yahoo 常为英文简称，若无中文则使用腾讯行情 qt.gtimg.cn（仅名称字段）补中文简称。
- 可编辑覆盖：`data/stock_display_cn.json`（如 NVDA→英伟达、000786→北新建材）。

环境变量：
  COMM_KB_NO_QUOTE_FETCH=1  仅使用已有缓存与本地覆盖，不访问网络。
"""
from __future__ import annotations

import contextlib
import io
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_HAN = re.compile(r"[\u4e00-\u9fff]")
# 与沪深北常见 A 股代码段对齐，避免韩股等六位数字误走 .SZ / 腾讯 A 股接口
_CN_ASHARE_6 = re.compile(
    r"^(?:"
    r"000\d{3}|001\d{3}|002\d{3}|003\d{3}|"
    r"300\d{3}|301\d{3}|302\d{3}|"
    r"600\d{3}|601\d{3}|603\d{3}|605\d{3}|"
    r"688\d{3}"
    r")$"
)

# 非规范写法 → 规范港股 key（如误写 HK18000 → 小米集团对应 HK1810）
HK_FULL_KEY_ALIASES: dict[str, str] = {"HK18000": "HK1810"}
# 非个股六位数字（基金、指数等）在单篇内合并为该虚拟标的
FUND_INDEX_KEY = "基金和指数"


def is_cn_ashare_six(code: str) -> bool:
    return bool(_CN_ASHARE_6.match((code or "").strip()))


def normalize_stock_key(s: str) -> str:
    s = (s or "").strip()
    lu = s.upper()
    if lu in HK_FULL_KEY_ALIASES:
        s = HK_FULL_KEY_ALIASES[lu]
    m = re.fullmatch(r"([A-Za-z]{1,5})\.(N|O|K|B|A)", s, re.I)
    if m:
        return f"{m.group(1).upper()}.{m.group(2).upper()}"
    m2 = re.fullmatch(r"HK(0*\d{3,5})", s, re.I)
    if m2:
        digits = m2.group(1).lstrip("0") or "0"
        return "HK" + digits
    if re.fullmatch(r"\d{6}", s):
        return s
    return s.strip()


def key_to_yahoo_symbol(key: str) -> str | None:
    """Yahoo Finance 标的代码。"""
    k = normalize_stock_key(key)
    if k == FUND_INDEX_KEY:
        return None
    if re.fullmatch(r"[03689]\d{5}", k):
        return _a_share_yahoo_symbol(k)
    m = re.fullmatch(r"([A-Z]{1,5})\.(N|O|K|B|A)", k, re.I)
    if m:
        return m.group(1).upper()
    if re.fullmatch(r"HK\d+", k, re.I):
        try:
            num = int(k[2:], 10)
        except ValueError:
            return None
        if num <= 0 or num > 99999:
            return None
        if num < 10000:
            return f"{num:04d}.HK"
        return f"{num}.HK"
    if re.fullmatch(r"[A-Z]{2,6}", k, re.I):
        return k.upper()
    return None


def _a_share_yahoo_symbol(code: str) -> str | None:
    if not _CN_ASHARE_6.match(code):
        return None
    head3 = code[:3]
    if head3.startswith(("000", "001", "002", "003")) or code.startswith("300"):
        return f"{code}.SZ"
    if head3.startswith(("600", "601", "603", "605", "688")):
        return f"{code}.SS"
    return f"{code}.SZ"


def _gtimg_market_prefix(code: str) -> str | None:
    if not _CN_ASHARE_6.match(code):
        return None
    head3 = code[:3]
    if head3.startswith(("000", "001", "002", "003")) or code.startswith("300"):
        return "sz" + code
    if head3.startswith(("600", "601", "603", "605", "688")):
        return "sh" + code
    return "sz" + code


def tencent_a_share_short_name(code: str) -> str | None:
    """腾讯行情接口：返回 A 股中文简称（gbk 编码）。"""
    sym = _gtimg_market_prefix(code)
    if not sym:
        return None
    url = f"https://qt.gtimg.cn/q={sym}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        raw = urllib.request.urlopen(req, timeout=12).read()
    except (urllib.error.URLError, OSError, TimeoutError):
        return None
    try:
        line = raw.decode("gbk", errors="replace").strip()
    except Exception:
        return None
    # v_sz000786="51~北新建材~000786~...
    m = re.search(r'="?\d+~([^~]*)~', line)
    if not m:
        return None
    name = m.group(1).strip()
    return name if len(name) >= 2 else None


def _yahoo_name_from_info(info: dict[str, Any]) -> str | None:
    if not info:
        return None
    for k in ("shortName", "longName", "symbol"):
        v = info.get(k)
        if isinstance(v, str) and len(v.strip()) >= 2:
            return v.strip()
    return None


def fetch_yahoo_name(yahoo_sym: str) -> str | None:
    try:
        import yfinance as yf  # type: ignore[import-untyped]
    except ImportError:
        return None
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            t = yf.Ticker(yahoo_sym)
            info = t.info or {}
    except Exception:
        return None
    return _yahoo_name_from_info(info)


def _has_han(s: str) -> bool:
    return bool(_HAN.search(s or ""))


def _should_replace_display(display: str, key: str) -> bool:
    """已有「中文（代码）」等则不覆盖。"""
    d = (display or "").strip()
    k = normalize_stock_key(key)
    if k == FUND_INDEX_KEY:
        return False
    if not d or d == k:
        return True
    if d.upper() == k.upper():
        return True
    if re.fullmatch(r"[A-Z]{1,5}\.[NOKBA]", d, re.I) and d.upper() == k.upper():
        return True
    if _has_han(d) and (f"（{k}）" in d or f"({k})" in d):
        return False
    if _has_han(d) and k in d:
        return False
    return True


def _format_label(label: str, key: str) -> str:
    k = normalize_stock_key(key)
    lab = (label or "").strip()
    if not lab:
        return k
    if lab.replace(" ", "").upper() == k.replace(" ", "").upper():
        return k
    if f"（{k}）" in lab or f"({k})" in lab:
        return lab
    return f"{lab}（{k}）"


class QuoteNameResolver:
    """解析 (key, display) → 展示用「名称（代码）」，带磁盘缓存。"""

    def __init__(self, cache_path: Path, cn_json: Path | None = None) -> None:
        self.cache_path = cache_path
        self.no_fetch = os.environ.get("COMM_KB_NO_QUOTE_FETCH", "").strip() in ("1", "true", "yes")
        base = Path(__file__).resolve().parent
        self.cn_json = cn_json or (base / "data" / "stock_display_cn.json")
        self.cn_overrides: dict[str, str] = {}
        if self.cn_json.is_file():
            raw = json.loads(self.cn_json.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                self.cn_overrides = {normalize_stock_key(str(a)): str(b).strip() for a, b in raw.items() if b}
        self._disk: dict[str, Any] = {}
        if self.cache_path.is_file():
            try:
                self._disk = json.loads(self.cache_path.read_text(encoding="utf-8"))
            except Exception:
                self._disk = {}

    def save(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self._disk, ensure_ascii=False, indent=2), encoding="utf-8")

    def _lookup_cn_override(self, key: str) -> str | None:
        k = normalize_stock_key(key)
        if k in self.cn_overrides:
            return self.cn_overrides[k]
        m = re.fullmatch(r"([A-Z]+)\.[NOKBA]", k, re.I)
        if m and m.group(1).upper() in self.cn_overrides:
            return self.cn_overrides[m.group(1).upper()]
        return None

    def resolve_display(self, key: str, display: str) -> str:
        k = normalize_stock_key(key)
        if not k:
            return display or ""
        if k == FUND_INDEX_KEY:
            return (display or "").strip() or FUND_INDEX_KEY
        if not _should_replace_display(display, k):
            return display

        refresh = os.environ.get("COMM_KB_QUOTE_REFRESH", "").strip() in ("1", "true", "yes")
        if not refresh:
            row = self._disk.get(k)
            if isinstance(row, dict):
                hit = row.get("display")
                if isinstance(hit, str) and hit.strip():
                    return hit.strip()

        ysym = key_to_yahoo_symbol(k)
        yname: str | None = None
        tname: str | None = None

        row0 = self._disk.get(k) if isinstance(self._disk.get(k), dict) else {}
        y_cached = row0.get("yahoo") if isinstance(row0, dict) else None

        if ysym and not self.no_fetch:
            if isinstance(y_cached, str) and y_cached.strip() and not refresh:
                yname = y_cached.strip()
            else:
                yname = fetch_yahoo_name(ysym)
                time.sleep(0.06)
        elif isinstance(y_cached, str) and y_cached.strip():
            yname = y_cached.strip()

        cn = self._lookup_cn_override(k)

        if _CN_ASHARE_6.match(k):
            if not cn:
                if yname and _has_han(yname):
                    cn = yname
                elif not yname or not _has_han(yname or ""):
                    tname = tencent_a_share_short_name(k) if not self.no_fetch else None
                    if tname:
                        cn = tname

        label = (cn or "").strip() or (yname or "").strip()
        if not label:
            return display if (display or "").strip() else k

        out = _format_label(label, k)

        self._disk[k] = {
            "display": out,
            "yahoo_symbol": ysym,
            "yahoo": yname,
            "tencent_cn": tname,
            "override_cn": self._lookup_cn_override(k),
        }
        return out


def enrich_records_stock_displays(records: list[dict[str, Any]], cache_path: Path) -> None:
    """就地更新每条 record 的 stock_mentions[].display。"""
    res = QuoteNameResolver(cache_path)
    for r in records:
        for m in r.get("stock_mentions") or []:
            if not isinstance(m, dict):
                continue
            key = str(m.get("key", ""))
            disp = str(m.get("display", ""))
            m["display"] = res.resolve_display(key, disp)
    res.save()
