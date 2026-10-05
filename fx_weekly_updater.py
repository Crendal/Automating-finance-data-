# -*- coding: utf-8 -*-
"""FX Weekly Report incremental updater.

Designed for FX_Weekly_Automation_Light.xlsx.

Main jobs
---------
1. Yahoo Finance daily FX/DXY -> FX_Daily.
2. PPT-ready metrics -> Metrics + regional sheets + Daily_Heatmap + MA5.
3. Yahoo Finance 5-minute bars -> 5min_usdkrw, 5min_usdjpy,
   5min_gbpusd, 5min_eurusd, 5min_dxy.
4. KMBCO IRS/CRS and BOK ECOS KTB yields.
5. KMBCO USD/KRW FX swap points (bid/offer and mid).
6. Derived bond-swap spread and swap basis.
7. PPT-ready chart PNGs and Excel snapshots.

Incremental rule
----------------
For each raw-data sheet the script reads the last stored Date/Datetime_KST,
starts after that point (5-minute Yahoo uses a one-day overlap and de-duplicates),
and stops at --asof. On a blank/short FX_Daily sheet, it bootstraps long history
because 15Y-high/MDD/RSI/volatility require history.

Example
-------
python fx_weekly_updater.py --workbook FX_Weekly_Automation_Light.xlsx --asof 2026-09-20
"""

from __future__ import annotations

import argparse
import math
import os
import random
import re
import time
import warnings
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

BOOTSTRAP_START = "2009-12-28"
MIN_HISTORY_ROWS = 2500
INTRADAY_BOOTSTRAP_DAYS = 10
INTRADAY_KEEP_DAYS = 35
MA_LOOKBACK_YEARS = 4
YF_BATCH_SIZE = 8
YF_PAUSE_SECONDS = 0.7

# User's report palette
MAIN = (0 / 255, 44 / 255, 95 / 255)
AUX = (115 / 255, 153 / 255, 198 / 255)
GREY = (162 / 255, 162 / 255, 165 / 255)
GOLD = (162 / 255, 117 / 255, 1 / 255)

KMB_DERI_URL = "https://www.kmbco.com/kor/rate/deri_rate.do"
KMB_SWAP_URL = "https://www.kmbco.com/kor/rate/swap_rate.do"
KMB_SWAP_TENORS = ["1M", "2M", "3M", "6M", "1Y"]

ECOS_BASE = "https://ecos.bok.or.kr/api"
ECOS_KTB_STAT_CODE = "817Y002"
ECOS_KTB_CYCLE = "D"
ECOS_KTB_HEADERS = {
    "1Y": "국고채(1년) [D]",
    "3Y": "국고채(3년) [D]",
    "5Y": "국고채(5년) [D]",
    "10Y": "국고채(10년) [D]",
}
# Fallback positions in the user's current ecos_rates.xlsx (0-based):
# D=5Y, I=1Y, J=10Y, N=3Y. Header-name matching is always preferred.
ECOS_KTB_FALLBACK_POS = {"5Y": 3, "1Y": 8, "10Y": 9, "3Y": 13}

# Final quote convention for PPT: USD/CCY. DXY is kept as-is.
DIRECT_YF = {
    "USD_KRW": "KRW=X", "USD_JPY": "JPY=X", "USD_CNY": "CNY=X", "USD_TWD": "TWD=X",
    "USD_CAD": "CAD=X", "USD_NOK": "NOK=X", "USD_SEK": "SEK=X", "USD_CHF": "CHF=X",
    "USD_RUB": "RUB=X", "USD_TRY": "TRY=X", "USD_DKK": "DKK=X", "USD_CZK": "CZK=X",
    "USD_PLN": "PLN=X", "USD_HUF": "HUF=X", "USD_MXN": "MXN=X", "USD_PEN": "PEN=X",
    "USD_CLP": "CLP=X", "USD_BRL": "BRL=X", "USD_COP": "COP=X", "USD_ARS": "ARS=X",
    "USD_PKR": "PKR=X", "USD_BDT": "BDT=X", "USD_INR": "INR=X", "USD_LKR": "LKR=X",
    "USD_THB": "THB=X", "USD_SGD": "SGD=X", "USD_VND": "VND=X", "USD_PHP": "PHP=X",
    "USD_MYR": "MYR=X", "USD_IDR": "IDR=X", "USD_ILS": "ILS=X", "USD_EGP": "EGP=X",
    "USD_NGN": "NGN=X", "USD_SAR": "SAR=X", "USD_AED": "AED=X", "USD_KES": "KES=X",
    "USD_ZAR": "ZAR=X", "DXY": "DX-Y.NYB",
}
INVERSE_YF = {
    "USD_EUR": "EURUSD=X", "USD_GBP": "GBPUSD=X",
    "USD_AUD": "AUDUSD=X", "USD_NZD": "NZDUSD=X",
}

FX_COLUMNS = [
    "DXY", "USD_KRW", "USD_JPY", "USD_CNY", "USD_TWD", "USD_CAD",
    "USD_NOK", "USD_EUR", "USD_RUB", "USD_SEK", "USD_TRY", "USD_GBP",
    "USD_CZK", "USD_DKK", "USD_PLN", "USD_CHF", "USD_HUF",
    "USD_MXN", "USD_PEN", "USD_CLP", "USD_BRL", "USD_COP", "USD_ARS",
    "USD_PKR", "USD_BDT", "USD_INR", "USD_LKR",
    "USD_THB", "USD_SGD", "USD_VND", "USD_PHP", "USD_MYR", "USD_IDR",
    "USD_AUD", "USD_NZD", "USD_ILS", "USD_EGP", "USD_NGN", "USD_SAR",
    "USD_AED", "USD_KES", "USD_ZAR",
]

REGIONS = {
    "East Asia": ["USD_KRW", "USD_CNY", "USD_JPY", "USD_TWD"],
    "US & Canada": ["DXY", "USD_CAD"],
    "Europe": ["USD_NOK", "USD_EUR", "USD_RUB", "USD_SEK", "USD_TRY", "USD_GBP", "USD_CZK", "USD_DKK", "USD_PLN", "USD_CHF", "USD_HUF"],
    "Latin America": ["USD_MXN", "USD_PEN", "USD_CLP", "USD_BRL", "USD_COP", "USD_ARS"],
    "South Asia": ["USD_PKR", "USD_BDT", "USD_INR", "USD_LKR"],
    "Southeast Asia": ["USD_THB", "USD_SGD", "USD_VND", "USD_PHP", "USD_MYR", "USD_IDR"],
    "Oceania": ["USD_AUD", "USD_NZD"],
    "Middle East & Africa": ["USD_ILS", "USD_EGP", "USD_NGN", "USD_SAR", "USD_AED", "USD_KES", "USD_ZAR"],
}

REGION_SHEETS = {
    "East Asia": "Metrics_EastAsia", "US & Canada": "Metrics_USCanada",
    "Europe": "Metrics_Europe", "Latin America": "Metrics_LatAm",
    "South Asia": "Metrics_SouthAsia", "Southeast Asia": "Metrics_SEAsia",
    "Oceania": "Metrics_Oceania", "Middle East & Africa": "Metrics_ME_Africa",
}

HISTORICAL_TREND_PAIRS = [
    "USD_KRW", "USD_JPY", "USD_CNY", "USD_TWD", "DXY", "USD_CAD",
    "USD_EUR", "USD_GBP", "USD_SEK", "USD_CHF", "USD_RUB", "USD_DKK",
    "USD_MXN", "USD_BRL", "USD_COP", "USD_ARS",
    "USD_INR", "USD_SGD", "USD_THB", "USD_IDR", "USD_VND", "USD_PHP",
    "USD_AUD", "USD_NZD",
]

HEATMAP_ORDER = ["USD_EUR", "USD_GBP", "USD_AUD", "USD_NZD", "USD_JPY", "USD_CHF", "USD_CAD", "USD_NOK", "USD_SEK", "USD_KRW"]
HEATMAP_LABELS = {
    "USD_EUR": "EUR", "USD_GBP": "GBP", "USD_AUD": "AUD", "USD_NZD": "NZD",
    "USD_JPY": "JPY", "USD_CHF": "CHF", "USD_CAD": "CAD", "USD_NOK": "NOK",
    "USD_SEK": "SEK", "USD_KRW": "KRW",
}

INTRADAY = {
    "5min_usdkrw": "KRW=X",
    "5min_usdjpy": "JPY=X",
    "5min_gbpusd": "GBPUSD=X",
    "5min_eurusd": "EURUSD=X",
    "5min_dxy": "DX-Y.NYB",
}


def log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


def parse_asof(s: Optional[str]) -> pd.Timestamp:
    return pd.Timestamp(s).normalize() if s else pd.Timestamp.today().normalize()


def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def ensure_workbook(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    from openpyxl import Workbook
    wb = Workbook()
    wb.active.title = "README"
    wb.save(path)


def read_sheet(path: Path, sheet: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet)
    except Exception:
        return pd.DataFrame()


def normalize_date_col(df: pd.DataFrame, preferred: str = "Date") -> pd.DataFrame:
    if df.empty:
        return df
    x = df.copy()
    if preferred not in x.columns:
        candidates = [c for c in x.columns if str(c).strip() in {"전송일", "일자", "date", "Date"}]
        if not candidates and len(x.columns):
            first = x.columns[0]
            # Old Excel exports often saved the index as Unnamed: 0.
            if str(first).startswith("Unnamed"):
                candidates = [first]
        if candidates:
            x = x.rename(columns={candidates[0]: preferred})
    if preferred in x.columns:
        x[preferred] = pd.to_datetime(x[preferred], errors="coerce")
    return x


def last_date(path: Path, sheet: str, col: str = "Date") -> Optional[pd.Timestamp]:
    df = normalize_date_col(read_sheet(path, sheet), col)
    if df.empty or col not in df.columns:
        return None
    s = pd.to_datetime(df[col], errors="coerce").dropna()
    return s.max() if not s.empty else None


def merge_by_date(old: pd.DataFrame, new: pd.DataFrame, date_col: str = "Date") -> pd.DataFrame:
    old = normalize_date_col(old, date_col)
    new = normalize_date_col(new, date_col)
    if old.empty:
        out = new.copy()
    elif new.empty:
        out = old.copy()
    else:
        out = pd.concat([old, new], ignore_index=True)
    if out.empty or date_col not in out.columns:
        return out
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col]).drop_duplicates(subset=[date_col], keep="last")
    return out.sort_values(date_col).reset_index(drop=True)


def replace_sheets(path: Path, sheets: Dict[str, pd.DataFrame]) -> None:
    ensure_workbook(path)
    with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
        for name, df in sheets.items():
            out = df.copy()
            for c in out.columns:
                if pd.api.types.is_datetime64_any_dtype(out[c]):
                    try:
                        out[c] = out[c].dt.tz_localize(None)
                    except Exception:
                        pass
            out.to_excel(writer, sheet_name=name, index=False)


def append_update_log(path: Path, dataset: str, asof: pd.Timestamp,
                      previous_last=None, requested_start=None, requested_end=None,
                      rows_added=None, status="OK") -> None:
    old = read_sheet(path, "Update_Log")
    row = pd.DataFrame([{
        "Run_Timestamp": pd.Timestamp.now().floor("s"), "AsOf": asof,
        "Dataset": dataset, "Previous_Last": previous_last,
        "Requested_Start": requested_start, "Requested_End": requested_end,
        "Rows_Added": rows_added, "Status": status,
    }])
    out = pd.concat([old, row], ignore_index=True) if not old.empty else row
    replace_sheets(path, {"Update_Log": out})


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    if isinstance(x.columns, pd.MultiIndex):
        cols = []
        for col in x.columns:
            parts = [str(v).strip() for v in col if str(v).strip() and not str(v).startswith("Unnamed")]
            cols.append(parts[-1] if parts else str(col[-1]))
        x.columns = cols
    else:
        x.columns = [str(c).strip() for c in x.columns]
    return x


# ------------------------- Yahoo daily -------------------------
def yf_download(tickers, start, end, interval, tries=3):
    import yfinance as yf
    last_exc = None
    for attempt in range(tries):
        try:
            return yf.download(
                tickers=tickers,
                start=pd.Timestamp(start).strftime("%Y-%m-%d"),
                end=pd.Timestamp(end).strftime("%Y-%m-%d"),
                interval=interval,
                auto_adjust=False,
                progress=False,
                threads=False,
                group_by="ticker",
            )
        except Exception as exc:
            last_exc = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Yahoo download failed: {tickers}: {last_exc}")


def extract_field(raw: pd.DataFrame, ticker: str, field: str) -> pd.Series:
    if raw is None or raw.empty:
        return pd.Series(dtype=float)
    if isinstance(raw.columns, pd.MultiIndex):
        for key in [(ticker, field), (field, ticker)]:
            if key in raw.columns:
                return pd.to_numeric(raw[key], errors="coerce")
    if field in raw.columns:
        return pd.to_numeric(raw[field], errors="coerce")
    return pd.Series(dtype=float)


def fetch_fx_daily(path: Path, asof: pd.Timestamp) -> pd.DataFrame:
    old = normalize_date_col(read_sheet(path, "FX_Daily"))
    valid_old = pd.to_datetime(old.get("Date"), errors="coerce").dropna() if not old.empty and "Date" in old else pd.Series(dtype="datetime64[ns]")
    prev = valid_old.max() if len(valid_old) else None

    if len(valid_old) >= MIN_HISTORY_ROWS:
        start = prev.normalize() + pd.Timedelta(days=1)
        mode = "incremental"
    else:
        start = pd.Timestamp(BOOTSTRAP_START)
        mode = "bootstrap"
    end = asof + pd.Timedelta(days=1)  # Yahoo end is exclusive.

    if start >= end:
        log("FX_Daily already current")
        append_update_log(path, "FX_Daily", asof, prev, start, asof, 0, "NO CHANGE")
        return old

    log(f"FX_Daily {mode}: {start.date()} -> {asof.date()}")
    ticker_to_final = {v: k for k, v in DIRECT_YF.items()}
    ticker_to_final.update({v: k for k, v in INVERSE_YF.items()})
    all_tickers = list(ticker_to_final)
    series = {}
    for batch in chunked(all_tickers, YF_BATCH_SIZE):
        try:
            raw = yf_download(batch, start, end, "1d")
        except Exception as exc:
            log(f"Yahoo batch failed {batch}: {exc}")
            continue
        for ticker in batch:
            s = extract_field(raw, ticker, "Close")
            if s.empty:
                log(f"  missing daily ticker: {ticker}")
                continue
            final = ticker_to_final[ticker]
            if final in INVERSE_YF:
                s = 1.0 / s.replace(0, np.nan)
            s.name = final
            series[final] = s
        time.sleep(YF_PAUSE_SECONDS)

    if not series:
        append_update_log(path, "FX_Daily", asof, prev, start, asof, 0, "FAILED: no Yahoo data")
        return old

    new = pd.concat(series.values(), axis=1)
    new.index = pd.to_datetime(new.index).tz_localize(None)
    new = new.sort_index().reset_index().rename(columns={"index": "Date"})
    for c in FX_COLUMNS:
        if c not in new.columns:
            new[c] = np.nan
    new = new[["Date"] + FX_COLUMNS]

    merged = new if mode == "bootstrap" else merge_by_date(old, new, "Date")
    for c in FX_COLUMNS:
        if c not in merged.columns:
            merged[c] = np.nan
    merged = merged[["Date"] + FX_COLUMNS]
    replace_sheets(path, {"FX_Daily": merged})
    added = len(merged) - (len(old) if mode == "incremental" else 0)
    append_update_log(path, "FX_Daily", asof, prev, start, asof, max(added, 0), mode.upper())
    return merged


# ------------------------- Metrics -------------------------
def rsi_wilder(price: pd.Series, period: int = 14) -> float:
    p = pd.to_numeric(price, errors="coerce").dropna()
    if len(p) <= period:
        return np.nan
    delta = p.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    if avg_loss.iloc[-1] == 0 and avg_gain.iloc[-1] > 0:
        return 100.0
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - 100 / (1 + rs)
    return float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else np.nan


def calculate_metrics(fx: pd.DataFrame) -> pd.DataFrame:
    if fx.empty or "Date" not in fx.columns:
        return pd.DataFrame()
    x = fx.copy()
    x["Date"] = pd.to_datetime(x["Date"], errors="coerce")
    x = x.dropna(subset=["Date"]).sort_values("Date").set_index("Date")
    latest = x.index.max()
    region_lookup = {c: r for r, cs in REGIONS.items() for c in cs}
    rows = []
    for ccy in FX_COLUMNS:
        if ccy not in x.columns:
            continue
        s = pd.to_numeric(x[ccy], errors="coerce").dropna()
        if s.empty:
            continue
        cur = s.iloc[-1]
        wow_base = s.iloc[-6] if len(s) >= 6 else np.nan
        mom_base = s.iloc[-22] if len(s) >= 22 else np.nan
        y = s[s.index.year == latest.year]
        ytd_base = y.iloc[0] if len(y) else np.nan
        wow = cur / wow_base - 1 if pd.notna(wow_base) and wow_base != 0 else np.nan
        mom = cur / mom_base - 1 if pd.notna(mom_base) and mom_base != 0 else np.nan
        ytd = cur / ytd_base - 1 if pd.notna(ytd_base) and ytd_base != 0 else np.nan

        trailing_15y = s[s.index >= latest - pd.DateOffset(years=15)]
        high15 = trailing_15y.max() if len(trailing_15y) else s.max()
        dev_high = cur / high15 - 1 if pd.notna(high15) and high15 != 0 else np.nan
        running_max = s.cummax()
        mdd = (s / running_max - 1).min()
        ret = np.log(s / s.shift(1)).dropna()
        vol20 = ret.rolling(20).std().iloc[-1] * np.sqrt(252) if len(ret) >= 20 else np.nan

        rows.append({
            "Region": region_lookup.get(ccy, "Other"), "Currency": ccy, "AsOf": latest,
            "Current": cur, "WoW(%)": wow, "MoM(%)": mom, "YTD(%)": ytd,
            "Deviation from 15Y High (%)": dev_high, "MDD(%)": mdd,
            "RSI(14)": rsi_wilder(s), "Vol 20D(%)": vol20,
        })
    return pd.DataFrame(rows)


def build_daily_heatmap(fx: pd.DataFrame) -> pd.DataFrame:
    """
    Build the PPT Daily % Change Heatmap.

    - Rows: EUR, GBP, AUD, NZD, JPY, CHF, CAD, NOK, SEK, KRW
    - Columns: latest five trading dates (YYYY-MM-DD)
    - Value: close-to-close daily change from the USD perspective

      USD/local pairs (JPY, CHF, CAD, NOK, SEK, KRW) are used directly.
      Local/USD pairs (EUR, GBP, AUD, NZD) are already inverted to USD/local
      when FX_Daily is downloaded. Therefore a positive value always means
      USD appreciation / local-currency depreciation.

    Five daily changes require at least six valid closes. Missing observations
    are not forward-filled; each currency uses its previous available close.
    """
    if fx.empty or "Date" not in fx.columns:
        return pd.DataFrame(columns=["Currency"])

    x = fx.copy()
    x["Date"] = pd.to_datetime(x["Date"], errors="coerce")
    x = x.dropna(subset=["Date"]).sort_values("Date").set_index("Date")

    cols = [c for c in HEATMAP_ORDER if c in x.columns]
    if not cols:
        return pd.DataFrame({"Currency": [HEATMAP_LABELS[c] for c in HEATMAP_ORDER]})

    # Explicitly disable padding so a missing quote is never treated as a flat day.
    pct = x[cols].apply(
        lambda s: pd.to_numeric(s, errors="coerce").pct_change(fill_method=None)
    )

    # Use the latest five market dates for which at least one heatmap currency
    # has a valid close-to-close change.
    valid_dates = pct.index[pct.notna().any(axis=1)]
    dates = list(valid_dates[-5:])

    rows = []
    for ccy in HEATMAP_ORDER:
        row = {"Currency": HEATMAP_LABELS[ccy]}
        for d in dates:
            row[d.strftime("%Y-%m-%d")] = (
                pct.at[d, ccy] if ccy in pct.columns and d in pct.index else np.nan
            )
        rows.append(row)

    return pd.DataFrame(rows, columns=["Currency"] + [d.strftime("%Y-%m-%d") for d in dates])


def build_ma5(fx: pd.DataFrame, asof: pd.Timestamp) -> pd.DataFrame:
    if fx.empty:
        return pd.DataFrame()
    x = fx.copy(); x["Date"] = pd.to_datetime(x["Date"], errors="coerce")
    x = x.dropna(subset=["Date"]).sort_values("Date").set_index("Date")
    cols = [c for c in HISTORICAL_TREND_PAIRS if c in x.columns]
    ma = x[cols].rolling(5).mean()
    ma = ma[ma.index >= asof - pd.DateOffset(years=MA_LOOKBACK_YEARS)]
    return ma.reset_index()


def write_fx_derived(path: Path, fx: pd.DataFrame, asof: pd.Timestamp) -> None:
    metrics = calculate_metrics(fx)
    sheets = {
        "Metrics": metrics,
        "Daily_Heatmap": build_daily_heatmap(fx),
        "MA5": build_ma5(fx, asof),
    }
    for region, sheet in REGION_SHEETS.items():
        if metrics.empty:
            sheets[sheet] = pd.DataFrame(columns=metrics.columns)
            continue
        order = REGIONS[region]
        d = metrics[metrics["Region"] == region].copy()
        d["_order"] = d["Currency"].map({c: i for i, c in enumerate(order)})
        sheets[sheet] = d.sort_values("_order").drop(columns="_order")
    replace_sheets(path, sheets)


def report_week_bounds(asof: pd.Timestamp) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Return Monday and the effective weekday end for the report week.

    Examples:
      asof=2026-09-18 (Fri) -> 2026-09-14 .. 2026-09-18
      asof=2026-09-20 (Sun) -> 2026-09-14 .. 2026-09-18
      asof=2026-09-16 (Wed) -> 2026-09-14 .. 2026-09-16
    """
    a = pd.Timestamp(asof).normalize()
    monday = a - pd.Timedelta(days=a.weekday())
    friday = monday + pd.Timedelta(days=4)
    effective_end = friday if a.weekday() >= 5 else min(a, friday)
    return monday, effective_end


def filter_report_week(df: pd.DataFrame, asof: pd.Timestamp, dt_col: str = "Datetime_KST") -> pd.DataFrame:
    if df is None or df.empty or dt_col not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    start, end = report_week_bounds(asof)
    x = df.copy()
    x[dt_col] = pd.to_datetime(x[dt_col], errors="coerce")
    x = x.dropna(subset=[dt_col])
    # Calendar weekdays in KST only; end is exclusive of the next day.
    mask = (x[dt_col] >= start) & (x[dt_col] < end + pd.Timedelta(days=1))
    return x.loc[mask].sort_values(dt_col).reset_index(drop=True)


# ------------------------- Yahoo 5-minute -------------------------
def fetch_one_intraday(path: Path, sheet: str, ticker: str, asof: pd.Timestamp) -> pd.DataFrame:
    """Download only the weekday window containing --asof.

    Unlike the daily datasets, 5-minute bars are report-week snapshots, so each
    run replaces the sheet with that week's data instead of keeping a rolling
    multi-week history.
    """
    week_start, week_end = report_week_bounds(asof)
    # yfinance end is exclusive. Fetch through the day after the effective end,
    # then apply an exact KST calendar filter below.
    fetch_start = week_start
    fetch_end = week_end + pd.Timedelta(days=1)
    prev_df = read_sheet(path, sheet)
    prev = None
    if not prev_df.empty and "Datetime_KST" in prev_df.columns:
        prev = pd.to_datetime(prev_df["Datetime_KST"], errors="coerce").max()

    log(f"{sheet}: report week {week_start.date()} -> {week_end.date()}")
    raw = yf_download([ticker], fetch_start, fetch_end, "5m")
    if raw is None or raw.empty:
        append_update_log(path, sheet, asof, prev, week_start, week_end, 0, "NO DATA")
        return pd.DataFrame(columns=prev_df.columns if not prev_df.empty else None)

    data = pd.DataFrame(index=raw.index)
    for field in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
        ser = extract_field(raw, ticker, field)
        if not ser.empty:
            data[field] = ser
    idx = pd.to_datetime(data.index)
    try:
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        idx = idx.tz_convert("Asia/Seoul").tz_localize(None)
    except Exception:
        if getattr(idx, "tz", None) is not None:
            idx = idx.tz_localize(None)
    data.insert(0, "Datetime_KST", idx)
    data = data.reset_index(drop=True)

    if sheet == "5min_eurusd" and "Close" in data:
        data["USD_EUR"] = 1 / pd.to_numeric(data["Close"], errors="coerce").replace(0, np.nan)
    if sheet == "5min_gbpusd" and "Close" in data:
        data["USD_GBP"] = 1 / pd.to_numeric(data["Close"], errors="coerce").replace(0, np.nan)

    data = filter_report_week(data, asof, "Datetime_KST")
    data = data.drop_duplicates("Datetime_KST", keep="last").reset_index(drop=True)
    append_update_log(path, sheet, asof, prev, week_start, week_end, len(data), "WEEK SNAPSHOT")
    return data


def update_intraday(path: Path, asof: pd.Timestamp) -> Dict[str, pd.DataFrame]:
    sheets = {}
    for sheet, ticker in INTRADAY.items():
        try:
            sheets[sheet] = fetch_one_intraday(path, sheet, ticker, asof)
        except Exception as exc:
            log(f"{sheet} failed: {exc}")
            sheets[sheet] = read_sheet(path, sheet)
            append_update_log(path, sheet, asof, last_date(path, sheet, "Datetime_KST"), None, asof, 0, f"FAILED: {exc}")
        time.sleep(YF_PAUSE_SECONDS)
    replace_sheets(path, sheets)
    return sheets


# ------------------------- Selenium common -------------------------
def build_chrome(headless: bool = True):
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=ko-KR")
    return webdriver.Chrome(options=opts)


def parse_kmb_rate_table(html: str) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html))
    matches = []
    for t in tables:
        t = flatten_columns(t)
        txt = "|".join(map(str, t.columns))
        if ("전송일" in txt or "일자" in txt or "Date" in txt) and "1Y" in txt and "10Y" in txt:
            matches.append(t)
    if not matches:
        return pd.DataFrame()
    df = max(matches, key=len).copy()
    date_col = next((c for c in df.columns if "전송일" in str(c) or "일자" in str(c) or str(c)=="Date"), df.columns[0])
    df = df.rename(columns={date_col: "Date"})
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    keep = ["Date"] + [c for c in ["1Y","2Y","3Y","4Y","5Y","7Y","10Y"] if c in df.columns]
    df = df[keep].dropna(subset=["Date"])
    for c in keep[1:]: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("Date").drop_duplicates("Date", keep="last").reset_index(drop=True)


def fetch_kmb_irs_crs() -> Dict[str, pd.DataFrame]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    drv = build_chrome(True)
    out = {}
    try:
        for name, idx in [("IRS",1),("CRS",2)]:
            drv.get(KMB_DERI_URL)
            wait = WebDriverWait(drv, 15)
            btn = wait.until(EC.presence_of_element_located((By.XPATH, f"/html/body/main/article[1]/form/nav/button[{idx}]")))
            drv.execute_script("arguments[0].click();", btn)
            time.sleep(2.5)
            out[name] = parse_kmb_rate_table(drv.page_source)
    finally:
        drv.quit()
    return out


def business_day_cutoff(asof: pd.Timestamp) -> pd.Timestamp:
    """Latest Mon-Fri business day on or before ``asof``.

    Korean public holidays are intentionally not encoded here: the KMBCO source
    itself returns no usable quote (or all-zero FX-swap quotes) on those dates,
    so they are skipped when fetched.
    """
    a = pd.Timestamp(asof).normalize()
    return pd.offsets.BDay().rollback(a)


def next_business_day(d: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(d).normalize() + pd.offsets.BDay(1)


def update_irs_crs(path: Path, asof: pd.Timestamp) -> Tuple[pd.DataFrame,pd.DataFrame]:
    """Append only KMBCO IRS/CRS rows missing from the workbook.

    The KMBCO derivatives page exposes a historical table, so one page fetch is
    enough.  We still apply a strict incremental window: the business day after
    the last stored row through the report cutoff.
    """
    old_irs = normalize_date_col(read_sheet(path,"IRS")); old_crs = normalize_date_col(read_sheet(path,"CRS"))
    cutoff = business_day_cutoff(asof)
    try:
        fetched = fetch_kmb_irs_crs(); writes={}
        for name,old in [("IRS",old_irs),("CRS",old_crs)]:
            prev = pd.to_datetime(old["Date"],errors="coerce").max() if not old.empty and "Date" in old else None
            start = next_business_day(prev) if prev is not None and pd.notna(prev) else None
            new = fetched.get(name,pd.DataFrame()).copy()
            if not new.empty:
                new["Date"] = pd.to_datetime(new["Date"], errors="coerce")
                new = new.dropna(subset=["Date"])
                new = new[new["Date"] <= cutoff]
                if start is not None:
                    new = new[new["Date"] >= start]
            merged = merge_by_date(old,new)
            writes[name]=merged
            added=max(len(merged)-len(old),0)
            status="NO CHANGE" if added==0 else "OK"
            append_update_log(path,name,asof,prev,start,cutoff,added,status)
            log(f"{name}: {start.date() if start is not None else 'initial'} -> {cutoff.date()} | added {added}")
        replace_sheets(path,writes)
        return writes["IRS"],writes["CRS"]
    except Exception as exc:
        log(f"IRS/CRS skipped: {exc}")
        append_update_log(path,"IRS_CRS",asof,None,None,cutoff,0,f"FAILED: {exc}")
        return old_irs,old_crs


def _pick_ecos_sheet(excel_path: Path) -> pd.DataFrame:
    """Read the raw daily-rate table from ecos_rates.xlsx.

    get_rates.py currently may leave the raw 817Y002 table as Sheet1, while
    older/full exports may still contain a sheet named 817Y002.  Support both.
    """
    if not excel_path.exists():
        return pd.DataFrame()
    for sheet in ["Sheet1", "817Y002", "Key Rates"]:
        try:
            df = pd.read_excel(excel_path, sheet_name=sheet)
            if not df.empty:
                return df
        except Exception:
            continue
    try:
        # Last resort: first sheet.
        return pd.read_excel(excel_path, sheet_name=0)
    except Exception:
        return pd.DataFrame()


def parse_ecos_ktb_file(excel_path: Path, asof: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """Extract 1Y/3Y/5Y/10Y KTB yields from ecos_rates.xlsx.

    Preferred mapping is by exact ECOS header name.  If headers change or the
    file is a raw positional export, fall back to the user's current layout:
    D=5Y, I=1Y, J=10Y, N=3Y.
    """
    raw = _pick_ecos_sheet(excel_path)
    if raw.empty:
        return pd.DataFrame(columns=["Date", "1Y", "3Y", "5Y", "10Y"])

    raw = raw.copy()
    date_col = next(
        (c for c in raw.columns if str(c).strip().lower() in {"date", "일자", "전송일"}),
        raw.columns[0],
    )
    out = pd.DataFrame({"Date": pd.to_datetime(raw[date_col], errors="coerce")})

    for tenor in ["1Y", "3Y", "5Y", "10Y"]:
        exact = ECOS_KTB_HEADERS[tenor]
        if exact in raw.columns:
            src = raw[exact]
        else:
            pos = ECOS_KTB_FALLBACK_POS[tenor]
            if pos >= len(raw.columns):
                src = pd.Series(np.nan, index=raw.index)
            else:
                src = raw.iloc[:, pos]
        out[tenor] = pd.to_numeric(src, errors="coerce")

    out = out.dropna(subset=["Date"]).sort_values("Date")
    out = out.drop_duplicates(subset=["Date"], keep="last")
    if asof is not None:
        out = out[out["Date"] <= asof]
    return out.reset_index(drop=True)


def _ecos_get_json(url: str, max_retry: int = 3, sleep: float = 0.5) -> dict:
    import requests
    last_exc = None
    for i in range(max_retry):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            js = r.json()
            if isinstance(js, dict) and "RESULT" in js:
                code = js["RESULT"].get("CODE")
                if code != "INFO-000":
                    raise RuntimeError(f"ECOS API error: {code} - {js['RESULT'].get('MESSAGE')}")
            return js
        except Exception as exc:
            last_exc = exc
            if i < max_retry - 1:
                time.sleep(sleep)
    raise RuntimeError(f"ECOS request failed: {last_exc}")


def fetch_ecos_ktb_range(start: pd.Timestamp, end: pd.Timestamp, api_key: str) -> pd.DataFrame:
    """Fetch only missing KTB tenors directly from BOK ECOS.

    This is an optional fallback.  If ecos_rates.xlsx has already been refreshed
    by get_rates.py, the updater simply consumes that file instead.
    """
    if not api_key:
        return pd.DataFrame(columns=["Date", "1Y", "3Y", "5Y", "10Y"])
    start_s = pd.Timestamp(start).strftime("%Y%m%d")
    end_s = pd.Timestamp(end).strftime("%Y%m%d")
    item_url = f"{ECOS_BASE}/StatisticItemList/{api_key}/json/kr/1/9999/{ECOS_KTB_STAT_CODE}"
    js = _ecos_get_json(item_url)
    rows = js.get("StatisticItemList", {}).get("row", [])
    if not rows:
        return pd.DataFrame(columns=["Date", "1Y", "3Y", "5Y", "10Y"])

    items = pd.DataFrame(rows)
    code_col = "ITEM_CODE1" if "ITEM_CODE1" in items.columns else "ITEM_CODE"
    name_col = "ITEM_NAME1" if "ITEM_NAME1" in items.columns else "ITEM_NAME"
    wanted_names = {tenor: header.replace(" [D]", "") for tenor, header in ECOS_KTB_HEADERS.items()}
    series_frames = []
    for tenor, wanted in wanted_names.items():
        hit = items[items[name_col].astype(str).str.strip() == wanted]
        if hit.empty:
            # A looser match makes the code resilient to minor naming changes.
            hit = items[items[name_col].astype(str).str.contains(re.escape(wanted), regex=True, na=False)]
        if hit.empty:
            log(f"ECOS item not found: {wanted}")
            continue
        item_code = str(hit.iloc[0][code_col])
        url = (
            f"{ECOS_BASE}/StatisticSearch/{api_key}/json/kr/1/100000/"
            f"{ECOS_KTB_STAT_CODE}/{ECOS_KTB_CYCLE}/{start_s}/{end_s}/{item_code}"
        )
        data = _ecos_get_json(url).get("StatisticSearch", {}).get("row", [])
        if not data:
            continue
        df = pd.DataFrame(data)
        x = pd.DataFrame({
            "Date": pd.to_datetime(df["TIME"], format="%Y%m%d", errors="coerce"),
            tenor: pd.to_numeric(df["DATA_VALUE"], errors="coerce"),
        })
        series_frames.append(x.dropna(subset=["Date"]))

    if not series_frames:
        return pd.DataFrame(columns=["Date", "1Y", "3Y", "5Y", "10Y"])
    out = series_frames[0]
    for x in series_frames[1:]:
        out = pd.merge(out, x, on="Date", how="outer")
    for tenor in ["1Y", "3Y", "5Y", "10Y"]:
        if tenor not in out.columns:
            out[tenor] = np.nan
    return out[["Date", "1Y", "3Y", "5Y", "10Y"]].sort_values("Date").reset_index(drop=True)


def update_ktb(
    path: Path,
    asof: pd.Timestamp,
    ecos_file: Path,
    ecos_api_key: Optional[str] = None,
) -> pd.DataFrame:
    """Incrementally update KTB from BOK ECOS / ecos_rates.xlsx.

    Priority:
      1) consume rows in ecos_rates.xlsx produced by get_rates.py;
      2) if that source is stale and ECOS_API_KEY is available, fetch only the
         missing date range directly from ECOS;
      3) merge with the existing KTB sheet and de-duplicate by date.
    """
    old = normalize_date_col(read_sheet(path, "KTB"))
    prev = (
        pd.to_datetime(old["Date"], errors="coerce").max()
        if not old.empty and "Date" in old
        else None
    )
    requested_start = (
        prev + pd.Timedelta(days=1)
        if prev is not None and pd.notna(prev)
        else pd.Timestamp("2020-01-01")
    )

    try:
        file_data = parse_ecos_ktb_file(ecos_file, asof=asof)
        if prev is not None and pd.notna(prev) and not file_data.empty:
            file_new = file_data[file_data["Date"] > prev].copy()
        else:
            file_new = file_data.copy()

        # If the refreshed ecos_rates.xlsx still ends before the requested as-of
        # date, optionally fill the remaining gap directly from ECOS.
        latest_source = None
        if not file_data.empty:
            latest_source = pd.to_datetime(file_data["Date"], errors="coerce").max()
        api_start = requested_start
        if latest_source is not None and pd.notna(latest_source):
            api_start = max(api_start, latest_source + pd.Timedelta(days=1))

        api_new = pd.DataFrame()
        if api_start <= asof and ecos_api_key:
            api_new = fetch_ecos_ktb_range(api_start, asof, ecos_api_key)

        pieces = [x for x in [file_new, api_new] if x is not None and not x.empty]
        new = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
        merged = merge_by_date(old, new)
        replace_sheets(path, {"KTB": merged})

        source_tail = None
        if not merged.empty and "Date" in merged:
            source_tail = pd.to_datetime(merged["Date"], errors="coerce").max()
        expected_end = pd.bdate_range(end=asof, periods=1)[0]
        if source_tail is not None and pd.notna(source_tail) and source_tail < expected_end:
            status = f"PARTIAL: KTB source through {source_tail:%Y-%m-%d}; refresh ecos_rates.xlsx or set ECOS_API_KEY"
        else:
            status = "OK"
        append_update_log(
            path, "KTB", asof, prev, requested_start, asof,
            max(len(merged) - len(old), 0), status,
        )
        log(f"KTB source: {ecos_file} | added {max(len(merged)-len(old),0)} rows | {status}")
        return merged
    except Exception as exc:
        log(f"KTB skipped: {exc}")
        append_update_log(path, "KTB", asof, prev, requested_start, asof, 0, f"FAILED: {exc}")
        return old


# ------------------------- KMBCO FX swap points -------------------------
def _kmb_to_number(x):
    """Convert KMBCO quote cell text to a float."""
    if x is None:
        return np.nan
    s = str(x).strip().replace(",", "").replace("−", "-").replace("–", "-")
    if s in {"", "-", "--", "nan", "None"}:
        return np.nan
    return pd.to_numeric(s, errors="coerce")


def parse_kmb_swap_table(html: str, d: pd.Timestamp) -> Optional[pd.DataFrame]:
    """Parse one KMBCO USD/KRW FX-swap BID/OFFER table.

    Returns
    -------
    None
        The expected table is not present / malformed.
    empty DataFrame
        A holiday/non-trading date (all quotes are zero or no valid mid quote).
    DataFrame
        Two rows (BID/OFFER) with Date, Side, 1M, 2M, 3M, 6M, 1Y.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    target = None
    for table in soup.find_all("table"):
        headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]
        normalized = [h.replace(" ", "") for h in headers]
        if all(x in normalized for x in ["구분", "1M", "2M", "3M", "6M", "1Y"]):
            target = table
            break
    if target is None:
        return None

    data = {}
    for tr in target.find_all("tr"):
        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
        if len(cells) < 6:
            continue
        side = cells[0].strip().upper()
        if side in {"BID", "OFFER"}:
            data[side] = {tenor: _kmb_to_number(v) for tenor, v in zip(KMB_SWAP_TENORS, cells[1:6])}

    if "BID" not in data or "OFFER" not in data:
        return None

    bid = pd.Series(data["BID"], dtype="float64")
    offer = pd.Series(data["OFFER"], dtype="float64")
    raw10 = pd.concat([bid, offer])
    all_zero = raw10.notna().all() and np.isclose(raw10.to_numpy(dtype=float), 0.0).all()
    mid = (bid + offer) / 2.0

    # KMBCO uses all-zero rows on some non-trading dates. Do not append them.
    if all_zero or all(pd.isna(mid[t]) for t in KMB_SWAP_TENORS):
        return pd.DataFrame(columns=["Date", "Side"] + KMB_SWAP_TENORS)

    rows = []
    for side, vals in [("BID", bid), ("OFFER", offer)]:
        row = {"Date": pd.Timestamp(d).normalize(), "Side": side}
        row.update({t: vals[t] for t in KMB_SWAP_TENORS})
        rows.append(row)
    return pd.DataFrame(rows, columns=["Date", "Side"] + KMB_SWAP_TENORS)


def set_kmb_swap_date(driver, date: pd.Timestamp, timeout: int = 15) -> Optional[str]:
    """Set KMBCO swap-date input (s_date_sh) and submit the search form."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    date_str = pd.Timestamp(date).strftime("%Y-%m-%d")
    wait = WebDriverWait(driver, timeout)
    inp = wait.until(EC.presence_of_element_located((By.ID, "s_date_sh")))

    driver.execute_script(
        """
        const el = arguments[0];
        const value = arguments[1];
        el.removeAttribute('readonly');
        el.value = value;
        el.dispatchEvent(new Event('input', {bubbles:true}));
        el.dispatchEvent(new Event('change', {bubbles:true}));
        """,
        inp,
        date_str,
    )

    form = inp.find_element(By.XPATH, "ancestor::form[1]")
    submitted = False
    candidates = []
    try:
        candidates += form.find_elements(By.XPATH, ".//button[contains(normalize-space(.), '검색')]")
        candidates += form.find_elements(By.XPATH, ".//input[@type='submit']")
        candidates += form.find_elements(By.XPATH, ".//a[contains(normalize-space(.), '검색')]")
        candidates += form.find_elements(By.XPATH, ".//a[.//img[contains(@alt,'검색')]]")
    except Exception:
        pass

    for btn in candidates:
        try:
            driver.execute_script("arguments[0].click();", btn)
            submitted = True
            break
        except Exception:
            continue
    if not submitted:
        driver.execute_script("arguments[0].submit();", form)

    wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
    time.sleep(0.20)
    try:
        return driver.find_element(By.ID, "s_date_sh").get_attribute("value")
    except Exception:
        return None


def fetch_kmb_swap_range(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Fetch only the missing weekday range from KMBCO in one browser session."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    days = pd.bdate_range(start=pd.Timestamp(start).normalize(), end=pd.Timestamp(end).normalize())
    if len(days) == 0:
        return pd.DataFrame(columns=["Date", "Side"] + KMB_SWAP_TENORS)

    drv = build_chrome(True)
    dfs = []
    try:
        drv.set_page_load_timeout(30)
        drv.get(KMB_SWAP_URL)
        WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.ID, "s_date_sh")))

        for i, d in enumerate(days, start=1):
            requested = d.strftime("%Y-%m-%d")
            success = False
            for attempt in range(2):
                try:
                    current_value = set_kmb_swap_date(drv, d)
                    if current_value and str(current_value).strip() != requested:
                        raise RuntimeError(
                            f"date submission mismatch: requested={requested}, page_input={current_value}"
                        )
                    x = parse_kmb_swap_table(drv.page_source, d)
                    if x is None:
                        raise RuntimeError(f"daily swap table not found for {requested}")
                    if x.empty:
                        log(f"Swap_Point {requested}: skip (holiday / no valid quote)")
                    else:
                        dfs.append(x)
                        mid = swap_mid(x)
                        if not mid.empty:
                            vals = " | ".join(f"{t}={mid.iloc[0][t]:g}" for t in KMB_SWAP_TENORS)
                            log(f"Swap_Point {requested}: {vals}")
                    success = True
                    break
                except Exception as exc:
                    if attempt == 0:
                        log(f"Swap_Point {requested}: retry after error: {exc}")
                        try:
                            drv.get(KMB_SWAP_URL)
                            WebDriverWait(drv, 15).until(
                                EC.presence_of_element_located((By.ID, "s_date_sh"))
                            )
                        except Exception:
                            pass
                    else:
                        log(f"Swap_Point {requested}: ERROR: {exc}")
            if not success:
                # Keep processing later dates; one bad day should not abort the weekly run.
                pass
            time.sleep(random.uniform(0.35, 0.70))
    finally:
        drv.quit()

    if not dfs:
        return pd.DataFrame(columns=["Date", "Side"] + KMB_SWAP_TENORS)
    return pd.concat(dfs, ignore_index=True)


def swap_mid(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["Date"] + KMB_SWAP_TENORS)
    rows=[]
    for d,g in raw.groupby('Date'):
        side=g['Side'].astype(str)
        bid=g[side.str.contains('bid',case=False,na=False)]
        offer=g[side.str.contains('offer',case=False,na=False)]
        if bid.empty or offer.empty:
            continue
        row={'Date':pd.Timestamp(d).normalize()}
        for c in KMB_SWAP_TENORS:
            b=pd.to_numeric(bid[c].iloc[0],errors='coerce')
            o=pd.to_numeric(offer[c].iloc[0],errors='coerce')
            row[c]=(b+o)/2 if pd.notna(b) and pd.notna(o) else np.nan
        rows.append(row)
    return pd.DataFrame(rows, columns=['Date'] + KMB_SWAP_TENORS).sort_values('Date') if rows else pd.DataFrame(columns=['Date'] + KMB_SWAP_TENORS)


def update_swap(path: Path, asof: pd.Timestamp) -> pd.DataFrame:
    """Incrementally append KMBCO FX-swap points to Swap_Point/Swap_Point_Raw.

    Window = business day after the latest stored MID row through the latest
    business day on/before ``--asof``. Existing history is never rewritten;
    duplicates are de-duplicated by Date (and Date+Side for raw quotes).
    """
    old = normalize_date_col(read_sheet(path, 'Swap_Point'))
    oldraw = normalize_date_col(read_sheet(path, 'Swap_Point_Raw'))
    prev = pd.to_datetime(old['Date'], errors='coerce').max() if not old.empty and 'Date' in old else None
    end = business_day_cutoff(asof)
    start = next_business_day(prev) if prev is not None and pd.notna(prev) else end - pd.offsets.BDay(35)

    if start > end:
        log(f"Swap_Point already current through {prev.date() if prev is not None and pd.notna(prev) else end.date()}")
        append_update_log(path, 'Swap_Point', asof, prev, start, end, 0, 'NO CHANGE')
        return old

    try:
        log(f"Swap_Point KMBCO incremental: {start.date()} -> {end.date()}")
        raw = fetch_kmb_swap_range(start, end)
        mid = swap_mid(raw)
        merged = merge_by_date(old, mid)

        if raw.empty:
            mergedraw = oldraw
        else:
            mergedraw = pd.concat([oldraw, raw], ignore_index=True) if not oldraw.empty else raw.copy()
            mergedraw['Date'] = pd.to_datetime(mergedraw['Date'], errors='coerce')
            mergedraw = mergedraw.dropna(subset=['Date'])
            mergedraw = (
                mergedraw.drop_duplicates(['Date','Side'], keep='last')
                         .sort_values(['Date','Side'])
                         .reset_index(drop=True)
            )

        replace_sheets(path, {'Swap_Point_Raw': mergedraw, 'Swap_Point': merged})
        added = max(len(merged) - len(old), 0)
        latest = pd.to_datetime(merged['Date'], errors='coerce').max() if not merged.empty and 'Date' in merged else prev
        if latest is not None and pd.notna(latest) and latest < end:
            status = f"PARTIAL: latest KMBCO quote {latest:%Y-%m-%d}; cutoff {end:%Y-%m-%d}"
        else:
            status = 'NO CHANGE' if added == 0 else 'OK'
        append_update_log(path, 'Swap_Point', asof, prev, start, end, added, status)
        log(f"Swap_Point added {added} rows | {status}")
        return merged
    except Exception as exc:
        log(f"Swap_Point skipped: {exc}")
        append_update_log(path, 'Swap_Point', asof, prev, start, end, 0, f"FAILED: {exc}")
        return old


# ------------------------- Derivatives -------------------------
def prep_rate(df: pd.DataFrame,prefix: str,cols: List[str]) -> pd.DataFrame:
    df=normalize_date_col(df)
    if df.empty or 'Date' not in df: return pd.DataFrame(columns=['Date'])
    keep=['Date']+[c for c in cols if c in df.columns]; x=df[keep].copy()
    return x.rename(columns={c:f'{prefix}_{c}' for c in keep if c!='Date'})


def build_derivatives(irs,crs,ktb,swap) -> pd.DataFrame:
    tenors=['1Y','3Y','5Y','10Y']; d=prep_rate(irs,'IRS',tenors)
    for x in [prep_rate(crs,'CRS',tenors),prep_rate(ktb,'KTB',tenors),prep_rate(swap,'SwapPoint',['1M','3M','6M','1Y'])]:
        d=pd.merge(d,x,on='Date',how='outer')
    if d.empty: return d
    d=d.sort_values('Date')
    for t in tenors:
        if f'IRS_{t}' in d and f'KTB_{t}' in d: d[f'BondSwapSpread_{t}_bp']=(d[f'IRS_{t}']-d[f'KTB_{t}'])*100
        if f'CRS_{t}' in d and f'IRS_{t}' in d: d[f'SwapBasis_{t}_bp']=(d[f'CRS_{t}']-d[f'IRS_{t}'])*100
    return d.reset_index(drop=True)


def weekly_snapshots(d: pd.DataFrame,n=4) -> pd.DataFrame:
    if d.empty: return d
    x=d.copy(); x['Date']=pd.to_datetime(x['Date']); iso=x['Date'].dt.isocalendar(); x['_week']=iso['year'].astype(str)+'-'+iso['week'].astype(str)
    return x.sort_values('Date').groupby('_week',as_index=False).tail(1).tail(n).drop(columns='_week').reset_index(drop=True)


# ------------------------- Charts -------------------------
def mpl():
    import matplotlib.pyplot as plt
    plt.rcParams['font.family']='Calibri'; plt.rcParams['axes.unicode_minus']=False
    return plt


def chart_dir(path: Path) -> Path:
    d=path.parent/'fx_chart_images'; d.mkdir(exist_ok=True); return d


def plot_single(x,y,title,outfile,ylabel=None):
    plt=mpl(); fig,ax=plt.subplots(figsize=(10.8,4.25)); ax.plot(x,y,linewidth=1.35,color=MAIN)
    ax.set_title(title,fontsize=13,fontweight='bold'); ax.grid(True,axis='y',alpha=.2); ax.tick_params(axis='x',rotation=63)
    if ylabel: ax.set_ylabel(ylabel)
    fig.tight_layout(); fig.savefig(outfile,dpi=180,bbox_inches='tight'); plt.close(fig)


def plot_two(df1,x1,y1,l1,df2,x2,y2,l2,title,outfile):
    plt=mpl(); fig,ax1=plt.subplots(figsize=(11.2,4.6)); ax2=ax1.twinx()
    ax1.plot(df1[x1],df1[y1],linewidth=1.35,color=MAIN,label=l1); ax2.plot(df2[x2],df2[y2],linewidth=1.15,color=AUX,label=l2)
    ax1.set_title(title,fontsize=14,fontweight='bold'); ax1.grid(True,axis='y',alpha=.2); ax1.tick_params(axis='x',rotation=63)
    lines=ax1.get_lines()+ax2.get_lines(); ax1.legend(lines,[z.get_label() for z in lines],loc='upper left',frameon=False)
    fig.tight_layout(); fig.savefig(outfile,dpi=180,bbox_inches='tight'); plt.close(fig)


def generate_charts(path: Path, asof: pd.Timestamp) -> List[Path]:
    d=chart_dir(path); made=[]
    week_start, week_end = report_week_bounds(asof)
    week_label = f"{week_start:%b %d}–{week_end:%b %d, %Y}"
    # Individual intraday sheets
    for sheet,ticker in INTRADAY.items():
        df=read_sheet(path,sheet)
        if df.empty or 'Datetime_KST' not in df or 'Close' not in df: continue
        df=filter_report_week(df,asof,'Datetime_KST')
        if df.empty: continue
        label={'5min_usdkrw':'USD/KRW','5min_usdjpy':'USD/JPY','5min_gbpusd':'GBP/USD','5min_eurusd':'EUR/USD','5min_dxy':'DXY'}[sheet]
        out=d/f'{sheet}.png'; plot_single(df['Datetime_KST'],pd.to_numeric(df['Close'],errors='coerce'),f'{label} — 5 Minute ({week_label})',out); made.append(out)

    krw=read_sheet(path,'5min_usdkrw'); jpy=read_sheet(path,'5min_usdjpy')
    if not krw.empty and not jpy.empty and 'Close' in krw and 'Close' in jpy:
        krw=filter_report_week(krw,asof,'Datetime_KST'); jpy=filter_report_week(jpy,asof,'Datetime_KST')
        if not krw.empty and not jpy.empty:
            out=d/'ppt_5min_usdkrw_usdjpy.png'; plot_two(krw,'Datetime_KST','Close','USD/KRW (lhs)',jpy,'Datetime_KST','Close','USD/JPY (rhs)',f'Weekly 5 Minute Chart of USD/KRW vs. USD/JPY ({week_label})',out); made.append(out)
    eur=read_sheet(path,'5min_eurusd'); gbp=read_sheet(path,'5min_gbpusd')
    if not eur.empty and not gbp.empty and 'USD_EUR' in eur and 'USD_GBP' in gbp:
        eur=filter_report_week(eur,asof,'Datetime_KST'); gbp=filter_report_week(gbp,asof,'Datetime_KST')
        if not eur.empty and not gbp.empty:
            out=d/'ppt_5min_usdeur_usdgbp.png'; plot_two(eur,'Datetime_KST','USD_EUR','USD/EUR (lhs)',gbp,'Datetime_KST','USD_GBP','USD/GBP (rhs)',f'Weekly 5 Minute Chart of USD/EUR vs. USD/GBP ({week_label})',out); made.append(out)

    # Swap charts
    deriv=normalize_date_col(read_sheet(path,'Derivatives'))
    if not deriv.empty:
        recent=deriv[deriv['Date']>=deriv['Date'].max()-pd.Timedelta(days=45)].copy()
        for prefix,title,fname in [('BondSwapSpread','Bond Swap Spread (IRS - KTB)','bond_swap_spread.png'),('SwapBasis','Swap Basis (CRS - IRS)','swap_basis.png')]:
            cols=[f'{prefix}_{t}_bp' for t in ['1Y','3Y','5Y','10Y'] if f'{prefix}_{t}_bp' in recent]
            if cols:
                plt=mpl(); fig,ax=plt.subplots(figsize=(10.5,4.2)); colors=[MAIN,AUX,GREY,GOLD]
                for c,clr in zip(cols,colors): ax.plot(recent['Date'],recent[c],linewidth=1.2,label=c.replace(prefix+'_','').replace('_bp',''),color=clr)
                ax.axhline(0,linewidth=.7,color='black',alpha=.5); ax.set_title(title,fontsize=13,fontweight='bold'); ax.set_ylabel('bp'); ax.grid(True,axis='y',alpha=.2); ax.tick_params(axis='x',rotation=63); ax.legend(frameon=False,ncol=4)
                fig.tight_layout(); out=d/fname; fig.savefig(out,dpi=180,bbox_inches='tight'); plt.close(fig); made.append(out)

    # MA5 mini-charts used across the regional slides
    ma=normalize_date_col(read_sheet(path,'MA5'))
    if not ma.empty:
        for ccy in HISTORICAL_TREND_PAIRS:
            if ccy not in ma: continue
            y=pd.to_numeric(ma[ccy],errors='coerce')
            if y.notna().sum()<2: continue
            out=d/f'ma5_{ccy.lower()}.png'; plot_single(ma['Date'],y,f"{ccy.replace('_','/')} (5 Days Moving Average)",out); made.append(out)
    return made


def insert_chart_images(path: Path, images: List[Path]) -> None:
    if not images: return
    from openpyxl import load_workbook
    from openpyxl.drawing.image import Image
    wb=load_workbook(path)
    ws=wb['Charts'] if 'Charts' in wb.sheetnames else wb.create_sheet('Charts')
    ws.delete_rows(1,ws.max_row); ws._images=[]; ws['A1']='PPT Chart Snapshots'; ws['A2']='Generated by fx_weekly_updater.py'
    row=4
    for p in images:
        ws.cell(row=row,column=1,value=p.stem); img=Image(str(p)); img.width=min(img.width,900); img.height=min(img.height,370); ws.add_image(img,f'A{row+1}'); row+=27

    # Put one native 5m snapshot in each requested 5m sheet.
    for sheet in ['5min_usdkrw','5min_usdjpy','5min_gbpusd','5min_eurusd','5min_dxy']:
        p=path.parent/'fx_chart_images'/f'{sheet}.png'
        if p.exists() and sheet in wb.sheetnames:
            sh=wb[sheet]; sh._images=[]; img=Image(str(p)); img.width=850; img.height=340; sh.add_image(img,'J2')
    wb.save(path)


# ------------------------- Excel styling -------------------------
def style_workbook(path: Path) -> None:
    from openpyxl import load_workbook
    from openpyxl.styles import Alignment,Font,PatternFill,Border,Side
    from openpyxl.formatting.rule import ColorScaleRule
    wb=load_workbook(path); navy='002C5F'; thin=Side(style='thin',color='E3E6E8')
    pct_headers={'WoW(%)','MoM(%)','YTD(%)','Deviation from 15Y High (%)','MDD(%)','Vol 20D(%)'}
    for ws in wb.worksheets:
        ws.freeze_panes='A2'
        if ws.max_row>=1:
            for cell in ws[1]:
                cell.fill=PatternFill('solid',fgColor=navy); cell.font=Font(color='FFFFFF',bold=True,name='Calibri'); cell.alignment=Alignment(horizontal='center',vertical='center',wrap_text=True)
        # Keep styling light: header + key number formats only.
        # Avoid applying per-cell styles to large 5-minute/raw sheets, which makes XLSX files heavy.
        headers={cell.value:cell.column for cell in ws[1] if cell.value is not None}
        for h,ci in headers.items():
            if h in pct_headers:
                for rr in range(2,ws.max_row+1): ws.cell(rr,ci).number_format='0.00%'
            if h in {'Date','AsOf'}:
                for rr in range(2,ws.max_row+1): ws.cell(rr,ci).number_format='yyyy-mm-dd'
            if h=='Datetime_KST':
                for rr in range(2,ws.max_row+1): ws.cell(rr,ci).number_format='yyyy-mm-dd hh:mm'
        for ci in range(1, ws.max_column + 1):
            letter=ws.cell(1,ci).column_letter; maxlen=0
            for rr in range(1, min(ws.max_row,150) + 1):
                v=ws.cell(rr,ci).value
                maxlen=max(maxlen,len(str(v)) if v is not None else 0)
            ws.column_dimensions[letter].width=min(max(maxlen+2,11),26)
    if 'Daily_Heatmap' in wb.sheetnames:
        ws=wb['Daily_Heatmap']
        if ws.max_row>=2 and ws.max_column>=2:
            rng=f'B2:{ws.cell(ws.max_row,ws.max_column).coordinate}'
            ws.conditional_formatting.add(rng,ColorScaleRule(start_type='num',start_value=-.035,start_color='0070C0',mid_type='num',mid_value=0,mid_color='FFFFFF',end_type='num',end_value=.035,end_color='C00000'))
            for row in ws.iter_rows(min_row=2,min_col=2):
                for cell in row: cell.number_format='0.00%'
    wb.save(path)


def update_derivatives(path: Path,irs,crs,ktb,swap) -> pd.DataFrame:
    d=build_derivatives(irs,crs,ktb,swap); replace_sheets(path,{'Derivatives':d,'Derivatives_Weekly':weekly_snapshots(d,4)}); return d


def resolve_ecos_api_key(cli_key: Optional[str], workbook_path: Path) -> Optional[str]:
    """Resolve ECOS key without requiring a second command-line argument.

    Priority: --ecos-api-key -> ECOS_API_KEY env -> sibling get_rates.py.
    Importing get_rates.py is safe because its downloader now only runs under
    `if __name__ == "__main__"`.
    """
    if cli_key:
        return cli_key.strip() or None
    env_key = os.getenv("ECOS_API_KEY", "").strip()
    if env_key:
        return env_key
    candidate = workbook_path.parent / "get_rates.py"
    if candidate.exists():
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("fx_local_get_rates", candidate)
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                key = str(getattr(mod, "ECOS_KEY", "")).strip()
                if key:
                    return key
        except Exception as exc:
            log(f"Could not load ECOS key from get_rates.py: {exc}")
    return None


def main() -> None:
    ap=argparse.ArgumentParser()
    ap.add_argument('--workbook',default='FX_Weekly_Automation_Light.xlsx')
    ap.add_argument('--asof',default=None,help='Report cutoff, e.g. 2026-09-20')
    ap.add_argument('--skip-selenium',action='store_true',help='Skip KMBCO IRS/CRS and FX-swap Selenium updates')
    ap.add_argument('--skip-intraday',action='store_true')
    ap.add_argument('--ecos-file',default=None,help='Path to ecos_rates.xlsx; default: workbook folder/ecos_rates.xlsx')
    ap.add_argument('--ecos-api-key',default=None,help='Optional BOK ECOS key; preferably use ECOS_API_KEY environment variable')
    args=ap.parse_args()

    path=Path(args.workbook).expanduser().resolve(); asof=parse_asof(args.asof); ensure_workbook(path)
    log(f'Workbook: {path}'); log(f'As-of: {asof.date()}')
    _ws, _we = report_week_bounds(asof)
    log(f'5-minute report week: {_ws.date()} -> {_we.date()}')

    fx=fetch_fx_daily(path,asof); write_fx_derived(path,fx,asof)
    if not args.skip_intraday: update_intraday(path,asof)

    if args.skip_selenium:
        irs=normalize_date_col(read_sheet(path,'IRS')); crs=normalize_date_col(read_sheet(path,'CRS')); swap=normalize_date_col(read_sheet(path,'Swap_Point'))
    else:
        irs,crs=update_irs_crs(path,asof); swap=update_swap(path,asof)
    ecos_file=Path(args.ecos_file).expanduser().resolve() if args.ecos_file else (path.parent/'ecos_rates.xlsx')
    ecos_api_key=resolve_ecos_api_key(args.ecos_api_key,path)
    ktb=update_ktb(path,asof,ecos_file,ecos_api_key)
    update_derivatives(path,irs,crs,ktb,swap)

    try:
        imgs=generate_charts(path,asof); insert_chart_images(path,imgs)
    except Exception as exc: log(f'Chart generation skipped: {exc}')
    try: style_workbook(path)
    except Exception as exc: log(f'Workbook styling skipped: {exc}')
    log('Done')


if __name__=='__main__':
    main()
