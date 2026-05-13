#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

What this file contains:
    - Polygon-compatible minute bar fetcher
    - Intraday ladder-pattern detector
    - Weighted market bias score
    - Per-ticker day bias score
    - Local browser dashboard
    - Optional email / Discord alert hooks

What this file intentionally does NOT contain:
    - API keys
    - Email addresses
    - SMTP passwords
    - Discord webhook URLs
    - Personal account size
    - Any user-specific file paths

Run example:
    py shareable_live_ladder_dashboard.py --api-key YOUR_POLYGON_KEY --tickers AAPL,NVDA,MSFT

Then open:
    http://127.0.0.1:8765

Optional environment variables:
    POLYGON_API_KEY
    LADDER_DISCORD_WEBHOOK_URL
    LADDER_SMTP_HOST
    LADDER_SMTP_PORT
    LADDER_SMTP_USER
    LADDER_SMTP_PASSWORD
    LADDER_EMAIL_FROM
    LADDER_EMAIL_TO

This is research software, not investment advice.
"""

from __future__ import annotations

import argparse
import concurrent.futures as futures
import datetime as dt
from email.message import EmailMessage
import json
import math
import os
import socket
import smtplib
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo


NY_ZONE = ZoneInfo("America/New_York")
DEFAULT_BASE_URL = "https://api.polygon.io"
DEFAULT_MARKET_CONTEXT_TICKERS = "SPY,QQQ"
DEFAULT_NASDAQ_BIAS_TICKERS = "QQQ,SPY,NVDA,AAPL,MSFT,AMZN,META,GOOGL,GOOG,AVGO,TSLA,AMD,SOXL,LRCX,MRVL,MU,WDC,STX"
DEFAULT_MEGA_CAP_TICKERS = "NVDA,AAPL,MSFT,AMZN,META,GOOGL,GOOG,AVGO,TSLA"
DEFAULT_SEMI_TICKERS = "NVDA,AVGO,AMD,SOXL,LRCX,MRVL,MU,WDC,STX"


class DashboardEngine:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.api_key = resolve_api_key(args)
        self.monitor_tickers = load_monitor_tickers(args)
        self.fetch_tickers = dedupe_keep_order(
            self.monitor_tickers
            + parse_tickers(args.market_context_tickers)
            + parse_tickers(args.nasdaq_bias_tickers)
        )
        self.email_notifier = EmailNotifier(args)
        self.discord_notifier = DiscordNotifier(args)
        self.data_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        self.captured_records: Dict[str, Dict[str, Any]] = {}
        self.seen_alert_keys: set[str] = set()
        self.last_scan_date: Optional[dt.date] = None
        self.lock = threading.RLock()
        self.refresh_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.last_refresh_finished_at: Optional[float] = None
        self.state: Dict[str, Any] = {
            "status": "starting",
            "message": "Waiting for first refresh.",
            "rows": [],
            "row_count": 0,
            "loaded_count": 0,
            "ticker_count": len(self.monitor_tickers),
            "fetch_ticker_count": len(self.fetch_tickers),
            "scan_date": "",
            "updated_at": "",
            "next_refresh_in": int(args.refresh_seconds),
            "min_price_change_pct": float(args.min_price_change_pct),
            "min_bar_volume": float(args.min_bar_volume),
            "account_capital": float(args.account_capital),
            "market_context": {},
            "errors": {},
            "error_count": 0,
            "email_alerts": self.email_notifier.enabled,
            "email_configured": self.email_notifier.ready(),
            "discord_alerts": self.discord_notifier.enabled,
            "discord_configured": bool(self.discord_notifier.webhook_url),
        }

    def start(self) -> None:
        thread = threading.Thread(target=self._loop, name="shareable-dashboard-refresh", daemon=True)
        thread.start()

    def stop(self) -> None:
        self.stop_event.set()

    def trigger_refresh(self) -> bool:
        if self.refresh_lock.locked():
            return False
        thread = threading.Thread(target=self.refresh_once, name="manual-refresh", daemon=True)
        thread.start()
        return True

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            self.refresh_once()
            wait_s = max(1, int(self.args.refresh_seconds))
            for _ in range(wait_s):
                if self.stop_event.is_set():
                    return
                time.sleep(1)

    def refresh_once(self) -> None:
        if not self.refresh_lock.acquire(blocking=False):
            return
        with self.lock:
            self.state["status"] = "refreshing"
            self.state["message"] = "Refreshing minute bars."
        try:
            now_ny = dt.datetime.now(tz=NY_ZONE)
            scan_date = resolve_scan_date(self.args.scan_date, now_ny)
            if self.last_scan_date is not None and self.last_scan_date != scan_date:
                self.data_by_ticker = {}
                self.captured_records = {}
                self.seen_alert_keys = set()
            self.last_scan_date = scan_date

            start_dt, end_dt = session_bounds(scan_date, now_ny, self.args)
            ranges = next_ranges_for_update(self.fetch_tickers, self.data_by_ticker, start_dt, end_dt)
            incoming, errors = fetch_many_minute_bars(
                ranges=ranges,
                api_key=self.api_key,
                base_url=str(self.args.base_url),
                adjusted=bool(self.args.adjusted),
                timeout=int(self.args.timeout),
                workers=int(self.args.workers),
            )
            for ticker, bars in incoming.items():
                self.data_by_ticker[ticker] = merge_bars(self.data_by_ticker.get(ticker, []), bars)

            market_context = build_market_context(self.data_by_ticker, self.args)
            raw_records = scan_all_tickers(self.monitor_tickers, self.data_by_ticker, self.args, market_context)
            display_records = self.capture_records(raw_records)
            new_records = self.take_new_alert_records(raw_records)
            if new_records:
                self.notify(new_records)

            loaded_count = sum(1 for ticker in self.monitor_tickers if self.data_by_ticker.get(ticker))
            with self.lock:
                self.last_refresh_finished_at = time.time()
                self.state = {
                    "status": "ok",
                    "message": "Dashboard updated.",
                    "rows": display_records,
                    "row_count": len(display_records),
                    "current_row_count": len(raw_records),
                    "loaded_count": loaded_count,
                    "ticker_count": len(self.monitor_tickers),
                    "fetch_ticker_count": len(self.fetch_tickers),
                    "scan_date": scan_date.isoformat(),
                    "updated_at": now_ny.strftime("%Y-%m-%d %H:%M:%S %Z"),
                    "refresh_seconds": int(self.args.refresh_seconds),
                    "next_refresh_in": int(self.args.refresh_seconds),
                    "min_price_change_pct": float(self.args.min_price_change_pct),
                    "min_bar_volume": float(self.args.min_bar_volume),
                    "account_capital": float(self.args.account_capital),
                    "market_context": market_context,
                    "errors": dict(list(errors.items())[:20]),
                    "error_count": len(errors),
                    "email_alerts": self.email_notifier.enabled,
                    "email_configured": self.email_notifier.ready(),
                    "email_error": self.email_notifier.last_error,
                    "discord_alerts": self.discord_notifier.enabled,
                    "discord_configured": bool(self.discord_notifier.webhook_url),
                    "discord_error": self.discord_notifier.last_error,
                }
        except Exception as exc:
            with self.lock:
                self.state["status"] = "error"
                self.state["message"] = str(exc)
                self.state["updated_at"] = dt.datetime.now(tz=NY_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
        finally:
            self.refresh_lock.release()

    def capture_records(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not bool(self.args.keep_captured_signals):
            return sort_display_rows(mark_record_status(rows, "active"))[: int(self.args.max_rows)]

        active_keys = {record_key(row) for row in rows}
        now_text = dt.datetime.now(tz=NY_ZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
        for row in rows:
            key = record_key(row)
            existing = self.captured_records.get(key, {})
            clean = dict(row)
            clean["record_status"] = "active"
            clean["first_seen_at"] = existing.get("first_seen_at") or now_text
            clean["last_seen_at"] = now_text
            self.captured_records[key] = clean

        for key, row in list(self.captured_records.items()):
            row["record_status"] = "active" if key in active_keys else "captured"

        sorted_rows = sort_display_rows(list(self.captured_records.values()))
        max_captured = max(1, int(self.args.max_captured_signals))
        self.captured_records = {record_key(row): row for row in sorted_rows[:max_captured]}
        return sort_display_rows(list(self.captured_records.values()))[: int(self.args.max_rows)]

    def take_new_alert_records(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in sort_display_rows(rows):
            key = record_key(row)
            if key in self.seen_alert_keys:
                continue
            self.seen_alert_keys.add(key)
            if not bool(self.args.prime_alerts_on_start) or self.last_refresh_finished_at is not None:
                out.append(row)
        return out[: max(1, int(self.args.max_alert_rows))]

    def notify(self, rows: List[Dict[str, Any]]) -> None:
        with futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="alert") as pool:
            tasks = [
                pool.submit(self.email_notifier.send, rows),
                pool.submit(self.discord_notifier.send, rows),
            ]
            for task in futures.as_completed(tasks):
                task.result()

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            snap = dict(self.state)
        if self.last_refresh_finished_at:
            elapsed = max(0, int(time.time() - self.last_refresh_finished_at))
            snap["next_refresh_in"] = max(0, int(self.args.refresh_seconds) - elapsed)
        return snap


def resolve_api_key(args: argparse.Namespace) -> str:
    api_key = str(args.api_key or os.environ.get("POLYGON_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Missing API key. Use --api-key or set POLYGON_API_KEY.")
    return api_key


def parse_tickers(value: str) -> List[str]:
    out: List[str] = []
    for item in str(value or "").replace(";", ",").split(","):
        ticker = normalize_ticker(item)
        if ticker:
            out.append(ticker)
    return dedupe_keep_order(out)


def normalize_ticker(value: Any) -> str:
    return str(value or "").strip().upper().replace("/", ".")


def dedupe_keep_order(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for value in values:
        item = normalize_ticker(value)
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def load_monitor_tickers(args: argparse.Namespace) -> List[str]:
    tickers = parse_tickers(str(args.tickers or ""))
    path = Path(str(args.tickers_file or ""))
    if path.exists():
        for raw in path.read_text(encoding="utf-8").replace("\n", ",").split(","):
            ticker = normalize_ticker(raw)
            if ticker:
                tickers.append(ticker)
    tickers = dedupe_keep_order(tickers)
    if not tickers:
        raise RuntimeError("No tickers provided. Use --tickers or --tickers-file.")
    return tickers


def resolve_scan_date(value: str, now_ny: dt.datetime) -> dt.date:
    if not value or str(value).lower() == "auto":
        date_value = now_ny.date()
        while date_value.weekday() >= 5:
            date_value -= dt.timedelta(days=1)
        return date_value
    return dt.date.fromisoformat(str(value))


def parse_hhmm(value: str, fallback: dt.time) -> dt.time:
    try:
        hour, minute = str(value).split(":", 1)
        return dt.time(int(hour), int(minute))
    except Exception:
        return fallback


def session_bounds(scan_date: dt.date, now_ny: dt.datetime, args: argparse.Namespace) -> tuple[dt.datetime, dt.datetime]:
    start_time = parse_hhmm(str(args.start_time), dt.time(4, 0))
    end_time = parse_hhmm(str(args.end_time), dt.time(20, 0)) if str(args.end_time).lower() != "auto" else None
    start_dt = dt.datetime.combine(scan_date, start_time, tzinfo=NY_ZONE)
    if end_time is None and scan_date == now_ny.date():
        end_dt = now_ny
    else:
        end_dt = dt.datetime.combine(scan_date, end_time or dt.time(20, 0), tzinfo=NY_ZONE)
    return start_dt, max(start_dt, end_dt)


def next_ranges_for_update(
    tickers: List[str],
    data_by_ticker: Dict[str, List[Dict[str, Any]]],
    start_dt: dt.datetime,
    end_dt: dt.datetime,
) -> Dict[str, tuple[dt.datetime, dt.datetime]]:
    ranges: Dict[str, tuple[dt.datetime, dt.datetime]] = {}
    for ticker in tickers:
        bars = data_by_ticker.get(ticker, [])
        if bars:
            last_ts = max(bar["ts"] for bar in bars)
            fetch_start = max(start_dt, last_ts + dt.timedelta(minutes=1))
        else:
            fetch_start = start_dt
        if fetch_start <= end_dt:
            ranges[ticker] = (fetch_start, end_dt)
    return ranges


def fetch_many_minute_bars(
    ranges: Dict[str, tuple[dt.datetime, dt.datetime]],
    api_key: str,
    base_url: str,
    adjusted: bool,
    timeout: int,
    workers: int,
) -> tuple[Dict[str, List[Dict[str, Any]]], Dict[str, str]]:
    incoming: Dict[str, List[Dict[str, Any]]] = {}
    errors: Dict[str, str] = {}
    if not ranges:
        return incoming, errors

    def one(item: tuple[str, tuple[dt.datetime, dt.datetime]]) -> tuple[str, List[Dict[str, Any]], str]:
        ticker, (start_dt, end_dt) = item
        try:
            return ticker, fetch_minute_bars(ticker, start_dt, end_dt, api_key, base_url, adjusted, timeout), ""
        except Exception as exc:
            return ticker, [], str(exc)

    with futures.ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        for ticker, bars, err in pool.map(one, ranges.items()):
            if err:
                errors[ticker] = err
            else:
                incoming[ticker] = bars
    return incoming, errors


def fetch_minute_bars(
    ticker: str,
    start_dt: dt.datetime,
    end_dt: dt.datetime,
    api_key: str,
    base_url: str,
    adjusted: bool,
    timeout: int,
) -> List[Dict[str, Any]]:
    start_ms = int(start_dt.astimezone(dt.timezone.utc).timestamp() * 1000)
    end_ms = int(end_dt.astimezone(dt.timezone.utc).timestamp() * 1000)
    path = f"/v2/aggs/ticker/{urllib.parse.quote(ticker, safe='')}/range/1/minute/{start_ms}/{end_ms}"
    payload = request_json(
        base_url=base_url,
        path=path,
        params={
            "adjusted": "true" if adjusted else "false",
            "sort": "asc",
            "limit": "50000",
            "apiKey": api_key,
        },
        timeout=timeout,
    )
    rows = payload.get("results") or []
    bars: List[Dict[str, Any]] = []
    for row in rows:
        ts_ms = row.get("t")
        if ts_ms is None:
            continue
        ts = dt.datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=dt.timezone.utc).astimezone(NY_ZONE)
        bars.append(
            {
                "ts": ts,
                "open": safe_float(row.get("o")),
                "high": safe_float(row.get("h")),
                "low": safe_float(row.get("l")),
                "close": safe_float(row.get("c")),
                "volume": float(safe_float(row.get("v")) or 0.0),
            }
        )
    return [bar for bar in bars if bar["close"] is not None and bar["open"] is not None]


def request_json(base_url: str, path: str, params: Dict[str, Any], timeout: int) -> Dict[str, Any]:
    query = urllib.parse.urlencode(params)
    url = f"{base_url.rstrip('/')}{path}?{query}"
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "shareable-ladder-dashboard/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8")
    payload = json.loads(raw)
    status = str(payload.get("status") or "").upper()
    if status in {"ERROR", "NOT_AUTHORIZED"}:
        raise RuntimeError(str(payload.get("error") or payload.get("message") or status))
    return payload


def merge_bars(old: List[Dict[str, Any]], new: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_ts: Dict[dt.datetime, Dict[str, Any]] = {bar["ts"]: bar for bar in old if bar.get("ts")}
    for bar in new:
        if bar.get("ts"):
            by_ts[bar["ts"]] = bar
    return [by_ts[ts] for ts in sorted(by_ts)]


def scan_all_tickers(
    tickers: List[str],
    data_by_ticker: Dict[str, List[Dict[str, Any]]],
    args: argparse.Namespace,
    market_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for ticker in tickers:
        rows.extend(scan_ticker(ticker, data_by_ticker.get(ticker, []), args, market_context))
    return sort_display_rows(rows)[: int(args.max_rows)]


def scan_ticker(
    ticker: str,
    bars: List[Dict[str, Any]],
    args: argparse.Namespace,
    market_context: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if len(bars) < max(3, int(args.min_window_minutes)):
        return []

    min_window = max(2, int(args.min_window_minutes))
    max_window = max(min_window, int(args.max_window_minutes))
    min_move = abs(float(args.min_price_change_pct))
    min_volume = float(args.min_bar_volume)
    candidates: List[Dict[str, Any]] = []
    closes = [float(bar["close"]) for bar in bars]

    for end_i in range(1, len(bars)):
        for length in range(min_window, max_window + 1):
            start_i = end_i - length + 1
            if start_i < 0:
                continue
            window_bars = bars[start_i : end_i + 1]
            if min_volume > 0 and any(float(bar.get("volume") or 0.0) < min_volume for bar in window_bars):
                continue
            start_price = closes[start_i]
            end_price = closes[end_i]
            if start_price <= 0:
                continue
            change_pct = (end_price / start_price - 1.0) * 100.0
            if abs(change_pct) < min_move:
                continue
            direction = "up" if change_pct > 0 else "down"
            window_closes = closes[start_i : end_i + 1]
            quality = ladder_quality(window_closes, window_bars, direction)
            if quality["monotonic_ratio"] < float(args.min_monotonic_ratio):
                continue
            if quality["efficiency"] < float(args.min_efficiency):
                continue
            if quality["r2"] < float(args.min_r2):
                continue
            if quality["same_color_ratio"] < float(args.min_same_color_ratio):
                continue

            row = {
                "ticker": ticker,
                "date": bars[end_i]["ts"].date().isoformat(),
                "time_window": f"{format_hhmm(bars[start_i]['ts'])}-{format_hhmm(bars[end_i]['ts'])}",
                "start_ts": bars[start_i]["ts"].isoformat(),
                "end_ts": bars[end_i]["ts"].isoformat(),
                "direction": direction,
                "price_change_pct": round(float(change_pct), 3),
                "start_price": round(float(start_price), 4),
                "end_price": round(float(end_price), 4),
                "window_minutes": length,
                "monotonic_ratio": round(float(quality["monotonic_ratio"]), 3),
                "efficiency": round(float(quality["efficiency"]), 3),
                "r2": round(float(quality["r2"]), 3),
                "same_color_ratio": round(float(quality["same_color_ratio"]), 3),
                "window_volume": int(sum(float(bar.get("volume") or 0.0) for bar in window_bars)),
            }
            attach_position(row, float(args.account_capital))
            row.update(ticker_day_bias(ticker, bars, market_context, int(args.context_lookback_minutes)))
            candidates.append(row)

    return reduce_overlapping_candidates(candidates, max_per_ticker=int(args.max_windows_per_ticker))


def ladder_quality(closes: List[float], bars: List[Dict[str, Any]], direction: str) -> Dict[str, float]:
    sign = 1.0 if direction == "up" else -1.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    if not deltas:
        return {"monotonic_ratio": 0.0, "efficiency": 0.0, "r2": 0.0, "same_color_ratio": 0.0}
    same_direction = sum(1 for delta in deltas if delta * sign > 0)
    path = sum(abs(delta) for delta in deltas)
    net = abs(closes[-1] - closes[0])
    color_count = 0
    for bar in bars:
        open_price = float(bar.get("open") or 0.0)
        close_price = float(bar.get("close") or 0.0)
        if (close_price - open_price) * sign >= 0:
            color_count += 1
    return {
        "monotonic_ratio": same_direction / len(deltas),
        "efficiency": net / path if path > 0 else 0.0,
        "r2": linear_r2(closes),
        "same_color_ratio": color_count / len(bars) if bars else 0.0,
    }


def linear_r2(values: List[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mean_x = sum(xs) / n
    mean_y = sum(values) / n
    ss_xx = sum((x - mean_x) ** 2 for x in xs)
    ss_yy = sum((y - mean_y) ** 2 for y in values)
    if ss_xx <= 0 or ss_yy <= 0:
        return 0.0
    ss_xy = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    return max(0.0, min(1.0, (ss_xy * ss_xy) / (ss_xx * ss_yy)))


def reduce_overlapping_candidates(candidates: List[Dict[str, Any]], max_per_ticker: int) -> List[Dict[str, Any]]:
    candidates = sorted(
        candidates,
        key=lambda row: (parse_iso_dt(row["end_ts"]), abs(float(row["price_change_pct"]))),
        reverse=True,
    )
    kept: List[Dict[str, Any]] = []
    for row in candidates:
        if any(same_signal_overlap(row, existing) for existing in kept):
            continue
        kept.append(row)
        if len(kept) >= max(1, max_per_ticker):
            break
    return kept


def same_signal_overlap(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    if a.get("ticker") != b.get("ticker") or a.get("direction") != b.get("direction"):
        return False
    a_start, a_end = parse_iso_dt(a["start_ts"]), parse_iso_dt(a["end_ts"])
    b_start, b_end = parse_iso_dt(b["start_ts"]), parse_iso_dt(b["end_ts"])
    overlap = max(dt.timedelta(0), min(a_end, b_end) - max(a_start, b_start)).total_seconds()
    shorter = max(60.0, min((a_end - a_start).total_seconds(), (b_end - b_start).total_seconds()))
    return overlap / shorter >= 0.50


def build_market_context(data_by_ticker: Dict[str, List[Dict[str, Any]]], args: argparse.Namespace) -> Dict[str, Any]:
    lookback = max(5, int(args.context_lookback_minutes))
    proxies: Dict[str, Dict[str, Any]] = {}
    for ticker in parse_tickers(args.market_context_tickers):
        snap = ticker_snapshot(ticker, data_by_ticker.get(ticker, []), lookback)
        if snap:
            proxies[ticker] = snap

    lookbacks = [float(item["lookback_change_pct"]) for item in proxies.values()]
    sessions = [float(item["session_change_pct"]) for item in proxies.values()]
    efficiencies = [float(item["efficiency"]) for item in proxies.values()]
    market_lookback = sum(lookbacks) / len(lookbacks) if lookbacks else 0.0
    market_session = sum(sessions) / len(sessions) if sessions else 0.0
    market_efficiency = sum(efficiencies) / len(efficiencies) if efficiencies else 0.0
    regime = classify_regime(market_lookback, market_session, market_efficiency)
    bias = build_weighted_nasdaq_bias(data_by_ticker, args, lookback)
    summary_parts = [f"{ticker} {snap['lookback_change_pct']:+.2f}%" for ticker, snap in sorted(proxies.items())]
    return {
        "lookback_minutes": lookback,
        "proxies": proxies,
        "market_lookback_pct": round(market_lookback, 3),
        "market_session_pct": round(market_session, 3),
        "market_efficiency": round(market_efficiency, 3),
        "market_regime": regime,
        "nasdaq_bias": bias,
        "summary": f"{regime}; {', '.join(summary_parts) if summary_parts else 'no proxy data'}",
    }


def ticker_snapshot(ticker: str, bars: List[Dict[str, Any]], lookback_minutes: int) -> Dict[str, Any]:
    if len(bars) < 2:
        return {}
    latest = bars[-1]
    first = bars[0]
    latest_price = float(latest["close"])
    first_price = float(first["close"])
    if first_price <= 0 or latest_price <= 0:
        return {}
    cutoff = latest["ts"] - dt.timedelta(minutes=lookback_minutes)
    lookback_bars = [bar for bar in bars if bar["ts"] >= cutoff]
    if len(lookback_bars) < 2:
        lookback_bars = bars[-min(len(bars), max(2, lookback_minutes)) :]
    lookback_start = float(lookback_bars[0]["close"])
    lookback_change = (latest_price / lookback_start - 1.0) * 100.0 if lookback_start > 0 else 0.0
    session_change = (latest_price / first_price - 1.0) * 100.0
    deltas = [
        abs(float(lookback_bars[i]["close"]) / float(lookback_bars[i - 1]["close"]) - 1.0) * 100.0
        for i in range(1, len(lookback_bars))
        if float(lookback_bars[i - 1]["close"]) > 0
    ]
    path = sum(deltas)
    efficiency = abs(lookback_change) / path if path > 0 else 0.0
    return {
        "ticker": ticker,
        "latest_time": format_hhmm(latest["ts"]),
        "latest_price": round(latest_price, 4),
        "lookback_change_pct": round(lookback_change, 3),
        "session_change_pct": round(session_change, 3),
        "efficiency": round(max(0.0, min(1.0, efficiency)), 3),
    }


def classify_regime(lookback_pct: float, session_pct: float, efficiency: float) -> str:
    if abs(lookback_pct) < 0.15 or efficiency < 0.30:
        if session_pct > 0.50:
            return "bull_bias_chop"
        if session_pct < -0.50:
            return "bear_bias_chop"
        return "range_chop"
    if lookback_pct > 0:
        return "bull_trend"
    return "bear_trend"


def build_weighted_nasdaq_bias(
    data_by_ticker: Dict[str, List[Dict[str, Any]]],
    args: argparse.Namespace,
    lookback_minutes: int,
) -> Dict[str, Any]:
    tickers = parse_tickers(args.nasdaq_bias_tickers)
    snapshots = {
        ticker: ticker_snapshot(ticker, data_by_ticker.get(ticker, []), lookback_minutes)
        for ticker in tickers
    }
    snapshots = {ticker: snap for ticker, snap in snapshots.items() if snap}
    if not snapshots:
        return {"side": "neutral", "label": "neutral", "label_cn": "中性", "score": 50, "drivers": "loading"}

    components = [
        ("QQQ", 0.35, ["QQQ"]),
        ("SPY", 0.10, ["SPY"]),
        ("Mega", 0.25, parse_tickers(DEFAULT_MEGA_CAP_TICKERS)),
        ("Semi", 0.20, parse_tickers(DEFAULT_SEMI_TICKERS)),
        ("All", 0.10, tickers),
    ]
    total = 0.0
    total_weight = 0.0
    details: List[Dict[str, Any]] = []
    for name, weight, members in components:
        score, breadth = basket_score(members, snapshots)
        if score is None:
            continue
        total += score * weight
        total_weight += weight
        details.append({"name": name, "weight": weight, "score": round(score, 1), "breadth": breadth})

    score = total / total_weight if total_weight else 50.0
    side, label_cn = bias_side(score, bull=58.0, bear=42.0)
    return {
        "side": side,
        "label": side,
        "label_cn": label_cn,
        "score": int(round(max(0.0, min(100.0, score)))),
        "drivers": bias_drivers(snapshots, details),
        "components": details,
    }


def basket_score(members: List[str], snapshots: Dict[str, Dict[str, Any]]) -> tuple[Optional[float], Dict[str, int]]:
    values: List[float] = []
    up = down = flat = 0
    for ticker in members:
        snap = snapshots.get(ticker)
        if not snap:
            continue
        values.append(snapshot_score(snap))
        session = float(snap.get("session_change_pct") or 0.0)
        if session > 0.05:
            up += 1
        elif session < -0.05:
            down += 1
        else:
            flat += 1
    if not values:
        return None, {"up": 0, "down": 0, "flat": 0, "count": 0}
    return sum(values) / len(values), {"up": up, "down": down, "flat": flat, "count": len(values)}


def snapshot_score(snapshot: Dict[str, Any]) -> float:
    session = float(snapshot.get("session_change_pct") or 0.0)
    lookback = float(snapshot.get("lookback_change_pct") or 0.0)
    efficiency = float(snapshot.get("efficiency") or 0.0)
    score = 50.0 + 27.0 * math.tanh(session / 0.75) + 20.0 * math.tanh(lookback / 0.35)
    score += 6.0 * (efficiency - 0.5) * (1.0 if lookback >= 0 else -1.0)
    return max(0.0, min(100.0, score))


def bias_drivers(snapshots: Dict[str, Dict[str, Any]], details: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for ticker in ("QQQ", "SPY"):
        snap = snapshots.get(ticker)
        if snap:
            parts.append(f"{ticker} {float(snap['session_change_pct']):+.2f}%/{float(snap['lookback_change_pct']):+.2f}%")
    for detail in details:
        if detail["name"] not in {"Mega", "Semi"}:
            continue
        breadth = detail["breadth"]
        count = int(breadth["count"])
        if count:
            parts.append(f"{detail['name']} {int(breadth['up'])}/{count} green")
    return "; ".join(parts) if parts else "loading"


def ticker_day_bias(
    ticker: str,
    bars: List[Dict[str, Any]],
    market_context: Dict[str, Any],
    lookback_minutes: int,
) -> Dict[str, Any]:
    snap = ticker_snapshot(ticker, bars, lookback_minutes)
    if not snap:
        return {"day_bias_side": "neutral", "day_bias_label": "中性", "day_bias_score": "", "day_bias_reason": "loading"}
    session = float(snap.get("session_change_pct") or 0.0)
    lookback = float(snap.get("lookback_change_pct") or 0.0)
    vwap_change = ticker_vwap_change(bars)
    market_session = float(market_context.get("market_session_pct") or 0.0)
    score = (
        0.42 * (50.0 + 38.0 * math.tanh(session / 1.00))
        + 0.25 * (50.0 + 32.0 * math.tanh(lookback / 0.45))
        + 0.23 * (50.0 + 30.0 * math.tanh((vwap_change or 0.0) / 0.35))
        + 0.10 * (50.0 + 24.0 * math.tanh((session - market_session) / 0.75))
    )
    score = max(0.0, min(100.0, score))
    side, label_cn = bias_side(score, bull=52.0, bear=48.0)
    return {
        "day_bias_side": side,
        "day_bias_label": label_cn,
        "day_bias_score": int(round(score)),
        "day_bias_reason": f"day {session:+.2f}%; {lookback_minutes}m {lookback:+.2f}%; vwap {(vwap_change or 0.0):+.2f}%; market {market_session:+.2f}%",
    }


def ticker_vwap_change(bars: List[Dict[str, Any]]) -> Optional[float]:
    if not bars:
        return None
    latest = float(bars[-1]["close"])
    weighted_sum = 0.0
    volume_sum = 0.0
    for bar in bars:
        volume = float(bar.get("volume") or 0.0)
        if volume > 0:
            weighted_sum += float(bar["close"]) * volume
            volume_sum += volume
    if volume_sum <= 0:
        vwap = sum(float(bar["close"]) for bar in bars) / len(bars)
    else:
        vwap = weighted_sum / volume_sum
    return (latest / vwap - 1.0) * 100.0 if vwap > 0 else None


def bias_side(score: float, bull: float, bear: float) -> tuple[str, str]:
    if score >= bull:
        return "bullish", "看多"
    if score <= bear:
        return "bearish", "看空"
    return "neutral", "中性"


def attach_position(row: Dict[str, Any], account_capital: float) -> None:
    price = float(row.get("end_price") or 0.0)
    if account_capital > 0 and price > 0:
        qty = int(account_capital // price)
        row["position_qty"] = qty
        row["position_notional"] = round(qty * price, 2)
    else:
        row["position_qty"] = ""
        row["position_notional"] = ""


def mark_record_status(rows: List[Dict[str, Any]], status: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        clean = dict(row)
        clean["record_status"] = status
        out.append(clean)
    return out


def sort_display_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(rows, key=lambda row: (parse_iso_dt(row.get("end_ts", "")), abs(float(row.get("price_change_pct") or 0.0))), reverse=True)


def record_key(row: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("ticker") or ""),
            str(row.get("date") or ""),
            str(row.get("time_window") or ""),
            str(row.get("direction") or ""),
            f"{float(row.get('price_change_pct') or 0.0):.4f}",
        ]
    )


def parse_iso_dt(value: str) -> dt.datetime:
    try:
        return dt.datetime.fromisoformat(str(value))
    except Exception:
        return dt.datetime.min.replace(tzinfo=NY_ZONE)


def safe_float(value: Any) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if math.isnan(number):
        return None
    return number


def format_hhmm(value: dt.datetime) -> str:
    return value.strftime("%H:%M")


class EmailNotifier:
    def __init__(self, args: argparse.Namespace) -> None:
        self.enabled = bool(args.email_alerts)
        self.smtp_host = str(args.smtp_host or os.environ.get("LADDER_SMTP_HOST") or "").strip()
        self.smtp_port = int(args.smtp_port or os.environ.get("LADDER_SMTP_PORT") or 587)
        self.smtp_user = str(args.smtp_user or os.environ.get("LADDER_SMTP_USER") or "").strip()
        self.smtp_password = str(args.smtp_password or os.environ.get("LADDER_SMTP_PASSWORD") or "").strip()
        self.from_addr = str(args.email_from or os.environ.get("LADDER_EMAIL_FROM") or "").strip()
        self.to_addr = str(args.email_to or os.environ.get("LADDER_EMAIL_TO") or "").strip()
        self.last_error = ""

    def ready(self) -> bool:
        return bool(self.from_addr and self.to_addr and self.smtp_host and self.smtp_user and self.smtp_password)

    def send(self, rows: List[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        if not self.ready():
            self.last_error = "Email is enabled but SMTP settings are incomplete."
            return
        msg = EmailMessage()
        msg["From"] = self.from_addr
        msg["To"] = self.to_addr
        msg["Subject"] = compact_alert_text(rows[0], include_time=False) if len(rows) == 1 else f"Ladder Alert: {len(rows)} signals"
        lines = [compact_alert_text(row) for row in rows]
        lines.append("Dashboard: http://127.0.0.1:8765")
        msg.set_content("\n".join(lines))
        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=20) as smtp:
                smtp.starttls()
                smtp.login(self.smtp_user, self.smtp_password)
                smtp.send_message(msg)
            self.last_error = ""
        except Exception as exc:
            self.last_error = str(exc)


class DiscordNotifier:
    def __init__(self, args: argparse.Namespace) -> None:
        self.webhook_url = str(args.discord_webhook_url or os.environ.get("LADDER_DISCORD_WEBHOOK_URL") or "").strip()
        self.enabled = bool(args.discord_alerts or self.webhook_url)
        self.last_error = ""

    def send(self, rows: List[Dict[str, Any]]) -> None:
        if not self.enabled:
            return
        if not self.webhook_url:
            self.last_error = "Discord is enabled but webhook URL is missing."
            return
        lines = [compact_alert_text(row) for row in rows]
        lines.append("http://127.0.0.1:8765")
        content = "\n".join(lines)
        if len(content) > 1900:
            content = content[:1890].rstrip() + "\n..."
        data = json.dumps({"content": content, "allowed_mentions": {"parse": []}}, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self.webhook_url,
            data=data,
            headers={"Content-Type": "application/json; charset=utf-8", "User-Agent": "shareable-ladder-dashboard/1.0"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                if int(resp.status) >= 300:
                    raise RuntimeError(f"Discord HTTP {resp.status}")
            self.last_error = ""
        except Exception as exc:
            self.last_error = str(exc)


def compact_alert_text(row: Dict[str, Any], include_time: bool = True) -> str:
    direction = str(row.get("direction") or "")
    action = "可观察做空回撤" if direction == "up" else "可观察做多反弹" if direction == "down" else "可观察"
    move_word = "涨幅" if direction == "up" else "跌幅" if direction == "down" else "波动"
    qty = row.get("position_qty")
    qty_text = f"，数量 {int(qty)}股" if str(qty).isdigit() and int(qty) > 0 else ""
    text = f"{action} {row.get('ticker', '')}{qty_text}，{move_word} {abs(float(row.get('price_change_pct') or 0.0)):.3f}%"
    if include_time:
        text += f"，时间 {row.get('time_window', '')}"
    return text


def make_handler(engine: DashboardEngine):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            if bool(engine.args.verbose_http):
                super().log_message(fmt, *args)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == "/":
                self.send_html(DASHBOARD_HTML)
            elif path == "/api/state":
                self.send_json(engine.snapshot())
            elif path == "/api/refresh":
                self.send_json({"started": engine.trigger_refresh(), "state": engine.snapshot()})
            else:
                self.send_error(404, "Not found")

        def send_json(self, payload: Dict[str, Any]) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def send_html(self, html: str) -> None:
            raw = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    return Handler


DASHBOARD_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Shareable Ladder Dashboard</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b0f14;
      --panel: #111820;
      --line: #24303b;
      --text: #e7edf3;
      --muted: #93a3b3;
      --up: #25c17b;
      --down: #ff5a5f;
      --warn: #f0bd4f;
      --button: #172333;
      --button-hover: #203044;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: Arial, Helvetica, sans-serif;
      font-size: 14px;
      letter-spacing: 0;
    }
    .shell { max-width: 1440px; margin: 0 auto; padding: 18px; }
    header {
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 16px;
      align-items: start;
      padding-bottom: 14px;
      border-bottom: 1px solid var(--line);
    }
    h1 { margin: 0 0 8px; font-size: 22px; line-height: 1.2; }
    .meta { display: flex; flex-wrap: wrap; gap: 8px 14px; color: var(--muted); font-size: 13px; }
    .pill { display: inline-flex; align-items: center; gap: 6px; white-space: nowrap; }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--warn); box-shadow: 0 0 10px currentColor; }
    .dot.ok { background: var(--up); }
    .dot.error { background: var(--down); }
    .actions { display: flex; align-items: center; justify-content: flex-end; gap: 8px; flex-wrap: wrap; }
    .market-bias {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 36px;
      min-width: 156px;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 0 12px;
      background: #0d141d;
      color: var(--warn);
      font-weight: 700;
    }
    .market-bias.bullish { color: var(--up); border-color: rgba(37,193,123,.55); background: rgba(37,193,123,.10); }
    .market-bias.bearish { color: var(--down); border-color: rgba(255,90,95,.55); background: rgba(255,90,95,.10); }
    .market-bias.neutral { color: var(--warn); border-color: rgba(240,189,79,.48); background: rgba(240,189,79,.09); }
    input {
      width: 132px;
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #0d141d;
      color: var(--text);
      padding: 0 10px;
      font: inherit;
      text-transform: uppercase;
    }
    button {
      min-height: 36px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--button);
      color: var(--text);
      padding: 0 12px;
      cursor: pointer;
      font: inherit;
    }
    button:hover { background: var(--button-hover); }
    button.hidden { display: none; }
    .summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; margin: 14px 0; }
    .metric { border: 1px solid var(--line); background: var(--panel); border-radius: 8px; padding: 12px; min-height: 72px; }
    .metric .label { color: var(--muted); font-size: 12px; margin-bottom: 8px; text-transform: uppercase; }
    .metric .value { font-size: 24px; line-height: 1; font-weight: 700; }
    .content-wrap { border: 1px solid var(--line); border-radius: 8px; overflow: auto; background: var(--panel); max-height: calc(100vh - 220px); }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 9px 12px; border-bottom: 1px solid var(--line); text-align: left; white-space: nowrap; }
    th { position: sticky; top: 0; background: #0f171f; color: var(--muted); font-size: 12px; text-transform: uppercase; z-index: 1; }
    tbody tr.flash { animation: flash 900ms ease-out; }
    tbody tr[data-ticker] { cursor: pointer; }
    tbody tr[data-ticker]:hover { background: rgba(255,255,255,0.04); }
    @keyframes flash { from { background: rgba(240,189,79,.22); } to { background: transparent; } }
    .ticker { font-weight: 700; }
    .day-bias {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 20px;
      min-width: 50px;
      margin-left: 6px;
      padding: 0 7px;
      border: 1px solid var(--line);
      border-radius: 999px;
      font-size: 11px;
      font-weight: 700;
      vertical-align: middle;
    }
    .day-bias.bullish { color: var(--up); border-color: rgba(37,193,123,.55); background: rgba(37,193,123,.10); }
    .day-bias.bearish { color: var(--down); border-color: rgba(255,90,95,.55); background: rgba(255,90,95,.10); }
    .day-bias.neutral { color: var(--warn); border-color: rgba(240,189,79,.48); background: rgba(240,189,79,.09); }
    .dir { display: inline-flex; align-items: center; justify-content: center; width: 52px; min-height: 24px; border-radius: 999px; font-size: 12px; font-weight: 700; text-transform: uppercase; }
    .dir.up { color: #04120b; background: var(--up); }
    .dir.down { color: #180406; background: var(--down); }
    .change.up { color: var(--up); font-weight: 700; }
    .change.down { color: var(--down); font-weight: 700; }
    .status { color: var(--muted); font-size: 12px; text-transform: uppercase; }
    .status.active { color: var(--up); font-weight: 700; }
    .empty { padding: 40px; text-align: center; color: var(--muted); }
    .foot { margin-top: 10px; color: var(--muted); font-size: 12px; }
    @media (max-width: 760px) {
      header { grid-template-columns: 1fr; }
      .actions { justify-content: flex-start; }
      .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>Shareable Ladder Dashboard</h1>
        <div class="meta">
          <span class="pill"><span id="statusDot" class="dot"></span><span id="statusText">Starting</span></span>
          <span class="pill">Updated <strong id="updatedAt">-</strong></span>
          <span class="pill">Scan date <strong id="scanDate">-</strong></span>
          <span class="pill">Next <strong id="nextRefresh">-</strong>s</span>
          <span class="pill">Market <strong id="marketSummary">-</strong></span>
        </div>
      </div>
      <div class="actions">
        <div id="marketBias" class="market-bias neutral">Nasdaq -</div>
        <form id="searchForm">
          <input id="tickerSearch" type="text" autocomplete="off" spellcheck="false" placeholder="Ticker">
          <button type="submit">Search</button>
        </form>
        <button id="backBtn" class="hidden">Back</button>
        <button id="refreshBtn">Refresh</button>
      </div>
    </header>
    <section class="summary">
      <div class="metric"><div class="label">Matches</div><div class="value" id="rowCount">0</div></div>
      <div class="metric"><div class="label">Loaded Tickers</div><div class="value" id="loadedCount">0/0</div></div>
      <div class="metric"><div class="label">Min Move</div><div class="value" id="minMove">-</div></div>
      <div class="metric"><div class="label">Min 1m Volume</div><div class="value" id="minVol">-</div></div>
    </section>
    <div class="content-wrap">
      <table id="resultTable">
        <thead>
          <tr>
            <th>Ticker</th>
            <th>Time Window</th>
            <th>Direction</th>
            <th>Price Change %</th>
            <th>Quality</th>
            <th>Position</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody></tbody>
      </table>
      <div id="empty" class="empty">Waiting for live matches.</div>
    </div>
    <div id="foot" class="foot"></div>
  </div>
  <script>
    const tableBody = document.querySelector('#resultTable tbody');
    const empty = document.getElementById('empty');
    const previousKeys = new Set();
    let latestState = null;
    let activeTicker = '';

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    function keyFor(row) {
      return `${row.ticker}|${row.date}|${row.time_window}|${row.direction}|${row.price_change_pct}`;
    }
    function dayBiasHtml(row) {
      if (!row.day_bias_label) return '';
      const side = String(row.day_bias_side || 'neutral').toLowerCase();
      const score = Number(row.day_bias_score);
      const scoreText = Number.isFinite(score) ? ` ${Math.round(score)}` : '';
      return `<span class="day-bias ${escapeHtml(side)}" title="${escapeHtml(row.day_bias_reason || '')}">${escapeHtml(row.day_bias_label)}${scoreText}</span>`;
    }
    function setStatus(status, message) {
      const dot = document.getElementById('statusDot');
      dot.className = 'dot';
      if (status === 'ok') dot.classList.add('ok');
      if (status === 'error') dot.classList.add('error');
      document.getElementById('statusText').textContent = message || status;
    }
    function marketBiasText(context) {
      const bias = context && context.nasdaq_bias ? context.nasdaq_bias : null;
      if (!bias) return 'Nasdaq -';
      return `Nasdaq ${bias.label_cn || bias.label || ''} ${bias.score ?? '-'}/100`;
    }
    function marketBiasClass(context) {
      const side = context && context.nasdaq_bias ? String(context.nasdaq_bias.side || 'neutral').toLowerCase() : 'neutral';
      return `market-bias ${side}`;
    }
    function positionText(row) {
      const qty = Number(row.position_qty || 0);
      return Number.isFinite(qty) && qty > 0 ? `${Math.floor(qty).toLocaleString()} shares` : '-';
    }
    function qualityText(row) {
      return `R2 ${Number(row.r2 || 0).toFixed(2)} / E ${Number(row.efficiency || 0).toFixed(2)}`;
    }
    function render(state) {
      latestState = state;
      setStatus(state.status, state.message);
      document.getElementById('updatedAt').textContent = state.updated_at || '-';
      document.getElementById('scanDate').textContent = state.scan_date || '-';
      document.getElementById('nextRefresh').textContent = state.next_refresh_in ?? '-';
      document.getElementById('loadedCount').textContent = `${state.loaded_count ?? 0}/${state.ticker_count ?? 0}`;
      document.getElementById('minMove').textContent = `${state.min_price_change_pct ?? '-'}%`;
      document.getElementById('minVol').textContent = `>${state.min_bar_volume ?? '-'}`;
      document.getElementById('marketSummary').textContent = state.market_context && state.market_context.summary ? state.market_context.summary : '-';
      const marketBias = document.getElementById('marketBias');
      marketBias.textContent = marketBiasText(state.market_context);
      marketBias.className = marketBiasClass(state.market_context);
      marketBias.title = state.market_context && state.market_context.nasdaq_bias ? state.market_context.nasdaq_bias.drivers || '' : '';

      const allRows = state.rows || [];
      const rows = activeTicker ? allRows.filter(row => String(row.ticker || '').toUpperCase() === activeTicker) : allRows;
      document.getElementById('rowCount').textContent = rows.length;
      document.getElementById('backBtn').classList.toggle('hidden', !activeTicker);
      tableBody.innerHTML = '';
      empty.style.display = rows.length ? 'none' : 'block';
      empty.textContent = activeTicker ? `${activeTicker} has no matching windows today.` : 'Waiting for live matches.';
      const nextKeys = new Set();
      for (const row of rows) {
        const tr = document.createElement('tr');
        const key = keyFor(row);
        nextKeys.add(key);
        if (!previousKeys.has(key)) tr.classList.add('flash');
        tr.dataset.ticker = row.ticker || '';
        tr.addEventListener('dblclick', () => enterTicker(String(row.ticker || '').toUpperCase(), true));
        const direction = String(row.direction || '').toLowerCase();
        const change = Number(row.price_change_pct || 0);
        tr.innerHTML = `
          <td class="ticker">${escapeHtml(row.ticker || '')}${dayBiasHtml(row)}</td>
          <td>${escapeHtml(row.time_window || '')}</td>
          <td><span class="dir ${direction}">${direction}</span></td>
          <td class="change ${direction}">${Number.isFinite(change) ? change.toFixed(3) : escapeHtml(row.price_change_pct)}</td>
          <td>${qualityText(row)}</td>
          <td>${positionText(row)}</td>
          <td class="status ${String(row.record_status || 'active').toLowerCase()}">${escapeHtml(row.record_status || 'active')}</td>
        `;
        tableBody.appendChild(tr);
      }
      previousKeys.clear();
      for (const key of nextKeys) previousKeys.add(key);
      const errors = state.error_count ? ` | warnings=${state.error_count}` : '';
      document.getElementById('foot').textContent = `${rows.length} rows | current=${state.current_row_count ?? '-'} | fetch=${state.fetch_ticker_count ?? '-'}${errors}`;
    }
    async function loadState() {
      try {
        const resp = await fetch('/api/state', {cache: 'no-store'});
        render(await resp.json());
      } catch (err) {
        setStatus('error', String(err));
      }
    }
    function enterTicker(ticker, pushHistory) {
      const next = String(ticker || '').trim().toUpperCase();
      if (!next) return;
      activeTicker = next;
      document.getElementById('tickerSearch').value = activeTicker;
      previousKeys.clear();
      if (pushHistory) history.pushState({ticker: activeTicker}, '', `#${activeTicker}`);
      if (latestState) render(latestState);
    }
    function leaveTicker(pushHistory) {
      activeTicker = '';
      document.getElementById('tickerSearch').value = '';
      previousKeys.clear();
      if (pushHistory && location.hash) history.replaceState({}, '', location.pathname);
      if (latestState) render(latestState);
    }
    document.getElementById('searchForm').addEventListener('submit', event => {
      event.preventDefault();
      enterTicker(document.getElementById('tickerSearch').value, true);
    });
    document.getElementById('backBtn').addEventListener('click', () => leaveTicker(true));
    document.getElementById('refreshBtn').addEventListener('click', async () => {
      await fetch('/api/refresh', {cache: 'no-store'});
      await loadState();
    });
    window.addEventListener('popstate', event => {
      const ticker = event.state && event.state.ticker ? String(event.state.ticker).toUpperCase() : '';
      if (ticker) enterTicker(ticker, false);
      else leaveTicker(false);
    });
    if (location.hash && location.hash.length > 1) enterTicker(decodeURIComponent(location.hash.slice(1)), false);
    loadState();
    setInterval(loadState, 1000);
  </script>
</body>
</html>
"""


def find_free_port(host: str, start_port: int) -> int:
    for port in range(start_port, start_port + 50):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex((host, port)) != 0:
                return port
    raise RuntimeError(f"No free port found from {start_port} to {start_port + 49}")


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Clean shareable intraday ladder dashboard.")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--tickers", default="", help="Comma-separated tickers to scan.")
    ap.add_argument("--tickers-file", default="tickers.txt", help="Optional comma/newline-separated ticker file.")
    ap.add_argument("--scan-date", default="auto", help="YYYY-MM-DD or auto.")
    ap.add_argument("--api-key", default="", help="Polygon API key. Prefer POLYGON_API_KEY.")
    ap.add_argument("--base-url", default=DEFAULT_BASE_URL)
    ap.add_argument("--refresh-seconds", type=int, default=5)
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--timeout", type=int, default=20)
    ap.add_argument("--adjusted", action="store_true")
    ap.add_argument("--start-time", default="04:00")
    ap.add_argument("--end-time", default="auto")
    ap.add_argument("--min-window-minutes", type=int, default=3)
    ap.add_argument("--max-window-minutes", type=int, default=30)
    ap.add_argument("--min-price-change-pct", type=float, default=2.5)
    ap.add_argument("--min-bar-volume", type=float, default=1000.0)
    ap.add_argument("--min-monotonic-ratio", type=float, default=0.65)
    ap.add_argument("--min-efficiency", type=float, default=0.45)
    ap.add_argument("--min-r2", type=float, default=0.45)
    ap.add_argument("--min-same-color-ratio", type=float, default=0.55)
    ap.add_argument("--max-windows-per-ticker", type=int, default=5)
    ap.add_argument("--max-rows", type=int, default=500)
    ap.add_argument("--keep-captured-signals", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--max-captured-signals", type=int, default=500)
    ap.add_argument("--prime-alerts-on-start", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--max-alert-rows", type=int, default=10)
    ap.add_argument("--account-capital", type=float, default=0.0, help="Optional paper position sizing capital. Default hides sizing.")
    ap.add_argument("--market-context-tickers", default=DEFAULT_MARKET_CONTEXT_TICKERS)
    ap.add_argument("--nasdaq-bias-tickers", default=DEFAULT_NASDAQ_BIAS_TICKERS)
    ap.add_argument("--context-lookback-minutes", type=int, default=30)
    ap.add_argument("--email-alerts", action="store_true")
    ap.add_argument("--email-to", default="")
    ap.add_argument("--email-from", default="")
    ap.add_argument("--smtp-host", default="")
    ap.add_argument("--smtp-port", type=int, default=587)
    ap.add_argument("--smtp-user", default="")
    ap.add_argument("--smtp-password", default="")
    ap.add_argument("--discord-alerts", action="store_true")
    ap.add_argument("--discord-webhook-url", default="")
    ap.add_argument("--verbose-http", action="store_true")
    return ap


def main() -> None:
    args = build_arg_parser().parse_args()
    try:
        engine = DashboardEngine(args)
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(2)

    engine.start()
    port = find_free_port(str(args.host), int(args.port))
    server = ThreadingHTTPServer((str(args.host), port), make_handler(engine))
    print(f"Shareable Ladder Dashboard: http://{args.host}:{port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping dashboard...")
    finally:
        engine.stop()
        server.server_close()


if __name__ == "__main__":
    main()
