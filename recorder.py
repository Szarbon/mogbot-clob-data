#!/usr/bin/env python3
"""Standalone public Polymarket 5-minute crypto Up/Down CLOB recorder.

Data-only. Never authenticates, never places orders. Uses only public
REST endpoints:
  - Gamma API (market discovery):  https://gamma-api.polymarket.com/events
  - CLOB API (order book):         https://clob.polymarket.com/book

Python 3.11+ stdlib only (urllib, gzip, json, threading) -- no third-party
dependencies, so the recorder runs unmodified inside GitHub Actions'
default ubuntu-latest Python.

Recording schema is intentionally kept field-compatible with MogBot's
private recorder (mogbot/data/polymarket_market_recorder.py ->
normalize_book_snapshot / MarketReplay._apply_book) so tapes produced here
can be replayed by that tooling without a schema shim. See README.md for
the field-by-field schema description.

Output layout:
  recordings/<YYYY-MM-DD>/<asset>_<window_ts>.jsonl.gz
    one gzip-compressed JSONL file per asset per 5-minute window.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import logging
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

SCHEMA_VERSION = 1
WINDOW_SEC = 300

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
USER_AGENT = "mogbot-clob-data-recorder/1.0 (+https://github.com/Szarbon/mogbot-clob-data)"

# Polymarket's crypto 5-minute Up/Down slug prefixes. Kept identical to
# MogBot's private asset registry (mogbot/core/crypto5m_assets.py) so
# slugs line up, but this file has zero dependency on that private code.
ASSET_SLUG_PREFIX: dict[str, str] = {
    "btc": "btc",
    "eth": "eth",
    "sol": "sol",
    "xrp": "xrp",
    "hype": "hype",
}
DEFAULT_ASSETS = tuple(sorted(ASSET_SLUG_PREFIX))

DEFAULT_SNAPSHOT_INTERVAL_SEC = 1.0
DEFAULT_WINDOW_DURATION_SEC = 310.0  # 300s window + 10s buffer for late resolution
DEFAULT_HTTP_TIMEOUT_SEC = 15.0
GAMMA_RETRY_ATTEMPTS = 6
GAMMA_RETRY_DELAY_SEC = 5.0

log = logging.getLogger("recorder")


# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only)
# ---------------------------------------------------------------------------


def _http_get_json(url: str, timeout: float) -> Any:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def _utc_ms() -> int:
    return int(time.time() * 1000)


def _record_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _parse_json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value or "[]")
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


# ---------------------------------------------------------------------------
# Market discovery + book snapshots
# ---------------------------------------------------------------------------


def current_window_ts(now_ts: float | None = None) -> int:
    now = int(time.time()) if now_ts is None else int(now_ts)
    return (now // WINDOW_SEC) * WINDOW_SEC


def crypto5m_slug(asset: str, window_ts: int) -> str:
    prefix = ASSET_SLUG_PREFIX[asset]
    return f"{prefix}-updown-5m-{window_ts}"


def fetch_gamma_market(slug: str, timeout: float = DEFAULT_HTTP_TIMEOUT_SEC) -> dict[str, Any]:
    """Fetch the Gamma event for a 5-min crypto Up/Down slug and return its market."""
    url = f"{GAMMA_BASE}/events?" + urllib.parse.urlencode({"slug": slug, "limit": "1"})
    data = _http_get_json(url, timeout=timeout)
    if not data:
        raise RuntimeError(f"no Gamma event found for slug={slug}")
    event = data[0]
    markets = event.get("markets", [])
    market = markets[0] if markets else event
    market["_slug"] = slug
    return market


def fetch_gamma_market_with_retry(
    slug: str,
    attempts: int = GAMMA_RETRY_ATTEMPTS,
    delay_sec: float = GAMMA_RETRY_DELAY_SEC,
    timeout: float = DEFAULT_HTTP_TIMEOUT_SEC,
) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fetch_gamma_market(slug, timeout=timeout)
        except Exception as exc:  # noqa: BLE001 - deliberately broad, network/parse errors both retry
            last_exc = exc
            if attempt < attempts:
                time.sleep(delay_sec)
    assert last_exc is not None
    raise last_exc


def extract_market_asset_ids(market: dict[str, Any]) -> list[dict[str, str]]:
    """Extract outcome -> CLOB token id pairs from a Gamma market payload."""
    outcomes = _parse_json_list(market.get("outcomes"))
    token_ids = _parse_json_list(market.get("clobTokenIds"))
    if len(outcomes) != len(token_ids) or not token_ids:
        raise ValueError("Gamma market is missing aligned outcomes/clobTokenIds")
    return [{"outcome": str(o), "asset_id": str(t)} for o, t in zip(outcomes, token_ids)]


def fetch_book_snapshot(asset_id: str, timeout: float = DEFAULT_HTTP_TIMEOUT_SEC) -> dict[str, Any]:
    url = f"{CLOB_BOOK_URL}?" + urllib.parse.urlencode({"token_id": asset_id})
    return _http_get_json(url, timeout=timeout)


def normalize_book_snapshot(asset_id: str, payload: dict[str, Any], received_at_ms: int | None = None) -> dict[str, Any]:
    """Wrap a /book response in the same envelope MogBot's private recorder
    writes (polymarket_market_recorder.normalize_book_snapshot), so replay
    tooling built against that schema can consume this tape unmodified."""
    received_at_ms = received_at_ms or _utc_ms()
    normalized = dict(payload)
    normalized.setdefault("event_type", "book")
    normalized.setdefault("asset_id", asset_id)
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "polymarket_book_snapshot",
        "received_at_ms": received_at_ms,
        "received_monotonic_ns": time.perf_counter_ns(),
        "event_type": "book",
        "asset_id": normalized.get("asset_id", asset_id),
        "raw_event_hash": _record_hash(normalized),
        "payload": normalized,
    }


def _error_record(asset_id: str, exc: Exception) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "source": "polymarket_book_snapshot",
        "received_at_ms": _utc_ms(),
        "received_monotonic_ns": time.perf_counter_ns(),
        "event_type": "snapshot_error",
        "asset_id": asset_id,
        "error": type(exc).__name__,
        "message": str(exc),
    }


# ---------------------------------------------------------------------------
# Per-asset, per-window recording
# ---------------------------------------------------------------------------


def record_window(
    asset: str,
    window_ts: int,
    out_dir: Path,
    snapshot_interval_sec: float = DEFAULT_SNAPSHOT_INTERVAL_SEC,
    duration_sec: float = DEFAULT_WINDOW_DURATION_SEC,
    http_timeout: float = DEFAULT_HTTP_TIMEOUT_SEC,
) -> Path | None:
    """Record both outcomes of one asset's 5-min window. Never raises --
    failures are logged and the asset is skipped for this window."""
    slug = crypto5m_slug(asset, window_ts)
    try:
        market = fetch_gamma_market_with_retry(slug, timeout=http_timeout)
        outcome_assets = extract_market_asset_ids(market)
    except Exception as exc:  # noqa: BLE001
        log.error("%s window=%s: market discovery failed: %s", asset, window_ts, exc)
        return None

    date_str = datetime.fromtimestamp(window_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    day_dir = out_dir / date_str
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"{asset}_{window_ts}.jsonl.gz"

    seq = 0
    snapshot_count = 0
    error_count = 0
    deadline = time.time() + duration_sec
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        while time.time() < deadline:
            loop_start = time.time()
            for item in outcome_assets:
                asset_id = item["asset_id"]
                try:
                    payload = fetch_book_snapshot(asset_id, timeout=http_timeout)
                    record = normalize_book_snapshot(asset_id, payload)
                    record["market_slug"] = slug
                    record["outcome"] = item["outcome"]
                    snapshot_count += 1
                except Exception as exc:  # noqa: BLE001 - one bad snapshot must not kill the window
                    record = _error_record(asset_id, exc)
                    record["market_slug"] = slug
                    record["outcome"] = item["outcome"]
                    error_count += 1
                seq += 1
                record["seq"] = seq
                fh.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
            fh.flush()
            elapsed = time.time() - loop_start
            sleep_for = snapshot_interval_sec - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)

    log.info(
        "%s window=%s OK: %d snapshots, %d errors -> %s",
        asset, window_ts, snapshot_count, error_count, path,
    )
    return path


def record_batch(
    assets: Iterable[str],
    window_ts: int,
    out_dir: Path,
    snapshot_interval_sec: float,
    duration_sec: float,
) -> dict[str, Path | None]:
    """Record one window for every asset concurrently (one thread each) so
    all assets share the same wall-clock cadence."""
    results: dict[str, Path | None] = {}
    lock = threading.Lock()

    def _run(asset: str) -> None:
        try:
            path = record_window(asset, window_ts, out_dir, snapshot_interval_sec, duration_sec)
        except Exception as exc:  # noqa: BLE001 - a thread must never crash the batch
            log.error("%s window=%s: unhandled recorder error: %s", asset, window_ts, exc)
            path = None
        with lock:
            results[asset] = path

    threads = [threading.Thread(target=_run, args=(asset,), name=f"rec-{asset}") for asset in assets]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


# ---------------------------------------------------------------------------
# Bounded / single-shot run loop
# ---------------------------------------------------------------------------


def run(
    assets: list[str],
    out_dir: Path,
    duration_min: float | None,
    once: bool,
    snapshot_interval_sec: float = DEFAULT_SNAPSHOT_INTERVAL_SEC,
    window_duration_sec: float = DEFAULT_WINDOW_DURATION_SEC,
) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    deadline = None if (once or duration_min is None) else time.time() + duration_min * 60
    windows_done = 0
    while True:
        window_ts = current_window_ts()
        log.info("Recording window %s for assets: %s", window_ts, ", ".join(a.upper() for a in assets))
        results = record_batch(assets, window_ts, out_dir, snapshot_interval_sec, window_duration_sec)
        windows_done += 1
        ok = sum(1 for p in results.values() if p is not None)
        log.info("Window %s complete: %d/%d assets recorded", window_ts, ok, len(assets))
        if once:
            break
        if deadline is not None and time.time() >= deadline:
            break
    return windows_done


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_file(path: Path) -> dict[str, Any]:
    """Parse a recording file and check basic sanity: valid JSON lines,
    monotone non-decreasing received_at_ms, at least one real snapshot."""
    opener = gzip.open if str(path).endswith(".gz") else open
    count = 0
    snapshot_count = 0
    error_count = 0
    last_ts: int | None = None
    monotone = True
    assets: set[str] = set()
    sample_best_ask: dict[str, Any] = {}

    with opener(path, "rt", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                return {"ok": False, "path": str(path), "error": f"json decode error at line {line_no}: {exc}"}
            count += 1
            ts = rec.get("received_at_ms")
            if isinstance(ts, int):
                if last_ts is not None and ts < last_ts:
                    monotone = False
                last_ts = ts
            asset_id = rec.get("asset_id")
            if asset_id:
                assets.add(str(asset_id))
            if rec.get("event_type") == "book":
                snapshot_count += 1
                asks = rec.get("payload", {}).get("asks") or []
                if asks and asset_id not in sample_best_ask:
                    try:
                        best = min(float(a["price"]) for a in asks if a.get("price") is not None)
                        sample_best_ask[str(asset_id)] = best
                    except (ValueError, TypeError, KeyError):
                        pass
            elif rec.get("event_type") == "snapshot_error":
                error_count += 1

    ok = count > 0 and monotone
    return {
        "ok": ok,
        "path": str(path),
        "records": count,
        "snapshots": snapshot_count,
        "errors": error_count,
        "monotone_timestamps": monotone,
        "distinct_assets": len(assets),
        "sample_best_ask": sample_best_ask,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_assets(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_ASSETS)
    chosen = [a.strip().lower() for a in value.split(",") if a.strip()]
    unknown = [a for a in chosen if a not in ASSET_SLUG_PREFIX]
    if unknown:
        raise SystemExit(f"unknown asset(s): {unknown}; supported: {sorted(ASSET_SLUG_PREFIX)}")
    return chosen


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [recorder] %(message)s", stream=sys.stdout)

    parser = argparse.ArgumentParser(description="Record public Polymarket 5-min crypto Up/Down order books")
    sub = parser.add_subparsers(dest="cmd", required=True)

    rec = sub.add_parser("record", help="Record 5-min crypto Up/Down books")
    rec.add_argument("--assets", default=None, help="Comma-separated asset codes (default: all)")
    rec.add_argument("--out-dir", type=Path, default=Path("recordings"))
    rec.add_argument("--snapshot-interval-sec", type=float, default=DEFAULT_SNAPSHOT_INTERVAL_SEC)
    rec.add_argument("--window-duration-sec", type=float, default=DEFAULT_WINDOW_DURATION_SEC)
    rec.add_argument("--duration-min", type=float, default=None, help="Bound total run time; loops windows until reached")
    rec.add_argument("--once", action="store_true", help="Record exactly one window then exit")

    val = sub.add_parser("validate", help="Validate a recorded .jsonl / .jsonl.gz file")
    val.add_argument("file", type=Path)

    args = parser.parse_args()

    if args.cmd == "record":
        assets = _parse_assets(args.assets)
        if not args.once and args.duration_min is None:
            parser.error("record requires --once or --duration-min")
        windows_done = run(
            assets,
            args.out_dir,
            args.duration_min,
            args.once,
            snapshot_interval_sec=args.snapshot_interval_sec,
            window_duration_sec=args.window_duration_sec,
        )
        log.info("Done: %d window(s) recorded.", windows_done)
        return 0

    if args.cmd == "validate":
        result = validate_file(args.file)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("ok") else 2

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
