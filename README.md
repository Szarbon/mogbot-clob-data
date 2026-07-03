# mogbot-clob-data

Public, continuously-recorded order-book tape for Polymarket's 5-minute
crypto Up/Down markets (BTC, ETH, SOL, XRP, HYPE). Data only -- this repo
never authenticates to Polymarket and never places an order. Everything in
here comes from Polymarket's public REST APIs:

- **Gamma API** (`https://gamma-api.polymarket.com/events`) -- discovers
  the current 5-minute market for each asset.
- **CLOB API** (`https://clob.polymarket.com/book`) -- public order-book
  snapshots, no API key required.

A GitHub Actions workflow (`.github/workflows/record.yml`) runs
continuously: it records ~1 order-book snapshot per second per outcome per
asset, commits the tape every ~14 minutes, and re-dispatches itself so
recording never stops. This is a research/backtesting data feed for
[MogBot](https://github.com/Szarbon) -- it is not, and will never contain,
trading credentials, wallet material, or any private infrastructure
reference.

## Layout

```
recordings/<YYYY-MM-DD>/<asset>_<window_ts>.jsonl.gz
```

- `<YYYY-MM-DD>` -- UTC date the 5-minute window started.
- `<asset>` -- one of `btc`, `eth`, `sol`, `xrp`, `hype`.
- `<window_ts>` -- Unix timestamp (seconds) of the window's start, i.e.
  `(unix_time // 300) * 300`. This is the same integer Polymarket uses in
  its market slug: `<asset>-updown-5m-<window_ts>`.
- Each file is a gzip-compressed JSONL (newline-delimited JSON) stream
  covering both outcomes (Up and Down) for that asset's window, snapshot
  roughly once per second for ~310 seconds (the 300-second window plus a
  buffer to catch late resolution).

## Recording schema

Each line is one JSON record. This schema is deliberately field-compatible
with MogBot's private recorder
(`mogbot/data/polymarket_market_recorder.py` ->
`normalize_book_snapshot` / `MarketReplay._apply_book`), so a tape from
this repo can be fed straight into that replay tooling without a schema
shim.

```jsonc
{
  "schema_version": 1,
  "source": "polymarket_book_snapshot",
  "received_at_ms": 1783043957726,        // wall-clock ms when this snapshot was fetched
  "received_monotonic_ns": 19416535719100, // process-local monotonic clock, for intra-run ordering only
  "event_type": "book",                    // "book" (normal snapshot) or "snapshot_error" (fetch failed)
  "asset_id": "422750845...373521",        // CLOB token id for this outcome
  "market_slug": "btc-updown-5m-1783043700",
  "outcome": "Up",                         // "Up" or "Down"
  "raw_event_hash": "b56e9d04...5e085",    // sha256 of the normalized payload, for dedup/integrity checks
  "seq": 1,                                // per-file sequence number
  "payload": {
    "market": "0xdc0925...31b2ac",         // Polymarket condition id
    "asset_id": "422750845...373521",
    "timestamp": "1783043957895",
    "hash": "067d66a1600322402d93f3398ed85b27b928600b",
    "bids": [ { "price": "0.42", "size": "120.5" }, ... ],
    "asks": [ { "price": "0.99", "size": "14105.37" }, ... ],
    "tick_size": "0.01",
    "min_order_size": "5"
  }
}
```

Notes on the raw `/book` response (`payload`):

- `asks`/`bids` arrays are **not** guaranteed sorted by the API. Compute
  `best_ask = min(price for price in asks)` and `best_bid = max(price for
  price in bids)` yourself -- do not assume `asks[0]` is the best price
  (it commonly is not).
- `event_type: "snapshot_error"` records replace the normal payload with
  `error` / `message` fields when a single HTTP fetch failed; they do not
  stop the recording loop. Treat them as a gap, not a crash.
- Recorded books are the same object Polymarket serves publicly at
  `GET https://clob.polymarket.com/book?token_id=<asset_id>` -- nothing is
  transformed except the wrapping envelope above.

## No secrets, data-only

- The recorder (`recorder.py`) is Python 3.11+ stdlib only (`urllib`,
  `gzip`, `json`, `threading`) -- no API keys, no wallet libraries, no
  `.env`.
- The workflow uses only the automatic, scoped `GITHUB_TOKEN` (via
  `${{ github.token }}`) to commit and to re-dispatch itself. No repo
  secrets are configured or required.
- Every workflow run gates on `.github/scripts/secret_scan.sh`, which
  greps all tracked, human-readable files for common secret patterns
  (API keys, private key headers, DB connection strings, webhook URLs,
  etc.) and fails the job if anything matches.

## Consuming this data from MogBot

Files are drop-in compatible with MogBot's replay tooling:

```python
from mogbot.data.polymarket_market_recorder import replay_file

replay = replay_file(Path("recordings/2026-07-03/btc_1783043700.jsonl.gz"))
book = replay.get(asset_id)
print(book.best_bid, book.best_ask, book.spread)
```

Or read it generically:

```python
import gzip, json

with gzip.open("recordings/2026-07-03/btc_1783043700.jsonl.gz", "rt") as f:
    for line in f:
        record = json.loads(line)
        ...
```

## Validating a recording

```bash
python recorder.py validate recordings/2026-07-03/btc_1783043700.jsonl.gz
```

Checks the file parses, timestamps are monotone non-decreasing, and
reports snapshot/error counts plus a sample best-ask per asset.

## Caveats

- **GitHub Actions cron jitter.** The `schedule` trigger (every 4 hours)
  is a watchdog restart in case the self-chain ever breaks; Actions cron
  can be delayed by several minutes under load, so treat it as
  best-effort, not exact.
- **Coverage, not perfection.** Each recording job self-chains before its
  ~350-minute timeout, but the few seconds between one run ending and the
  next starting (plus any queued-but-not-yet-started window) means a
  handful of 5-minute windows per day may be missed or only partially
  covered. This is a research tape, not a guaranteed-complete archive.
- **Data volume.** ~1 snapshot/sec/outcome/asset, gzip-compressed JSON, 5
  assets, 288 windows/day -- expect roughly tens of MB per day; the repo
  will grow indefinitely unless a compaction/pruning job is added later.
