# StockPick backend — working notes

## Yahoo rate limiting: read before touching any fetch path

Yahoo rate-limits **by IP**, and this service runs on Render, where the egress IP is
shared datacenter space that other tenants are already burning. The budget is far
smaller than it looks from a laptop — code that runs fine locally gets blocked in
production. Every fetch added to a loop is spent against that budget.

### Measured cost per call

| Call | Requests | Endpoint | Notes |
|---|---|---|---|
| `Ticker.history()` | **1** | `v8/finance/chart` | cheap, no crumb |
| `Ticker.info` | **3** | `quoteSummary` + `v7/finance/quote` + `fundamentals-timeseries` | crumb-authenticated, throttled hardest |
| `Ticker.income_stmt` / `.balance_sheet` | **1** each | `fundamentals-timeseries` | |
| `Ticker.news` | **1** | `xhr` | |
| batched quote (`fetch_quote_batch`) | **1 per 50 symbols** | `v7/finance/quote` | no sector/growth/ROE |

Re-measure with `yfinance.data.YfData._make_request` wrapped in a counter before
trusting any of this after a yfinance upgrade.

### Rules

1. **Never call `.info` in a loop over the universe.** This is what broke the screener:
   500 × 4 requests ≈ 2,000 a run, 1,500 of them crumb-authenticated, and Yahoo cut us
   off inside the first five tickers. `.info` belongs behind a cap
   (`FUNDAMENTALS_MAX`), on stocks that already survived the price screen.
2. **The wide screen is price-only.** `screen_stock_combined()` must stay at exactly one
   request per stock. Nothing in its accept/reject decision may depend on fundamentals —
   the score is price-derived, and the fundamental target floor only ever *raises* a
   target that is already ≥ `MIN_TARGET_PCT` by construction.
3. **Prefer the batched quote** (`fetch_quote_batch`) for anything it covers: market cap,
   both PEs, price/book, EPS, long name. One request per 50 symbols. It does **not**
   carry sector, margins, growth or ROE — those need per-stock `.info`.
4. **An empty DataFrame is not "no data".** yfinance does not raise when the chart
   endpoint is throttled; it logs "possibly delisted" and hands back an empty frame.
   Always fetch price history through `_fetch_history()`, which retries empties with
   backoff and reports whether it was throttled. Taking empties at face value made a
   60-stock trial report 40 dead tickers that all fetched fine on retry.
5. **Every fetch goes through `_yf_retry()` or `_fetch_history()`.** Both back off
   exponentially. A flat retry is worse than none — Yahoo's cooldown is minutes, so a
   15s retry just draws a second 429 and drops the stock.
6. **Bounded blocking.** Phase 2 stops after `FUNDAMENTALS_ABORT_AFTER` consecutively
   blocked stocks. Without it a blocked run spends ~140s of backoff per stock and still
   ends with nothing.
7. **Cache what does not change.** The benchmark series is shared by every user and every
   Analyse call — `_cached_benchmark()` holds it for an hour. Don't re-fetch it per call.
8. **One run at a time.** `main.py` admits screening runs through `_run_guard`; callers
   that ask while one is in flight attach to it. Don't add a code path that starts a
   second concurrent run.
9. **Degrade, don't fail.** A throttled fundamentals fetch should leave the pick with its
   price-derived levels, not abort the run. The summary generators already treat an empty
   fundamentals block as "no data".

### Pacing constants (`signals.py`)

`SCREEN_REQUEST_GAP`, `FUNDAMENTALS_GAP`, `FUNDAMENTALS_MAX`, `QUOTE_BATCH_SIZE`,
`FUNDAMENTALS_ABORT_AFTER`. Lowering the gaps or raising the caps trades reliability for
speed. A full 500-stock run is roughly:

```
phase 1  price screen    ~500 requests   (1 per stock, chart endpoint)
phase 2a batched quote     ~4 requests   (1 per 50 survivors)
phase 2b full .info       ~75 requests   (FUNDAMENTALS_MAX × 3)
phase 3  valuation        ~60 requests   (top 15 × 4)
phase 4  news             ~15 requests
                         ─────────────
                         ~650 requests, ~12 min
```

Verified on a 60-stock slice at production pacing: 60 chart requests for 60 stocks, zero
throttled responses.

### Reading a run log

Rejections name their reason and each phase ends with a tally. If the tally shows a large
`throttled` / `no data` / `fetch failed` count, the run saw only part of the universe and
its picks are **not** a full screen — the log says so explicitly above 20%. Treat that as
a failed run, not a thin market.
