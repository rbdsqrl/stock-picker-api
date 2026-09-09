from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import uvicorn
import threading
import asyncio
import logging
import os
from datetime import datetime, timezone
from signals import run_screening, run_screening_combined
from database import init_db, get_today_picks, get_history, save_pick, update_outcome, check_and_update_target_hits, recalculate_all_levels, get_pick_events, get_archive, backfill_frozen_open_prices

log = logging.getLogger(__name__)

app = FastAPI(title="StockPick API")

# FRONTEND_URL is set via environment variable in production (Cloudflare Pages URL).
# Fallback keeps local dev working without any config.
_FRONTEND_URL = os.environ.get("FRONTEND_URL", "http://localhost:5173")
_CORS_ORIGINS  = list({_FRONTEND_URL, "http://localhost:5173"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=_CORS_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

scheduler = BackgroundScheduler()

# ── Screening state ───────────────────────────────────────────────────────────
# There is one screening run for the whole service, not one per user: the screen is
# identical for everybody and each run costs ~500 upstream fetches. Callers that ask
# for a run while one is in flight attach to it instead of starting another.
_state_lock = threading.Lock()
_state = {
    "status":     "idle",   # idle | running | done | stopped | error
    "logs":       [],
    "abort":      threading.Event(),
    "started_at": None,
}

# Held for the duration of a run. Guarding on the status string alone is racy — two
# requests can both read "idle" before either task starts — so admission is decided
# by whoever takes this lock.
_run_guard = threading.Lock()

def _emit(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    with _state_lock:
        _state["logs"].append(line)
        if len(_state["logs"]) > 300:
            _state["logs"] = _state["logs"][-300:]
    log.info(msg)

# ─────────────────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    init_db()
    scheduler.add_job(run_and_save, "cron", hour=10, minute=45, timezone="UTC")
    scheduler.start()

@app.on_event("shutdown")
def shutdown():
    scheduler.shutdown()

def run_and_save():
    # Non-blocking: if a run is already in flight this call is a no-op, and whoever
    # asked for it is expected to follow the live run via /api/screen/status.
    if not _run_guard.acquire(blocking=False):
        log.info("run_and_save: a screening run is already in flight — not starting another")
        return
    try:
        with _state_lock:
            _state["status"]     = "running"
            _state["logs"]       = []
            _state["started_at"] = datetime.now().isoformat()
            _state["abort"].clear()
        _run_screening_body()
    finally:
        _run_guard.release()


def _run_screening_body():
    try:
        ready_picks = run_screening_combined(log_cb=_emit, abort_event=_state["abort"])
        for pick in ready_picks:
            save_pick(pick, rank=pick["rank"])
        result = check_and_update_target_hits()
        if result["failed"]:
            _emit(f"Outcome check: {result['updated']} updated, skipped {result['failed']}")
        with _state_lock:
            _state["status"] = "stopped" if _state["abort"].is_set() else "done"
    except Exception as e:
        log.exception(f"run_and_save fatal: {e}")
        _emit(f"Fatal error: {e}")
        with _state_lock:
            _state["status"] = "error"

@app.get("/api/pick/today")
def today():
    picks = get_today_picks()
    if not picks:
        return {"status": "no_pick", "message": "No picks for today yet. Run screening manually."}
    return {"status": "ok", "picks": picks}

@app.get("/api/pick/history")
def history():
    rows = get_history(limit=30)
    return {"picks": rows}

@app.get("/api/pick/archive")
def archive():
    """Everything frozen into the one-time Archive snapshot — calls from before the
    snapshot was taken. These never change, so unlike /history there's no limit."""
    return {"picks": get_archive()}

@app.post("/api/pick/backfill-archive-outcomes")
async def backfill_archive_outcomes():
    """One-off: fill in a closing price for archived picks that were still open when
    frozen, so the Archive tab shows what each call was worth at the snapshot moment
    instead of a blank. Safe to re-run — only rows with no outcome_price yet are
    touched. Remove this endpoint once it has been run.
    """
    result = await asyncio.to_thread(backfill_frozen_open_prices)
    log.info(f"backfill_archive_outcomes: updated {result['updated']}, failed {result['failed']}")
    return result

@app.get("/api/pick/events")
def pick_events(since: str | None = None, pick_id: int | None = None, limit: int = 200):
    """Outcome transitions — what turned, when, and which bar decided it.

    `since` is an ISO timestamp the client holds as its seen-marker; without one
    the caller gets the most recent `limit` events. `pick_id` returns a single
    call's trail in chronological order instead.
    """
    return {"events": get_pick_events(since=since, pick_id=pick_id, limit=limit)}

@app.post("/api/pick/refresh-outcomes")
async def refresh_outcomes():
    """Re-score past picks against the levels they were published with.

    This deliberately does NOT recalculate entry/target/stop. Those are anchored
    to the date the call was given — rewriting them retroactively would score the
    track record against moving goalposts. recalculate_all_levels() still exists
    as a one-off backfill tool but is not wired to any endpoint.
    """
    log.info("refresh_outcomes: started")
    try:
        # force=True re-resolves picks that already have an outcome, so the
        # SL-before-target rule is applied to rows scored under the old logic.
        # Aware and in UTC on purpose: this is compared against a TIMESTAMPTZ column.
        # A naive datetime.now() carries IST wall time with no offset, which Postgres
        # reads as UTC — five and a half hours in the future, so the window matched
        # nothing and the banner listed no calls at all.
        started = datetime.now(timezone.utc).isoformat()
        result  = await asyncio.to_thread(check_and_update_target_hits, True, "manual_refresh")
        log.info(
            f"refresh_outcomes: done — changed={result['updated']} hits={result['hits']} "
            f"t1={result['t1_hits']} misses={result['misses']} pending={result['pending']} "
            f"failed={result['failed']}"
        )
        return {
            "hits_updated": result["updated"],
            "hits":         result["hits"],
            "t1_hits":      result["t1_hits"],
            "misses":       result["misses"],
            "pending":      result["pending"],
            "failed":       result["failed"],
            # What this run changed, so the banner can name the calls instead of
            # leaving a count for the user to go hunting through the table for.
            "events":       await asyncio.to_thread(get_pick_events, started),
        }
    except Exception as e:
        log.exception(f"refresh_outcomes: failed — {e}")
        raise

@app.post("/api/pick/recalculate-levels")
async def recalculate_levels():
    """Backfill stop / T1 / T2 on historical picks under the current policy.

    Kept separate from refresh-outcomes on purpose: that one only re-scores, this
    one rewrites levels. The entry (price_at_pick) is never touched, and ATR comes
    from pick-date data only, so this is idempotent — running it twice changes
    nothing the second time.
    """
    log.info("recalculate_levels: started")
    try:
        recalculated = await asyncio.to_thread(recalculate_all_levels)
        # Levels just moved, so every stored outcome is stale — re-resolve them all.
        result = await asyncio.to_thread(check_and_update_target_hits, True, "recalc_levels")
        log.info(
            f"recalculate_levels: done — levels={recalculated} changed={result['updated']} "
            f"hits={result['hits']} t1={result['t1_hits']} misses={result['misses']} "
            f"pending={result['pending']} failed={result['failed']}"
        )
        return {
            "levels_recalculated": recalculated,
            "outcomes_changed":    result["updated"],
            "hits":                result["hits"],
            "t1_hits":             result["t1_hits"],
            "misses":              result["misses"],
            "pending":             result["pending"],
            "failed":              result["failed"],
        }
    except Exception as e:
        log.exception(f"recalculate_levels: failed — {e}")
        raise


@app.post("/api/screen/run")
def manual_run(background_tasks: BackgroundTasks):
    """Start a screening run, or hand back the one already in progress.

    The screen is the same for every user, so a second request while one is running
    attaches the caller to it rather than queueing another ~500-fetch pass upstream.
    The logs so far come back with the response so the client can render the live run
    immediately instead of waiting for its first status poll.
    """
    with _state_lock:
        if _state["status"] == "running":
            return {
                "status":     "already_running",
                "logs":       list(_state["logs"]),
                "started_at": _state["started_at"],
            }
    background_tasks.add_task(run_and_save)
    return {"status": "started"}

@app.get("/api/screen/status")
def screen_status():
    with _state_lock:
        return {
            "status":     _state["status"],
            "logs":       list(_state["logs"]),
            "started_at": _state["started_at"],
        }

@app.post("/api/screen/stop")
def screen_stop():
    _state["abort"].set()
    with _state_lock:
        if _state["status"] == "running":
            _state["status"] = "stopped"
    return {"status": "stopping"}

@app.get("/api/prices")
async def current_prices(tickers: str):
    """Latest price for a comma-separated list of NSE tickers (no .NS suffix).

    The fetch is batched and cached in fetch_current_prices(); this used to loop one
    chart request per ticker, which Yahoo throttled after the first symbol or two and
    left the History "Now" column nearly empty. Run off-thread so a slow Yahoo does
    not block the event loop.
    """
    from signals import fetch_current_prices
    syms = [t.strip() for t in tickers.split(",") if t.strip()]
    return await asyncio.to_thread(fetch_current_prices, syms)

@app.get("/api/analyse")
def analyse(ticker: str):
    from signals import analyse_stock
    return analyse_stock(ticker)

@app.get("/api/health")
def health():
    return {"status": "ok", "time": datetime.now().isoformat()}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
