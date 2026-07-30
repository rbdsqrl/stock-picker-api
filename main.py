from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
import uvicorn
import threading
import asyncio
import logging
import os
from datetime import datetime
from signals import run_screening, run_screening_combined
from database import init_db, get_today_picks, get_today_watchlist, get_history, get_watchlist_history, save_pick, save_watchlist_picks, update_outcome, check_and_update_target_hits

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
_state_lock = threading.Lock()
_state = {
    "status": "idle",   # idle | running | done | stopped | error
    "logs":   [],
    "abort":  threading.Event(),
}

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
    with _state_lock:
        if _state["status"] == "running":
            return
        _state["status"] = "running"
        _state["logs"] = []
        _state["abort"].clear()
    try:
        ready_picks, watchlist_picks = run_screening_combined(log_cb=_emit, abort_event=_state["abort"])
        for pick in ready_picks:
            save_pick(pick, rank=pick["rank"])
        save_watchlist_picks(watchlist_picks)
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
        result = await asyncio.to_thread(check_and_update_target_hits, True)
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
        }
    except Exception as e:
        log.exception(f"refresh_outcomes: failed — {e}")
        raise

@app.post("/api/screen/run")
def manual_run(background_tasks: BackgroundTasks):
    with _state_lock:
        if _state["status"] == "running":
            return {"status": "already_running"}
    background_tasks.add_task(run_and_save)
    return {"status": "started"}

@app.get("/api/screen/status")
def screen_status():
    with _state_lock:
        return {
            "status": _state["status"],
            "logs":   list(_state["logs"]),
        }

@app.post("/api/screen/stop")
def screen_stop():
    _state["abort"].set()
    with _state_lock:
        if _state["status"] == "running":
            _state["status"] = "stopped"
    return {"status": "stopping"}

@app.get("/api/prices")
def current_prices(tickers: str):
    """Return latest close price for a comma-separated list of NSE tickers (no .NS suffix)."""
    import yfinance as yf
    from datetime import date as _date, timedelta
    import pandas as pd

    start = (_date.today() - timedelta(days=7)).isoformat()
    end   = (_date.today() + timedelta(days=1)).isoformat()

    result = {}
    for sym in [t.strip() for t in tickers.split(",") if t.strip()]:
        try:
            df = yf.Ticker(sym + ".NS").history(start=start, end=end, actions=False)
            if df.empty:
                continue
            # Strip TZ-aware index before any pandas ops to avoid SystemError/SIGBUS
            df = df.reset_index(drop=True)
            price = pd.to_numeric(df["Close"], errors="coerce").dropna()
            if not price.empty:
                result[sym] = round(float(price.iloc[-1]), 2)
        except (Exception, SystemError) as e:
            log.warning(f"current_prices: {sym} failed — {e}")
    return result

@app.get("/api/watchlist/today")
def today_watchlist():
    picks = get_today_watchlist()
    if not picks:
        return {"status": "no_picks", "picks": []}
    return {"status": "ok", "picks": picks}

@app.get("/api/watchlist/history")
def watchlist_history():
    rows = get_watchlist_history(limit=30)
    return {"picks": rows}

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
