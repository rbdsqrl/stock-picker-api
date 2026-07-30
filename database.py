import psycopg2
import psycopg2.extras
from datetime import date as date_type, timedelta
import json
import os
import logging

log = logging.getLogger(__name__)

_DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_conn():
    url = _DATABASE_URL
    # Render provides postgres:// but psycopg2 needs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url)


def _cur(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def _col_names(conn, table: str) -> set:
    cur = _cur(conn)
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = %s",
        (table,),
    )
    return {row["column_name"] for row in cur.fetchall()}


def init_db():
    conn = get_conn()
    try:
        cur = _cur(conn)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS picks (
                id              SERIAL PRIMARY KEY,
                date            TEXT    NOT NULL,
                rank            INTEGER NOT NULL DEFAULT 1,
                ticker          TEXT    NOT NULL,
                company         TEXT,
                sector          TEXT,
                price_at_pick   REAL,
                score           REAL,
                signals         TEXT,
                rationale       TEXT,
                news            TEXT,
                fundamentals    TEXT,
                stop_loss       REAL,
                stop_pct        REAL,
                target          REAL,
                target_pct      REAL,
                target_short     REAL,
                target_short_pct REAL,
                entry_breakout  REAL,
                atr_14          REAL,
                rr_ratio        REAL,
                target_days_est INTEGER,
                target_short_hit      INTEGER,
                target_short_hit_date TEXT,
                target_short_hit_days INTEGER,
                target_hit            INTEGER,
                target_hit_date       TEXT,
                target_hit_days       INTEGER,
                sl_hit                INTEGER,
                sl_hit_date           TEXT,
                sl_hit_days           INTEGER,
                fundamental_floor_pct REAL,
                outcome_price   REAL,
                outcome_pct     REAL,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(date, rank)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS watchlist_picks (
                id                SERIAL PRIMARY KEY,
                date              TEXT    NOT NULL,
                rank              INTEGER NOT NULL DEFAULT 1,
                ticker            TEXT    NOT NULL,
                company           TEXT,
                sector            TEXT,
                price_at_pick     REAL,
                early_score       REAL,
                pct_from_52w_high REAL,
                setup_summary     TEXT,
                watch_for         TEXT,
                signals           TEXT,
                fundamentals      TEXT,
                stop_loss         REAL,
                stop_pct          REAL,
                target            REAL,
                target_pct        REAL,
                entry_breakout    REAL,
                atr_14            REAL,
                rr_ratio          REAL,
                target_days_est   INTEGER,
                screened_count    INTEGER,
                run_at            TEXT,
                news              TEXT,
                news_sentiment    INTEGER,
                created_at        TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(date, rank)
            )
        """)

        # Additive column migrations — safe to re-run (IF NOT EXISTS)
        for col, typedef in [
            ("fundamentals",          "TEXT"),
            ("entry_breakout",        "REAL"),
            ("atr_14",                "REAL"),
            ("stop_pct",              "REAL"),
            ("target_pct",            "REAL"),
            ("rr_ratio",              "REAL"),
            ("target_days_est",       "INTEGER"),
            ("target_hit",            "INTEGER"),
            ("target_hit_date",       "TEXT"),
            ("target_hit_days",       "INTEGER"),
            ("fundamental_floor_pct", "REAL"),
            ("outcome_price",         "REAL"),
            ("outcome_pct",           "REAL"),
            ("sl_hit",                "INTEGER"),
            ("sl_hit_date",           "TEXT"),
            ("sl_hit_days",           "INTEGER"),
            ("target_short",          "REAL"),
            ("target_short_pct",      "REAL"),
            ("target_short_hit",      "INTEGER"),
            ("target_short_hit_date", "TEXT"),
            ("target_short_hit_days", "INTEGER"),
        ]:
            cur.execute(f"ALTER TABLE picks ADD COLUMN IF NOT EXISTS {col} {typedef}")

        for col, typedef in [
            ("news",             "TEXT"),
            ("news_sentiment",   "INTEGER"),
            ("target_short",     "REAL"),
            ("target_short_pct", "REAL"),
        ]:
            cur.execute(f"ALTER TABLE watchlist_picks ADD COLUMN IF NOT EXISTS {col} {typedef}")

        conn.commit()
    finally:
        conn.close()


def save_pick(pick: dict, rank: int = 1):
    conn = get_conn()
    today = date_type.today().isoformat()
    try:
        cur = _cur(conn)
        cur.execute("""
            INSERT INTO picks
            (date, rank, ticker, company, sector, price_at_pick,
             score, signals, rationale, news, fundamentals,
             stop_loss, stop_pct, target, target_pct, target_short, target_short_pct,
             entry_breakout, atr_14, rr_ratio, target_days_est,
             fundamental_floor_pct)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date, rank) DO UPDATE SET
                ticker                = EXCLUDED.ticker,
                company               = EXCLUDED.company,
                sector                = EXCLUDED.sector,
                price_at_pick         = EXCLUDED.price_at_pick,
                score                 = EXCLUDED.score,
                signals               = EXCLUDED.signals,
                rationale             = EXCLUDED.rationale,
                news                  = EXCLUDED.news,
                fundamentals          = EXCLUDED.fundamentals,
                stop_loss             = EXCLUDED.stop_loss,
                stop_pct              = EXCLUDED.stop_pct,
                target                = EXCLUDED.target,
                target_pct            = EXCLUDED.target_pct,
                target_short          = EXCLUDED.target_short,
                target_short_pct      = EXCLUDED.target_short_pct,
                entry_breakout        = EXCLUDED.entry_breakout,
                atr_14                = EXCLUDED.atr_14,
                rr_ratio              = EXCLUDED.rr_ratio,
                target_days_est       = EXCLUDED.target_days_est,
                fundamental_floor_pct = EXCLUDED.fundamental_floor_pct
        """, (
            today, rank,
            pick["ticker"],
            pick.get("company", ""),
            pick.get("sector", ""),
            pick.get("price"),
            pick.get("score"),
            json.dumps(pick.get("signals", {})),
            pick.get("rationale", ""),
            json.dumps(pick.get("news", [])),
            json.dumps(pick.get("fundamentals", {})),
            pick.get("stop_loss"),
            pick.get("stop_pct"),
            pick.get("target"),
            pick.get("target_pct"),
            pick.get("target_short"),
            pick.get("target_short_pct"),
            pick.get("entry_breakout"),
            pick.get("atr_14"),
            pick.get("rr_ratio"),
            pick.get("target_days_est"),
            pick.get("fundamental_floor_pct"),
        ))
        conn.commit()
    finally:
        conn.close()


def get_today_picks() -> list[dict]:
    conn = get_conn()
    today = date_type.today().isoformat()
    try:
        cur = _cur(conn)
        cur.execute("SELECT * FROM picks WHERE date = %s ORDER BY rank", (today,))
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_history(limit=30) -> list[dict]:
    conn = get_conn()
    try:
        cur = _cur(conn)
        cur.execute("""
            SELECT * FROM picks
            WHERE date IN (
                SELECT DISTINCT date FROM picks ORDER BY date DESC LIMIT %s
            )
            ORDER BY date DESC, rank ASC
        """, (limit,))
        return [_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def check_and_update_target_hits(force: bool = False) -> dict:
    """Resolve each pick into hit / miss / still-pending.

    A pick counts as a MISS when either:
      * the stock CLOSED below the stop before the long target was reached, or
      * 45 days passed with neither level resolved.

    The short target (T1, 1R) is tracked alongside but never resolves a pick — it
    is an early read that the move is underway while T2 is still in play.

    Scoring starts the session AFTER the pick date, and the stop is judged on the
    close rather than the intraday low. Under the old rule (intraday low, pick day
    included) 83% of resolved picks were stop-outs, and 46% of those were dated on
    the pick day itself — i.e. price action that preceded the call.

    On a bar that both closes below the stop and tags the target the SL wins —
    intraday order is unknowable from daily OHLC, so assume the worse outcome.

    Levels are read as stored — entry, target and stop are anchored to the date
    the call was given and are never recomputed here.

    force=True re-resolves picks that already carry an outcome (and clears it
    back to pending if the data now says so). The manual refresh uses this so
    rows scored under the old target-wins-ties logic get corrected; the daily
    scheduled run stays incremental.
    """
    import yfinance as yf
    import pandas as pd

    today = date_type.today()
    conn = get_conn()
    try:
        cur = _cur(conn)
        pending_only = "" if force else "target_hit IS NULL AND "
        cur.execute(
            "SELECT id, ticker, date, target, target_short, stop_loss, "
            "target_hit, sl_hit, target_short_hit FROM picks "
            f"WHERE {pending_only}target IS NOT NULL AND date <= %s",
            (today.isoformat(),),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    changed = 0
    hits    = 0
    t1_hits = 0
    misses  = 0
    pending = 0
    failed  = []

    for row in rows:
        pick_id    = row["id"]
        ticker     = row["ticker"] + ".NS"
        pick_date  = row["date"]
        target     = row["target"]
        target_s   = row["target_short"]
        stop_loss  = row["stop_loss"]
        picked_on  = date_type.fromisoformat(pick_date)
        days_since = (today - picked_on).days

        try:
            fetch_end = (today + timedelta(days=1)).isoformat()
            df = yf.Ticker(ticker).history(start=pick_date, end=fetch_end, actions=False)
            if df.empty:
                continue

            # tz_localize(None) drops the tz keeping IST wall time. tz_convert(None)
            # would shift to UTC first, turning a 00:00 IST bar into 18:30 the previous
            # day and dating every hit/SL one calendar day early.
            bar_dates = [d.date() for d in df.index.tz_localize(None).normalize().to_pydatetime()]

            df = df.reset_index(drop=True)
            df["_bar_date"] = bar_dates
            for col in ("High", "Low", "Close"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["High", "Low", "Close"])
            if df.empty:
                continue

            # Plain lists keep bar dates aligned with prices after the dropna above
            dates  = list(df["_bar_date"])
            highs  = df["High"].tolist()
            closes = df["Close"].tolist()

            # The pick is generated from the pick-day bar itself, so scoring against
            # that same bar tests the call on price action that preceded it. Start
            # from the next session.
            start_i = 1 if dates and dates[0] == picked_on else 0

            # Scan day-by-day: whichever level is resolved first decides the outcome.
            # T1 is recorded in passing and never breaks the loop — it is early
            # confirmation the move is underway, not an exit.
            target_event = None
            sl_event     = None
            t1_event     = None
            for i in range(start_i, len(dates)):
                if t1_event is None and target_s is not None and highs[i] >= target_s:
                    t1_event = dates[i]
                # SL is judged on the CLOSE, not the intraday low: a wick through the
                # stop that recovers by the bell is noise, not a broken thesis. SL is
                # checked first so a bar that both closes below stop and tags the
                # target counts as a miss.
                if stop_loss is not None and closes[i] <= stop_loss:
                    sl_event = dates[i]
                    break
                if highs[i] >= target:
                    target_event = dates[i]
                    break

            if target_event:
                outcome  = (1, target_event.isoformat(), (target_event - picked_on).days, 0, None, None)
                verdict  = f"Target hit: {ticker} pick={pick_date} hit={target_event} days={outcome[2]}"
                hits    += 1
            elif sl_event:
                outcome  = (0, None, None, 1, sl_event.isoformat(), (sl_event - picked_on).days)
                verdict  = f"SL hit before target: {ticker} pick={pick_date} sl={sl_event} days={outcome[5]}"
                misses  += 1
            elif days_since > 45:
                outcome  = (0, None, None, 0, None, None)
                verdict  = f"Target missed (45d expired): {ticker} pick={pick_date}"
                misses  += 1
            else:
                outcome  = (None, None, None, None, None, None)
                verdict  = None
                pending += 1

            t1 = ((1, t1_event.isoformat(), (t1_event - picked_on).days)
                  if t1_event else (0 if target_s is not None else None, None, None))
            if t1_event:
                t1_hits += 1

            prev = (row["target_hit"], row["sl_hit"], row["target_short_hit"])
            if prev == (outcome[0], outcome[3], t1[0]):
                continue  # already resolved this way — nothing to write

            c = get_conn()
            try:
                cur2 = _cur(c)
                cur2.execute(
                    "UPDATE picks SET target_hit=%s, target_hit_date=%s, target_hit_days=%s, "
                    "sl_hit=%s, sl_hit_date=%s, sl_hit_days=%s, "
                    "target_short_hit=%s, target_short_hit_date=%s, target_short_hit_days=%s "
                    "WHERE id=%s",
                    (*outcome, *t1, pick_id),
                )
                c.commit()
                changed += 1
            finally:
                c.close()

            log.info(verdict or f"Outcome cleared (pending): {ticker} pick={pick_date}")

        except Exception as e:
            log.warning(f"check_target_hits: {ticker} failed — {e}")
            failed.append(row["ticker"])

    return {
        "updated": changed,
        "hits":    hits,
        "t1_hits": t1_hits,
        "misses":  misses,
        "pending": pending,
        "failed":  failed,
    }


def recalculate_all_levels() -> int:
    """Backfill stop / T1 / T2 on historical picks under the current policy.

    The ENTRY IS NEVER TOUCHED. price_at_pick stays exactly as recorded, and every
    level is derived from it, so a pick keeps the entry it was actually called at.
    ATR and the swing low come from data up to the pick date only — no post-pick
    information leaks in — which also makes this idempotent: re-running it produces
    the same numbers.

    Outcomes computed against the old levels are stale afterwards, so callers must
    follow this with check_and_update_target_hits(force=True).
    """
    import yfinance as yf
    import pandas as pd
    from signals import compute_trade_levels

    conn = get_conn()
    try:
        cur = _cur(conn)
        cur.execute(
            "SELECT id, ticker, date, price_at_pick, fundamental_floor_pct "
            "FROM picks WHERE price_at_pick IS NOT NULL"
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    updated = 0
    for row in rows:
        pick_id   = row["id"]
        ticker    = row["ticker"] + ".NS"
        pick_date = row["date"]
        entry     = row["price_at_pick"]
        floor_pct = row["fundamental_floor_pct"]

        try:
            start = (date_type.fromisoformat(pick_date) - timedelta(days=90)).isoformat()
            end   = (date_type.fromisoformat(pick_date) + timedelta(days=1)).isoformat()
            df    = yf.Ticker(ticker).history(start=start, end=end, actions=False)

            if df.empty or len(df) < 14:
                continue

            df = df.reset_index(drop=True)
            for col in ("Close", "High", "Low"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["Close", "High", "Low"])
            if len(df) < 14:
                continue

            # entry_price pins every level to the recorded entry
            levels = compute_trade_levels(df["Close"], df["High"], df["Low"], entry_price=entry)

            if floor_pct:
                fund_floor = entry * (1 + floor_pct / 100)
                if fund_floor > levels["target"]:
                    risk = entry - levels["stop_loss"]
                    levels["target"]          = round(fund_floor, 2)
                    levels["target_pct"]      = round(floor_pct, 1)
                    levels["rr_ratio"]        = round((fund_floor - entry) / risk, 2) if risk > 0 else 2.0
                    atr = levels["atr_14"]
                    levels["target_days_est"] = max(5, min(45, round((fund_floor - entry) / atr * 2))) if atr > 0 else 10

            c = get_conn()
            try:
                cur2 = _cur(c)
                # price_at_pick and entry_breakout are deliberately absent — the entry
                # a pick was called at is a fact of record, not something to recompute.
                cur2.execute("""
                    UPDATE picks SET
                        stop_loss=%s, stop_pct=%s, target=%s, target_pct=%s,
                        target_short=%s, target_short_pct=%s,
                        atr_14=%s, rr_ratio=%s, target_days_est=%s
                    WHERE id=%s
                """, (
                    levels["stop_loss"],    levels["stop_pct"],
                    levels["target"],       levels["target_pct"],
                    levels["target_short"], levels["target_short_pct"],
                    levels["atr_14"],       levels["rr_ratio"],
                    levels["target_days_est"],
                    pick_id,
                ))
                c.commit()
                updated += 1
            finally:
                c.close()

            log.info(f"Recalculated: {ticker} {pick_date} → stop={levels['stop_pct']}% target={levels['target_pct']}%")

        except Exception as e:
            log.warning(f"recalculate_levels: {ticker} {pick_date} — {e}")

    # The old blanket "enforce 5% SL / 10% target" sweep that used to run here is
    # gone. It overwrote levels with flat percentages regardless of volatility,
    # which is exactly the behaviour the ATR-based policy replaces.
    return updated


def get_recently_picked_tickers(days: int = 3) -> set:
    conn = get_conn()
    cutoff = (date_type.today() - timedelta(days=days)).isoformat()
    today  = date_type.today().isoformat()
    try:
        cur = _cur(conn)
        cur.execute(
            "SELECT DISTINCT ticker FROM picks WHERE date >= %s AND date < %s",
            (cutoff, today),
        )
        return {row["ticker"] for row in cur.fetchall()}
    finally:
        conn.close()


def save_watchlist_picks(picks: list[dict]):
    if not picks:
        return
    conn = get_conn()
    today = date_type.today().isoformat()
    try:
        cur = _cur(conn)
        for pick in picks:
            cur.execute("""
                INSERT INTO watchlist_picks
                (date, rank, ticker, company, sector, price_at_pick,
                 early_score, pct_from_52w_high, setup_summary, watch_for,
                 signals, fundamentals,
                 stop_loss, stop_pct, target, target_pct,
                 entry_breakout, atr_14, rr_ratio, target_days_est,
                 screened_count, run_at, news, news_sentiment)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (date, rank) DO UPDATE SET
                    ticker            = EXCLUDED.ticker,
                    company           = EXCLUDED.company,
                    sector            = EXCLUDED.sector,
                    price_at_pick     = EXCLUDED.price_at_pick,
                    early_score       = EXCLUDED.early_score,
                    pct_from_52w_high = EXCLUDED.pct_from_52w_high,
                    setup_summary     = EXCLUDED.setup_summary,
                    watch_for         = EXCLUDED.watch_for,
                    signals           = EXCLUDED.signals,
                    fundamentals      = EXCLUDED.fundamentals,
                    stop_loss         = EXCLUDED.stop_loss,
                    stop_pct          = EXCLUDED.stop_pct,
                    target            = EXCLUDED.target,
                    target_pct        = EXCLUDED.target_pct,
                    entry_breakout    = EXCLUDED.entry_breakout,
                    atr_14            = EXCLUDED.atr_14,
                    rr_ratio          = EXCLUDED.rr_ratio,
                    target_days_est   = EXCLUDED.target_days_est,
                    screened_count    = EXCLUDED.screened_count,
                    run_at            = EXCLUDED.run_at,
                    news              = EXCLUDED.news,
                    news_sentiment    = EXCLUDED.news_sentiment
            """, (
                today, pick.get("rank", 1),
                pick["ticker"],
                pick.get("company", ""),
                pick.get("sector", ""),
                pick.get("price"),
                pick.get("early_score"),
                pick.get("pct_from_52w_high"),
                pick.get("setup_summary", ""),
                pick.get("watch_for", ""),
                json.dumps(pick.get("signals", {})),
                json.dumps(pick.get("fundamentals", {})),
                pick.get("stop_loss"),
                pick.get("stop_pct"),
                pick.get("target"),
                pick.get("target_pct"),
                pick.get("entry_breakout"),
                pick.get("atr_14"),
                pick.get("rr_ratio"),
                pick.get("target_days_est"),
                pick.get("screened_count"),
                pick.get("run_at"),
                json.dumps(pick.get("news", [])),
                pick.get("news_sentiment", 0),
            ))
        conn.commit()
    finally:
        conn.close()


def get_watchlist_history(limit=30) -> list[dict]:
    conn = get_conn()
    try:
        cur = _cur(conn)
        cur.execute("""
            SELECT * FROM watchlist_picks
            WHERE date IN (
                SELECT DISTINCT date FROM watchlist_picks ORDER BY date DESC LIMIT %s
            )
            ORDER BY date DESC, rank ASC
        """, (limit,))
        return [_watchlist_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_today_watchlist() -> list[dict]:
    conn = get_conn()
    today = date_type.today().isoformat()
    try:
        cur = _cur(conn)
        cur.execute("SELECT * FROM watchlist_picks WHERE date = %s ORDER BY rank", (today,))
        return [_watchlist_row_to_dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _watchlist_row_to_dict(row):
    d = dict(row)
    for field in ("signals", "fundamentals", "news"):
        if d.get(field):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                pass
    d["price"]     = d.get("price_at_pick")
    d["entry_cmp"] = d.get("price_at_pick")
    return d


def update_outcome(pick_date: str, outcome_price: float):
    conn = get_conn()
    try:
        cur = _cur(conn)
        cur.execute(
            "SELECT price_at_pick FROM picks WHERE date = %s AND rank = 1", (pick_date,)
        )
        row = cur.fetchone()
        if row and row["price_at_pick"]:
            pct = ((outcome_price - row["price_at_pick"]) / row["price_at_pick"]) * 100
            cur.execute(
                "UPDATE picks SET outcome_price = %s, outcome_pct = %s WHERE date = %s AND rank = 1",
                (outcome_price, round(pct, 2), pick_date),
            )
            conn.commit()
    finally:
        conn.close()


def _row_to_dict(row):
    d = dict(row)
    for field in ("signals", "news", "fundamentals"):
        if d.get(field):
            try:
                d[field] = json.loads(d[field])
            except Exception:
                pass
    d["price"]     = d.get("price_at_pick")
    d["entry_cmp"] = d.get("price_at_pick")
    return d
