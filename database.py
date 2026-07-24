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
                entry_breakout  REAL,
                atr_14          REAL,
                rr_ratio        REAL,
                target_days_est INTEGER,
                target_hit            INTEGER,
                target_hit_date       TEXT,
                target_hit_days       INTEGER,
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
        ]:
            cur.execute(f"ALTER TABLE picks ADD COLUMN IF NOT EXISTS {col} {typedef}")

        for col, typedef in [
            ("news",           "TEXT"),
            ("news_sentiment", "INTEGER"),
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
             stop_loss, stop_pct, target, target_pct,
             entry_breakout, atr_14, rr_ratio, target_days_est,
             fundamental_floor_pct)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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


def check_and_update_target_hits() -> dict:
    import yfinance as yf
    import pandas as pd

    today = date_type.today()
    conn = get_conn()
    try:
        cur = _cur(conn)
        cur.execute(
            "SELECT id, ticker, date, target FROM picks "
            "WHERE target_hit IS NULL AND target IS NOT NULL AND date <= %s",
            (today.isoformat(),),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    updated = 0
    failed  = []

    for row in rows:
        pick_id    = row["id"]
        ticker     = row["ticker"] + ".NS"
        pick_date  = row["date"]
        target     = row["target"]
        days_since = (today - date_type.fromisoformat(pick_date)).days

        try:
            fetch_end = (today + timedelta(days=1)).isoformat()
            df = yf.Ticker(ticker).history(start=pick_date, end=fetch_end, actions=False)
            if df.empty:
                continue

            row_dates = df.index.tz_convert(None).normalize().to_pydatetime()
            row_dates = [d.date() for d in row_dates]

            df = df.reset_index(drop=True)
            df["High"]  = pd.to_numeric(df["High"],  errors="coerce")
            df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
            df = df.dropna(subset=["High"])
            if df.empty:
                continue

            hit_mask = (df["High"] >= target) | (df["Close"].fillna(0) >= target)
            c = get_conn()
            try:
                cur2 = _cur(c)
                if hit_mask.any():
                    hit_pos  = int(hit_mask.idxmax())
                    hit_date = row_dates[hit_pos].isoformat()
                    hit_days = (row_dates[hit_pos] - date_type.fromisoformat(pick_date)).days
                    cur2.execute(
                        "UPDATE picks SET target_hit=1, target_hit_date=%s, target_hit_days=%s WHERE id=%s",
                        (hit_date, hit_days, pick_id),
                    )
                    log.info(f"Target hit: {ticker} pick={pick_date} hit={hit_date} days={hit_days}")
                elif days_since > 90:
                    cur2.execute("UPDATE picks SET target_hit=0 WHERE id=%s", (pick_id,))
                    log.info(f"Target missed (90d expired): {ticker} pick={pick_date}")
                c.commit()
                updated += 1
            finally:
                c.close()

        except Exception as e:
            log.warning(f"check_target_hits: {ticker} failed — {e}")
            failed.append(row["ticker"])

    return {"updated": updated, "failed": failed}


def recalculate_all_levels() -> int:
    import yfinance as yf
    import pandas as pd
    from signals import compute_trade_levels

    conn = get_conn()
    try:
        cur = _cur(conn)
        cur.execute("SELECT id, ticker, date, fundamental_floor_pct FROM picks WHERE price_at_pick IS NOT NULL")
        rows = cur.fetchall()
    finally:
        conn.close()

    updated = 0
    for row in rows:
        pick_id   = row["id"]
        ticker    = row["ticker"] + ".NS"
        pick_date = row["date"]
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

            levels = compute_trade_levels(df["Close"], df["High"], df["Low"])

            if floor_pct:
                price      = levels["entry_cmp"]
                fund_floor = price * (1 + floor_pct / 100)
                if fund_floor > levels["target"]:
                    risk = price - levels["stop_loss"]
                    levels["target"]          = round(fund_floor, 2)
                    levels["target_pct"]      = round(floor_pct, 1)
                    levels["rr_ratio"]        = round((fund_floor - price) / risk, 2) if risk > 0 else 2.0
                    atr = levels["atr_14"]
                    levels["target_days_est"] = max(5, min(45, round((fund_floor - price) / atr * 2))) if atr > 0 else 10

            c = get_conn()
            try:
                cur2 = _cur(c)
                cur2.execute("""
                    UPDATE picks SET
                        stop_loss=%s, stop_pct=%s, target=%s, target_pct=%s,
                        atr_14=%s, rr_ratio=%s, target_days_est=%s, entry_breakout=%s
                    WHERE id=%s
                """, (
                    levels["stop_loss"], levels["stop_pct"],
                    levels["target"],    levels["target_pct"],
                    levels["atr_14"],    levels["rr_ratio"],
                    levels["target_days_est"], levels["entry_breakout"],
                    pick_id,
                ))
                c.commit()
                updated += 1
            finally:
                c.close()

            log.info(f"Recalculated: {ticker} {pick_date} → stop={levels['stop_pct']}% target={levels['target_pct']}%")

        except Exception as e:
            log.warning(f"recalculate_levels: {ticker} {pick_date} — {e}")

    # Enforce hard limits on bad/stale picks
    c = get_conn()
    try:
        cur2 = _cur(c)
        cur2.execute("""
            UPDATE picks
            SET stop_loss  = ROUND(CAST(price_at_pick * 0.97 AS numeric), 2),
                stop_pct   = 3.0,
                target     = ROUND(CAST(price_at_pick * 1.06 AS numeric), 2),
                target_pct = 6.0,
                rr_ratio   = 2.0
            WHERE price_at_pick IS NOT NULL
              AND (target_pct < 6.0 OR target_pct IS NULL
                   OR stop_pct > 3.0 OR stop_pct IS NULL
                   OR rr_ratio IS NULL OR rr_ratio < 2.0)
        """)
        bad = cur2.rowcount
        c.commit()
    finally:
        c.close()

    if bad:
        log.info(f"Enforced 6%% minimum on {bad} pick(s) with stale targets")
        updated += bad

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
