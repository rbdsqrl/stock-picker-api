"""
Capitulation-Reversal Screen — StockPick

The counterpart to the momentum screen in signals.py, and deliberately a separate
screen rather than extra signals bolted onto that one. The two setups have opposite
signatures: signal_trend() wants price above both SMAs, signal_breakout() rejects
anything more than 20% below the 52-week high (`pct_from_high < -20` → -1). Every
major bottom violates both by construction, so a stock at the exact moment of
reversal scores near the floor of the momentum composite. Blending the two into one
weighted score does not fix that — it just makes a screen that is mediocre at both.
So: same universe, same liquidity filter, separate scoring path, separate output.

The setup this looks for, in the order the conditions must occur in time:

  1. WASHOUT      an established downtrend, not a dip — deep drawdown from the
                  52-week high, sustained weeks below the 30-week average.
  2. CLIMAX       one week at a 52-week low on enormous volume. Forced sellers
                  finishing against a buyer large enough to absorb them.
  3. TRIGGER      the first weekly close back above that climax week's high.

Entry is the trigger close; the stop is the climax low. If price returns below the
low that was made on capitulation volume, the absorption thesis is simply wrong.

Everything runs on WEEKLY bars. Daily bars are the wrong resolution here: a
capitulation low is a multi-week event, and the volume signature that identifies it
is only legible once the daily noise is summed away. It also needs far more history
than the momentum screen — the base a stock reclaims on the way out is often two or
three years old, so this reads 5y where screen_stock() reads 1y.

READ_THIS — what the backtest actually says
-------------------------------------------
Measured over 442 Nifty 500 names with usable history, 2016-2026, weekly bars, no
lookahead (detect_reversal(w, t) reads only w[:t+1], asserted in the harness):

  * At the settings below the screen fires ~8 times a year across the whole
    universe. 73% of signals are positive 52 weeks later, mean +49.1%, against a
    universe base rate of 66.6% positive / +35.1% mean.
  * That margin is mostly ONE REGIME. Drop calendar 2020 and the arm returns
    +25.4% mean / +13.0% median against an ex-2020 base rate of +26.1% / +12.4%.
    What survives ex-2020 is the hit rate — 71.6% positive vs 62.9% — and not the
    return. Treat this as a screen that finds survivors more reliably than random,
    NOT as a screen that beats buying the index.
  * The universe is today's Nifty 500 back-projected, so stocks that capitulated
    and never came back are largely absent. Everything above is optimistic by an
    unknown margin. Relative comparisons between gates are unaffected.
  * The control arm matters: "any 52-week low that gets reclaimed", with no
    drawdown, downtrend or volume gate, fires 755 times and returns +24.9% mean /
    61.7% positive — BELOW the base rate. The gates are what create the edge;
    the pattern on its own is worth nothing.

SIS, the setup this module was written for, is a cautionary case and does NOT fire.
Its 2026 low at 257 cleared every washout gate (-45.9% from the 2-year high, 25 of
26 weeks below the 30-week average) and failed only on volume: the low printed at
0.93x average weekly volume and the reclaim week at 0.30x. The 9.6x volume bar came
six weeks later at a close of 390 — a third of the way up the move, not at the low.
Relaxing both volume gates produces the entry at 302.8 on 2026-04-10 with a stop at
254.5, which is the trade a discretionary reader of that chart wanted. It also turns
the screen into the quiet-low variant that underperforms a random entry, and on SIS
itself the earlier textbook-looking signal — 2025-04-04, entry 327.4, a proper 4.6x
climax low — lost 12.1% over the following year. One good outcome is not evidence.
"""

import pandas as pd
import numpy as np

from signals import _atr, passes_liquidity

# ── Detection thresholds ─────────────────────────────────────────────────────
# All in weekly units. Every value below is the one that won an ablation over the
# Nifty 500, 2016-2026, 442 stocks with usable history. Read the honest summary in
# the module docstring for READ_THIS before trusting any of it — the edge here is
# narrow and regime-dependent, and two of these gates were set the WRONG way in the
# first draft.

MIN_HISTORY_WEEKS = 130    # 2.5 years. Set by DRAWDOWN_LOOKBACK below, not by the
                           # 52-week windows — the drawdown gate needs its full
                           # lookback before it means anything, and a stock that
                           # listed 18 months ago has no measurable "prior high".

# The washout. Measured at the climax bar, not at the trigger — by the time the
# trigger fires price has already bounced, so measuring the drawdown there would
# understate how deep the hole was and let shallow pullbacks through.
#
# 45% rather than the obvious 35%: at a 35% floor the screen returned +2.3% mean
# over 52 weeks against a +35.1% universe base rate, i.e. it was actively worse
# than buying at random. Raising it to 45% moved that to +49.1% mean / 73.4% win.
# 60% is better still (+109% mean) but fires twice a year across 442 stocks, which
# is too thin to allocate against.
MIN_DRAWDOWN_PCT   = 45.0
# 104 weeks, not 52. A stock that bleeds out slowly never shows a big drawdown in
# any single 52-week window, because the window's own high steps down with price.
# SIS fell 581 -> 257 over two years and never once printed a 52-week drawdown past
# -41%, so a 52-week lookback cannot see that decline at all.
DRAWDOWN_LOOKBACK  = 104
DOWNTREND_WINDOW   = 26    # weeks examined for trend persistence
MIN_WEEKS_BELOW_MA = 20    # of those, how many must close below the 30-week SMA
TREND_MA_WEEKS     = 30

# The climax bar. The volume gate is the single load-bearing filter in this module:
# holding every other gate fixed, lows made on >=2x average weekly volume returned
# +49.1% mean / 73.4% positive over 52 weeks, while lows made on <=1.0x volume
# returned +27.1% / 57.7% — the quiet ones underperform a random entry. 3x is worse
# than 2x (+40.0% / 71.8%): past a point the gate is just shrinking the sample.
CLIMAX_VOL_MULT  = 2.0
CLIMAX_VOL_LOOKBACK = 20
CLIMAX_MAX_AGE   = 8       # trigger must arrive within this many weeks of the climax
# Requiring the climax bar to close in the upper part of its own range is intuitive
# and measurably harmful — it cost ~16 percentage points of mean return by dropping
# lows that closed weak and reversed the following week. Off by default; the
# threshold is kept only so require_close_loc=True remains testable.
CLIMAX_CLOSE_LOC = 0.40
REQUIRE_CLOSE_LOC = False

# The trigger. Kept at 1.0 (reclaim on at least average volume) because a reclaim on
# no participation is usually drift rather than demand — but see the SIS note in the
# docstring: this gate is what blocks the one setup that prompted the whole module.
TRIGGER_VOL_MULT = 1.0

# Risk. The stop is structural (below the climax low), not ATR-derived, so it is
# frequently wider than signals.MAX_RISK_PCT = 9. That ceiling is correct for a
# momentum continuation entry and wrong here: there is no meaningful stop between a
# reclaim entry and the low it reclaimed.
#
# Be aware of what the stop costs. Over the same signal set, exiting at a 2R target
# with this stop returned +7.4% mean; holding the identical signals for 52 weeks with
# no stop at all returned +49.1%. The structural stop is hit on roughly 39% of trades
# and a large share of those go on to be the biggest winners (TATAPOWER +221%,
# NEULANDLAB +141%, OLECTRA +130% were all stopped out first). The stop is here
# because an unstopped screen is not a tradeable product, not because it adds return.
STOP_BUFFER_PCT = 1.0
MAX_RISK_PCT    = 22.0
# 3R rather than 2R: same signals, E[R] 0.62 vs 0.54, mean +8.5% vs +7.4%. Both are
# far below what the setup actually pays when left alone.
TARGET_R        = 3.0


# ── Weekly bars ──────────────────────────────────────────────────────────────

def to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate daily OHLCV into weekly bars, grouped Saturday-to-Friday.

    Done with numpy reduceat rather than DataFrame.resample(). This is not a
    micro-optimisation: pandas' resample C path segfaults (SIGBUS) on the
    pandas 2.2 / numpy 1.26 pairing this project pins, on exactly the frames
    yfinance hands back — the same family of C-extension crash the comment in
    screen_stock() already works around for _atr(). reduceat touches none of that
    machinery, and being O(n) over a pre-sorted key it is also faster.

    Grouping key: days since the epoch, offset so each bucket runs Sat→Fri.
    1970-01-01 was a Thursday, so day 2 is the first Saturday. Weeks are labelled
    with the LAST TRADING DATE in the bucket, not a synthetic Friday — the whole
    screen reasons about "as of this bar's close", and a holiday-shortened week
    closes when it closes.
    """
    d = df.copy()
    if "Date" not in d.columns:
        d = d.reset_index()

    dates = pd.to_datetime(d["Date"])
    if getattr(dates.dt, "tz", None) is not None:
        dates = dates.dt.tz_localize(None)

    cols = {c: pd.to_numeric(d[c], errors="coerce").to_numpy(dtype="float64")
            for c in ("Open", "High", "Low", "Close", "Volume")}
    keep = ~np.isnan(np.column_stack(list(cols.values()))).any(axis=1)
    if not keep.any():
        return pd.DataFrame(columns=["Date", "Open", "High", "Low", "Close", "Volume"])

    dates = dates[keep].reset_index(drop=True)
    cols = {k: v[keep] for k, v in cols.items()}

    days = dates.to_numpy().astype("datetime64[D]").astype("int64")
    order = np.argsort(days, kind="stable")
    days = days[order]
    dates = dates.iloc[order].reset_index(drop=True)
    cols = {k: v[order] for k, v in cols.items()}

    week = (days - 2) // 7
    starts = np.concatenate(([0], np.flatnonzero(np.diff(week)) + 1))
    ends = np.append(starts[1:] - 1, len(week) - 1)

    w = pd.DataFrame({
        "Date":   dates.iloc[ends].reset_index(drop=True),
        "Open":   cols["Open"][starts],
        "High":   np.maximum.reduceat(cols["High"], starts),
        "Low":    np.minimum.reduceat(cols["Low"], starts),
        "Close":  cols["Close"][ends],
        "Volume": np.add.reduceat(cols["Volume"], starts),
    })
    # A week with no traded volume carries no information about participation and
    # would drag down the 20-week average, manufacturing spikes in the week after.
    return w[w["Volume"] > 0].reset_index(drop=True)


def _rsi_series(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ── Detection ────────────────────────────────────────────────────────────────

def _find_climax(w: pd.DataFrame, t: int, cfg: dict) -> int | None:
    """Index of the most recent qualifying climax bar at or before week t.

    Reads only w[:t+1]. Returns the LATEST qualifying bar in the window — if a stock
    capitulates twice, the second low is the one the reclaim is measured against.
    """
    low, high, close, vol = w["Low"], w["High"], w["Close"], w["Volume"]
    lookback = cfg["climax_vol_lookback"]

    oldest = max(cfg["min_history"] - 1, t - cfg["climax_max_age"])
    for c in range(t, oldest - 1, -1):
        if c - lookback < 0 or c < 52:
            continue

        # Volume average strictly before the bar, so the spike cannot inflate its
        # own baseline.
        base_vol = float(vol.iloc[c - lookback:c].mean())
        if base_vol <= 0:
            continue
        if vol.iloc[c] < cfg["climax_vol_mult"] * base_vol:
            continue
        # Upper bound, used to test the opposite hypothesis: that the low is made on
        # DRIED-UP volume (sellers exhausted through absence rather than through one
        # violent flush). Off by default — see backtest notes.
        if cfg["climax_vol_max"] is not None and vol.iloc[c] > cfg["climax_vol_max"] * base_vol:
            continue

        # Must be the lowest low of the trailing year.
        if float(low.iloc[c]) > float(low.iloc[c - 51:c + 1].min()):
            continue

        if cfg["require_close_loc"]:
            rng = float(high.iloc[c] - low.iloc[c])
            if rng <= 0:
                continue
            loc = (float(close.iloc[c]) - float(low.iloc[c])) / rng
            reclaimed = (c + 1 <= t and
                         float(close.iloc[c + 1]) > float(low.iloc[c]) + 0.5 * rng)
            if loc < cfg["climax_close_loc"] and not reclaimed:
                continue

        return c
    return None


def _washout_ok(w: pd.DataFrame, c: int, cfg: dict) -> tuple[bool, dict]:
    """Was the move into bar c an established downtrend, and how deep?"""
    high, low, close = w["High"], w["Low"], w["Close"]

    # The high preceding the climax low, and the drawdown into it. The lookback is
    # longer than a year on purpose: a slow bleed that takes two years to complete
    # never shows a large drawdown inside any single 52-week window, because the
    # window's own high keeps stepping down with price. Measuring from the 2-year
    # high is what separates "this stock has actually been destroyed" from "this
    # stock fell hard in the last twelve months".
    lb = cfg["drawdown_lookback"]
    prior_high = float(high.iloc[max(0, c - lb + 1):c + 1].max())
    if prior_high <= 0:
        return False, {}
    drawdown = (float(low.iloc[c]) - prior_high) / prior_high * 100.0

    sma = close.rolling(cfg["trend_ma"]).mean()
    window = slice(c - cfg["downtrend_window"] + 1, c + 1)
    below = int((close.iloc[window] < sma.iloc[window]).sum())

    details = {
        "drawdown_pct": round(drawdown, 1),
        "weeks_below_ma": below,
        "prior_high": round(prior_high, 2),
    }
    ok = (drawdown <= -cfg["min_drawdown"]) and (below >= cfg["min_weeks_below_ma"])
    return ok, details


def detect_reversal(w: pd.DataFrame, t: int | None = None, **overrides) -> dict | None:
    """Evaluate the capitulation-reversal setup as of the close of weekly bar t.

    Reads only w[:t+1] — no bar after t is touched anywhere in this path, which is
    what makes the backtest honest. Returns None when the setup is absent, or a dict
    describing the trade when the trigger fires on bar t exactly.

    The trigger fires on ONE bar only: the first weekly close back above the climax
    high. Later weeks that are also above it return None, so a single reversal
    produces a single signal rather than one every week while the rally runs.
    """
    cfg = {
        "min_history":         MIN_HISTORY_WEEKS,
        "min_drawdown":        MIN_DRAWDOWN_PCT,
        "drawdown_lookback":   DRAWDOWN_LOOKBACK,
        "downtrend_window":    DOWNTREND_WINDOW,
        "min_weeks_below_ma":  MIN_WEEKS_BELOW_MA,
        "trend_ma":            TREND_MA_WEEKS,
        "climax_vol_mult":     CLIMAX_VOL_MULT,
        "climax_vol_max":      None,
        "climax_vol_lookback": CLIMAX_VOL_LOOKBACK,
        "climax_max_age":      CLIMAX_MAX_AGE,
        "climax_close_loc":    CLIMAX_CLOSE_LOC,
        "require_close_loc":   REQUIRE_CLOSE_LOC,
        "trigger_vol_mult":    TRIGGER_VOL_MULT,
        "require_ma10_reclaim": False,
        "max_risk_pct":        MAX_RISK_PCT,
    }
    cfg.update(overrides)

    if t is None:
        t = len(w) - 1
    if t < cfg["min_history"] or t >= len(w):
        return None

    c = _find_climax(w, t, cfg)
    if c is None or c >= t:
        return None

    high, low, close, vol = w["High"], w["Low"], w["Close"], w["Volume"]
    climax_high = float(high.iloc[c])

    # Trigger: close above the climax high, and the FIRST such close since it.
    # Checked before the washout gate purely for cost — it is two comparisons
    # against the rolling average _washout_ok() has to build, and it rejects the
    # overwhelming majority of bars.
    entry = float(close.iloc[t])
    if entry <= climax_high:
        return None
    if c + 1 <= t - 1 and float(close.iloc[c + 1:t].max()) > climax_high:
        return None

    ok, wash = _washout_ok(w, c, cfg)
    if not ok:
        return None

    base_vol = float(vol.iloc[t - cfg["climax_vol_lookback"]:t].mean())
    trigger_vol_ratio = float(vol.iloc[t]) / base_vol if base_vol > 0 else 0.0
    if trigger_vol_ratio < cfg["trigger_vol_mult"]:
        return None

    if cfg["require_ma10_reclaim"]:
        ma10 = float(close.iloc[t - 9:t + 1].mean())
        if entry <= ma10:
            return None

    climax_low = float(low.iloc[c])
    stop = round(climax_low * (1 - STOP_BUFFER_PCT / 100), 2)
    risk = entry - stop
    if risk <= 0:
        return None
    risk_pct = risk / entry * 100
    if risk_pct > cfg["max_risk_pct"]:
        return None

    base_climax_vol = float(vol.iloc[c - cfg["climax_vol_lookback"]:c].mean())
    rsi_w = _rsi_series(close.iloc[:t + 1])

    return {
        "trigger_idx":     t,
        "climax_idx":      c,
        "weeks_since_climax": t - c,
        "entry":           round(entry, 2),
        "stop":            stop,
        "risk_pct":        round(risk_pct, 1),
        "target":          round(entry + TARGET_R * risk, 2),
        "target_pct":      round(TARGET_R * risk / entry * 100, 1),
        "climax_low":      round(climax_low, 2),
        "climax_high":     round(climax_high, 2),
        "climax_vol_ratio": round(float(vol.iloc[c]) / base_climax_vol, 1) if base_climax_vol > 0 else None,
        "trigger_vol_ratio": round(trigger_vol_ratio, 1),
        "rsi_w_at_climax": round(float(rsi_w.iloc[c]), 1) if not np.isnan(rsi_w.iloc[c]) else None,
        "rsi_w_at_trigger": round(float(rsi_w.iloc[t]), 1) if not np.isnan(rsi_w.iloc[t]) else None,
        "atr_w":           round(_atr(high.iloc[:t + 1], low.iloc[:t + 1], close.iloc[:t + 1]), 2),
        **wash,
    }


# ── Screening entry point ────────────────────────────────────────────────────

def screen_reversal(ticker: str, df_daily: pd.DataFrame) -> dict | None:
    """Run the reversal screen on one stock's daily history.

    Takes an already-fetched frame rather than fetching its own, so a caller running
    both screens over the universe spends one chart request per stock, not two — the
    request budget in CLAUDE.md leaves no room for a second pass.
    """
    # MIN_HISTORY_WEEKS of weekly bars needs roughly five trading days per week of
    # daily history; 650 is that with slack for holidays and halts.
    if df_daily is None or df_daily.empty or len(df_daily) < 650:
        return None

    close_d = pd.to_numeric(df_daily["Close"], errors="coerce").dropna()
    vol_d = pd.to_numeric(df_daily["Volume"], errors="coerce").dropna()
    if len(close_d) < 650 or not passes_liquidity(vol_d, close_d):
        return None

    w = to_weekly(df_daily)
    if len(w) < MIN_HISTORY_WEEKS:
        return None

    setup = detect_reversal(w)
    if setup is None:
        return None

    setup["ticker"] = ticker.replace(".NS", "")
    setup["climax_date"] = str(w["Date"].iloc[setup["climax_idx"]].date())
    setup["trigger_date"] = str(w["Date"].iloc[setup["trigger_idx"]].date())
    setup["rr_ratio"] = TARGET_R
    return setup
