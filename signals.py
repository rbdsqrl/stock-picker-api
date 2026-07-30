"""
Signal Engine — StockPick
Screens Nifty 500 and returns the top 3 ranked picks.

Signals computed per stock:
  1. Trend        — price vs 50DMA and 200DMA
  2. Momentum     — RSI (14) in the 40-65 sweet spot
  3. Volume       — today vs 20-day avg
  4. Breakout     — price near/above recent 52w high zone
  5. Rel Strength — stock vs Nifty 50 (1M return comparison)
  6. News         — sentiment from last 5 days of articles (top 10 candidates only)

Each signal is scored -1 / 0 / +1. Composite score = weighted sum.
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone
import logging
import urllib.request
import ssl
import certifi
import io
import time

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── Nifty 500 universe ────────────────────────────────────────────────────────
# Fetched live from NSE on each run; falls back to snapshot below if unreachable.
_NSE_URL = "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv"
_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/",
}

# Snapshot of Nifty 500 constituents (as of mid-2024). Used when NSE is unreachable.
_NIFTY500_FALLBACK = [
    "360ONE","3MINDIA","ABB","ACC","AIAENG","APLAPOLLO","ATGL","AUBANK","AAVAS",
    "ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ADANIPOWER","ADANITRANS",
    "ABCAPITAL","ABFRL","ADVENZYMES","AEGISCHEM","AETHER","AJANTPHARM","AKZOINDIA",
    "APLLTD","ALKEM","ALLCARGO","AMARAJABAT","AMBUJACEM","ANGELONE","ANURAS",
    "APOLLOHOSP","APOLLOTYRE","ARVINDFASN","ASAHIINDIA","ASHOKLEY","ASIANPAINT",
    "ASTERDM","ASTRAL","ATUL","AUROPHARMA","AVANTIFEED","DMART","AXISBANK",
    "BASF","BEML","BSE","BAJAJ-AUTO","BAJAJCON","BAJAJELEC","BAJFINANCE",
    "BAJAJFINSV","BAJAJHLDNG","BALKRISIND","BALMLAWRIE","BALRAMCHIN","BANDHANBNK",
    "BANKBARODA","BANKINDIA","MAHABANK","BATAINDIA","BAYERCROP","BERGEPAINT",
    "BDL","BEL","BHARATFORG","BHEL","BPCL","BHARTIARTL","BIOCON","BIRLACORPN",
    "BLUEDART","BLUESTARCO","BBTC","BOSCHLTD","BRIGADE","BRITANNIA","BSOFT",
    "CAMS","CARERATING","CASTROLIND","CEATLTD","CENTRALBK","CDSL","CENTURYPLY",
    "CERA","CHAMBLFERT","CHENNPETRO","CHOLAHLDNG","CHOLAFIN","CIPLA","CUB",
    "COALINDIA","COCHINSHIP","COLPAL","CONCOR","COFORGE","COROMANDEL","CROMPTON",
    "CUMMINSIND","CYIENT","DCBBANK","DCMSHRIRAM","DLF","DABUR","DALBHARAT",
    "DEEPAKFERT","DEEPAKNTR","DELTACORP","DBL","DIVISLAB","DIXON","LALPATHLAB",
    "DRREDDY","EIDPARRY","EIHOTEL","EICHERMOT","ELGIEQUIP","EMAMILTD","ENDURANCE",
    "ENGINERSIN","EPIGRAL","EQUITASBNK","ERIS","ESCORTS","EXIDEIND","FDC",
    "FEDERALBNK","FINEORG","FINCABLES","FINPIPE","FSL","FORTIS","FRETAIL",
    "GAIL","GEPIL","GHCL","GMRINFRA","GALAXYSURF","GDL","GICRE","GILLETTE",
    "GLENMARK","GODFRYPHLP","GODREJAGRO","GODREJCP","GODREJIND","GODREJPROP",
    "GRANULES","GRAPHITE","GRASIM","GREAVESCOT","GRINDWELL","GUJALKALI",
    "GUJFLUORO","GUJGASLTD","GMDCLTD","GNFC","GPPL","GSFC","GSPL","GULFOILLUB",
    "HEG","HCLTECH","HDFCAMC","HDFCBANK","HDFCLIFE","HATSUN","HAVELLS",
    "HEIDELBERG","HERITGFOOD","HEROMOTOCO","HFCL","HIMATSEIDE","HINDALCO","HAL",
    "HINDCOPPER","HINDPETRO","HINDUNILVR","HINDZINC","HONAUT","HUDCO",
    "ICICIBANK","ICICIGI","ICICIPRULI","ISEC","ICRA","IDFCFIRSTB","IDFC",
    "IFBIND","IRB","IRCON","ITC","ITDCEM","ITI","INDIACEM","INDIANB","IEX",
    "INDHOTEL","IOC","IOB","INDOCO","IGL","INDUSINDBK","NAUKRI","INFY",
    "INOXLEISUR","INOXWIND","INTELLECT","INDIGO","IPCALAB","IREDA","IRFC",
    "JBCHEPHARM","JKCEMENT","JKLAKSHMI","JKPAPER","JKTYRE","JMFINANCIL",
    "JSWENERGY","JSWSTEEL","JAICORPLTD","JPASSOCIAT","J&KBANK","JAMNAAUTO",
    "JINDALSAW","JSL","JINDALSTEL","JUBLFOOD","JUBILANT","JUSTDIAL","JYOTHYLAB",
    "KPRMILL","KEI","KIOCL","KNRCON","KRBL","KAJARIACER","KALPATPOWR",
    "KANSAINER","KTKBANK","KARURVYSYA","KSCL","KEC","KIRLOSENG","KOLTEPATIL",
    "KOTAKBANK","L&TFH","LTTS","LICHSGFIN","LAXMIMACH","LTI","LT","LAURUSLABS",
    "LEMONTREE","LINDEINDIA","LUPIN","LUXIND","MASFIN","MMTC","MOIL","MRF",
    "MGL","M&MFIN","M&M","MAHINDCIE","MHRIL","MANAPPURAM","MRPL","MARICO",
    "MARUTI","MFSL","MAXHEALTH","MPHASIS","MUTHOOTFIN","NATCOPHARM","NBCC",
    "NCC","NESCO","NHPC","NLCINDIA","NMDC","NTPC","NH","NATIONALUM","NFL",
    "NAVINFLUOR","NETWORK18","NILKAMAL","NUVAMA","NUVOCO","OBEROIRLTY","ONGC",
    "OIL","OFSS","ORIENTCEM","ORIENTELEC","PCBL","PIIND","PNBHOUSING","PNCINFRA",
    "PTC","PVR","PAGEIND","PERSISTENT","PETRONET","PFIZER","PHILIPCARB",
    "PHOENIXLTD","PIDILITIND","PEL","PFC","POWERGRID","PRAJIND","PRESTIGE",
    "PRSMJOHNSN","PGHL","PGHH","PNB","RBLBANK","RECLTD","RITES","RADICO",
    "RAIN","RAJESHEXPO","RALLIS","RKFORGE","RCF","RAYMOND","REDINGTON","RELAXO",
    "RELIANCE","RNAM","REPCOHOME","RUPA","SBICARD","SBILIFE","SJVN","SKFINDIA",
    "SRF","SANOFI","SCHAEFFLER","SIS","SHARDACROP","SFL","SHILPAMED","SCI",
    "SHOPERSTOP","SHREECEM","SHRIRAMFIN","SIEMENS","SOBHA","SOLARINDS",
    "SONATSOFTW","SOUTHBANK","STARCEMENT","SBIN","SAIL","SUDARSCHEM","SPARC",
    "SUNPHARMA","SUNTV","SUNDARMFIN","SUNDRMFAST","SUNTECK","SUPRAJIT",
    "SUPREMEIND","SUVEN","SUZLON","SYMPHONY","SYNGENE","TTKPRESTIG","TVTODAY",
    "TV18BRDCST","TVSMOTOR","TNPL","TATACHEM","TCS","TATAELXSI","TATAINVEST",
    "TATAMOTORS","TATAPOWER","TATASTEEL","TATATECH","TEAMLEASE","TECHM",
    "NIACL","RAMCOCEM","THERMAX","THOMASCOOK","THYROCARE","TIMKEN","TITAN",
    "TORNTPHARM","TORNTPOWER","TRENT","TRIDENT","TIINDIA","UCOBANK","UFLEX",
    "UPL","UJJIVANSFB","ULTRACEMCO","UNIONBANK","UBL","MCDOWELL-N","VGUARD",
    "VMART","VIPIND","VSTIND","VAKRANGEE","VBL","VEDL","VENKEYS","VINATIORGA",
    "IDEA","VOLTAS","WABCOINDIA","WELCORP","WELSPUNIND","WHIRLPOOL","WIPRO",
    "YESBANK","ZEEL","ZENSARTECH","ZYDUSWELL","ECLERX","KAYNES","NETWEB",
    "KPITTECH","LTIM","TRANSRAIL","GMRAIRPORT","DATAPATTNS","ASTRAMICRO",
    "ZENTEC","AVANTEL","PARAS","RVNL","IRCTC","ZOMATO","NYKAA","PAYTM",
    "DELHIVERY","POLICYBZR","CAMPUS","LATENTVIEW","HAPPYMNDS","CLEAN",
    "POWERMECH","GPIL","WELSPUNLIV","SIGNATURE","SUVENPHAR","CSBBANK",
    "CRAFTSMAN","CHALET","SENCO","SBFC","MANKIND","MAZDOCK","COCHINSHIP",
    "ELCID","GESHIP","GAEL","GOCOLORS","PGEL","SYRMA","RPTECH","TATVA",
]

def get_watchlist() -> list[str]:
    """Fetch current Nifty 500 constituents from NSE. Falls back to snapshot on error."""
    try:
        ctx = ssl.create_default_context(cafile=certifi.where())
        req = urllib.request.Request(_NSE_URL, headers=_NSE_HEADERS)
        with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
            content = resp.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))
        # NSE CSV has a 'Symbol' column
        col = next((c for c in df.columns if "symbol" in c.lower()), None)
        if col is None:
            raise ValueError(f"No Symbol column found; columns: {list(df.columns)}")
        tickers = [f"{s.strip()}.NS" for s in df[col].dropna().tolist()]
        log.info(f"Watchlist: {len(tickers)} stocks fetched live from NSE")
        return tickers
    except Exception as e:
        log.warning(f"Could not fetch live Nifty 500 list ({e}); using built-in snapshot ({len(_NIFTY500_FALLBACK)} stocks)")
        return [f"{s}.NS" for s in _NIFTY500_FALLBACK]

BENCHMARK = "^NSEI"   # Nifty 50

SIGNAL_WEIGHTS = {
    "trend":        0.10,   # downweighted — confirmed trend means already in motion
    "momentum":     0.30,   # MACD early crossover / RSI recovery is the key trigger
    "volume":       0.10,   # quiet accumulation preferred; high volume = already started
    "breakout":     0.35,   # base formation near 52W high is the primary criterion
    "rel_strength": 0.15,
}
NEWS_WEIGHT = 0.10           # Applied post-hoc to top 10 ready candidates
WATCHLIST_NEWS_WEIGHT = 0.10 # Applied post-hoc to top watchlist candidates

# ── Risk / target policy ─────────────────────────────────────────────────────
# The stop is sized in ATR units, not percent. A flat 5% cap used to put the
# median stop ~1 ATR from entry — inside the stock's own daily range — so routine
# chop stopped picks out before the thesis had room to play.
#
# 1.5 ATR comes from an MAE study over picks with a full 10-day horizon: every
# eventual winner's worst drawdown was <= 1.46 ATR (p90 1.09), while the MEDIAN
# non-winner fell 1.98 ATR. So 1.5 keeps the winners whole and exits the losers
# sooner than 2.0 did. MAX_RISK_PCT only bounds very volatile names.
STOP_ATR_MULT  = 1.5    # stop this many ATRs below entry
MAX_RISK_PCT   = 9.0    # hard ceiling on risk per pick
MIN_TARGET_PCT = 12.0   # floor on the long target; also the screener's gate

# Two-tier targets. T1 is an early-confirmation level, not an exit signal: at 1R
# it sits near the median 10-day MFE (1.40 ATR), so roughly half of picks tag it
# and you get a read on the thesis long before T2 resolves.
TARGET_SHORT_R = 1.0    # short-term target, in multiples of risk
TARGET_LONG_R  = 2.0    # long-term target, in multiples of risk

# Weights for the early / leading-indicator watchlist tier
EARLY_SIGNAL_WEIGHTS = {
    "higher_lows":      0.25,  # ascending support toward 52W high — strongest breakout predictor
    "macd_crossover":   0.20,
    "rsi_recovery":     0.20,
    "obv_accumulation": 0.15,
    "bb_squeeze":       0.10,
    "rel_strength":     0.10,
}

# ── News sentiment ────────────────────────────────────────────────────────────

_POS_WORDS = {"beat","beats","record","growth","strong","gain","gains","profit","surge",
              "jump","rise","rises","upgrade","buy","expansion","wins","award","order",
              "orders","contract","acquisition","bullish","breakout","outperform"}
_NEG_WORDS = {"miss","misses","loss","losses","decline","fall","falls","cut","warning",
              "warn","debt","probe","fraud","delay","resign","downgrade","bearish",
              "selloff","recall","penalty","fine","default","restructure"}

def fetch_news(ticker_sym: str) -> tuple[list[dict], int]:
    """
    Fetches news from the last 5 days for ticker_sym.
    Returns (articles, sentiment_score) where sentiment_score is -1, 0, or +1.
    Articles: list of {title, url, publisher, date}.
    """
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=5)
        raw = yf.Ticker(ticker_sym).news or []
        articles = []
        pos = neg = 0

        for item in raw:
            content = item.get("content", {})
            pub_str  = content.get("pubDate", "")
            try:
                pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                if pub_dt < cutoff:
                    continue
            except Exception:
                continue

            title     = content.get("title", "").strip()
            url       = (content.get("canonicalUrl") or {}).get("url", "")
            publisher = (content.get("provider") or {}).get("displayName", "")
            pub_date  = pub_str[:10]

            if not title or not url:
                continue

            # Strip "Publisher -- " prefix pattern from summaries
            raw_summary = content.get("summary", "").strip()
            if raw_summary and " -- " in raw_summary[:60]:
                raw_summary = raw_summary.split(" -- ", 1)[1].strip()

            articles.append({
                "title":     title,
                "url":       url,
                "publisher": publisher,
                "date":      pub_date,
                "summary":   raw_summary or "",
            })

            words = set(title.lower().split())
            pos += len(words & _POS_WORDS)
            neg += len(words & _NEG_WORDS)

        if pos > neg and pos >= 2:
            sentiment = 1
        elif neg > pos and neg >= 2:
            sentiment = -1
        else:
            sentiment = 0

        return articles[:6], sentiment

    except Exception as e:
        log.debug(f"News fetch failed for {ticker_sym}: {e}")
        return [], 0

# ── Helpers ──────────────────────────────────────────────────────────────────

def _rsi(series: pd.Series, period=14) -> float:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if not rsi.empty else 50.0

def _sma(series: pd.Series, period: int) -> float:
    return float(series.rolling(period).mean().iloc[-1])

def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period=14) -> float:
    """Wilder's ATR — exponential smoothing with alpha = 1/period."""
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()
    return float(atr.iloc[-1]) if not atr.empty else 0.0

def _macd(close: pd.Series, fast=12, slow=26, signal=9):
    """Returns (macd_line, signal_line, histogram)."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line, macd_line - signal_line

def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff().fillna(0))
    return (volume * direction).cumsum()

def _bb_width(close: pd.Series, period=20) -> pd.Series:
    """Normalised Bollinger Band width = 4σ / SMA."""
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    return (4 * std) / sma.replace(0, np.nan)

def compute_trade_levels(close: pd.Series, high: pd.Series, low: pd.Series) -> dict:
    """
    ATR-calibrated entry, stop, and target levels.

    Stop Loss  — STOP_ATR_MULT × ATR below CMP, extended to the 5-day swing low when
                 that sits lower, then capped at MAX_RISK_PCT worst-case risk. Sizing
                 in ATR units keeps the stop outside the stock's daily noise band.
    Target     — 2× the actual risk (1:2 R:R), floored at MIN_TARGET_PCT. No 52W high
                 cap — that cap was squashing targets to <3% while stops stayed at 10%,
                 inverting R:R. 52W high is noted as resistance, not a hard ceiling.
    """
    price          = float(close.iloc[-1])
    day_high       = float(high.iloc[-1])
    atr            = _atr(high, low, close)

    entry_cmp      = price
    entry_breakout = round(day_high * 1.002, 2)

    # min() takes the lower of the two, so risk is never less than STOP_ATR_MULT ATRs.
    # This is what stops a stock sitting on its 5-day low from getting a near-zero stop.
    stop_atr  = price - STOP_ATR_MULT * atr
    swing_low = float(low.iloc[-5:].min())
    stop_raw  = min(stop_atr, swing_low)
    stop_loss = round(max(stop_raw, price * (1 - MAX_RISK_PCT / 100)), 2)

    risk = price - stop_loss

    # Long target: 2× risk (1:2 R:R), floored at MIN_TARGET_PCT upside.
    target = round(max(price + TARGET_LONG_R * risk, price * (1 + MIN_TARGET_PCT / 100)), 2)
    # Short target: 1R. Confirmation level only — it never resolves the pick.
    target_short = round(price + TARGET_SHORT_R * risk, 2)

    rr                = round((target - price) / risk, 2) if risk > 0 else 2.0
    stop_pct          = round((price - stop_loss) / price * 100, 1)
    target_pct        = round((target - price)       / price * 100, 1)
    target_short_pct  = round((target_short - price) / price * 100, 1)

    # Estimate trading days: distance / (0.5 ATR per day), clamped 5–30
    target_days_est = max(5, min(30, round((target - price) / atr * 2))) if atr > 0 else 10

    return {
        "entry_cmp":       round(entry_cmp, 2),
        "entry_breakout":  entry_breakout,
        "stop_loss":         stop_loss,
        "stop_pct":          stop_pct,
        "target_short":      target_short,
        "target_short_pct":  target_short_pct,
        "target":          target,
        "target_pct":      target_pct,
        "atr_14":          round(atr, 2),
        "rr_ratio":        rr,
        "target_days_est": target_days_est,
    }

# ── Signal functions (each returns -1, 0, or +1) ─────────────────────────────

def signal_trend(close: pd.Series) -> tuple[int, dict]:
    if len(close) < 200:
        return 0, {}
    price = close.iloc[-1]
    sma50  = _sma(close, 50)
    sma200 = _sma(close, 200)
    above50  = price > sma50
    above200 = price > sma200
    if above50 and above200:
        score = 1
    elif not above50 and not above200:
        score = -1
    else:
        score = 0
    return score, {"price": round(price, 2), "sma50": round(sma50, 2), "sma200": round(sma200, 2)}

def signal_momentum(close: pd.Series) -> tuple[int, dict]:
    rsi = _rsi(close)
    details: dict = {"rsi": round(rsi, 1)}

    # Check whether MACD histogram just flipped positive — early momentum shift
    macd_crossed = False
    if len(close) >= 35:
        _, _, hist = _macd(close)
        today_hist   = float(hist.iloc[-1])
        prev5        = hist.iloc[-6:-1]
        macd_crossed = bool(any(prev5 < 0) and today_hist > 0)
        details["macd_crossed"] = macd_crossed

    if rsi > 75:
        score = -1     # overbought regardless
    elif 40 <= rsi <= 65:
        score = 1      # classic sweet spot
    elif macd_crossed and rsi >= 28:
        score = 1      # early: histogram just flipped positive
    elif rsi < 28:
        score = -1     # deeply broken
    else:
        score = 0
    return score, details

def signal_volume(volume: pd.Series) -> tuple[int, dict]:
    if len(volume) < 21:
        return 0, {}
    today_vol  = volume.iloc[-1]
    avg20_vol  = float(volume.iloc[-21:-1].mean())
    ratio = today_vol / avg20_vol if avg20_vol > 0 else 1.0
    if ratio >= 1.5:
        score = 1
    elif ratio < 0.7:
        score = -1
    else:
        score = 0
    return score, {"vol_ratio": round(ratio, 2), "avg_vol_20d": int(avg20_vol)}

def signal_breakout(close: pd.Series, high: pd.Series) -> tuple[int, dict]:
    if len(close) < 52:
        return 0, {}
    price    = float(close.iloc[-1])
    high_52w = float(high.iloc[-252:].max()) if len(high) >= 252 else float(high.max())
    pct_from_high = (price - high_52w) / high_52w * 100

    # Detect tight consolidation base 3-20% below 52W high — pre-breakout setup
    forming_base = False
    if -20 <= pct_from_high < -3 and len(close) >= 15:
        rng_pct = (float(close.iloc[-15:].max()) - float(close.iloc[-15:].min())) / price * 100
        forming_base = rng_pct < 8.0

    if pct_from_high >= -5:
        score = 1      # already in breakout zone
    elif forming_base:
        score = 1      # tight base forming before breakout
    elif pct_from_high < -20:
        score = -1     # deep in the hole
    else:
        score = 0
    return score, {
        "price":         round(price, 2),
        "52w_high":      round(high_52w, 2),
        "pct_from_high": round(pct_from_high, 1),
        "forming_base":  forming_base,
    }

def signal_rel_strength(ticker_close: pd.Series, bench_close: pd.Series) -> tuple[int, dict]:
    if len(ticker_close) < 22 or len(bench_close) < 22:
        return 0, {}
    stock_ret = (ticker_close.iloc[-1] / ticker_close.iloc[-22] - 1) * 100
    bench_ret = (bench_close.iloc[-1] / bench_close.iloc[-22] - 1) * 100
    diff = stock_ret - bench_ret
    if diff > 3:
        score = 1
    elif diff < -3:
        score = -1
    else:
        score = 0
    return score, {"stock_1m_ret": round(stock_ret, 1), "nifty_1m_ret": round(bench_ret, 1), "rel_strength": round(diff, 1)}

# ── Early / leading-indicator signals ────────────────────────────────────────

def signal_macd_crossover(close: pd.Series) -> tuple[int, dict]:
    """MACD histogram crossed from negative to positive in last 5 bars = early momentum shift."""
    if len(close) < 35:
        return 0, {}
    _, _, hist = _macd(close)
    today        = float(hist.iloc[-1])
    prev5        = hist.iloc[-6:-1]
    just_crossed = bool(any(prev5 < 0) and today > 0)
    sustained    = bool(today > 0 and float(hist.iloc[-2]) > 0 and float(hist.iloc[-3]) > 0)
    hist_pct     = today / float(close.iloc[-1]) * 100   # normalise by price

    if just_crossed or sustained:
        score = 1
    elif hist_pct < -0.3:
        score = -1
    else:
        score = 0
    return score, {"hist_pct": round(hist_pct, 3), "just_crossed": just_crossed}


def signal_rsi_recovery(close: pd.Series) -> tuple[int, dict]:
    """RSI was below 45 in the last 10 bars, now above 40 and rising = catching the turn."""
    if len(close) < 30:
        return 0, {}
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rsi_s = (100 - 100 / (1 + gain / loss.replace(0, np.nan))).dropna()
    if len(rsi_s) < 15:
        return 0, {}
    current  = float(rsi_s.iloc[-1])
    low_10d  = float(rsi_s.iloc[-11:-1].min())
    slope_3d = float(rsi_s.iloc[-1]) - float(rsi_s.iloc[-4])

    if low_10d < 45 and current > 40 and slope_3d > 0:
        score = 1    # recovering from weakness — catching the turn
    elif current > 60:
        score = -1   # already running — not an early entry
    elif current < 28:
        score = -1   # deeply broken
    else:
        score = 0
    return score, {"rsi": round(current, 1), "rsi_10d_low": round(low_10d, 1)}


def signal_obv_accumulation(close: pd.Series, volume: pd.Series) -> tuple[int, dict]:
    """OBV trending up while price is flat or down = smart-money accumulation before price moves."""
    if len(close) < 25:
        return 0, {}
    obv       = _obv(close, volume)
    price_chg = (float(close.iloc[-1]) / float(close.iloc[-20]) - 1) * 100
    base_obv  = float(obv.iloc[-20])
    if base_obv == 0:
        return 0, {}
    obv_chg = (float(obv.iloc[-1]) - base_obv) / abs(base_obv) * 100

    if obv_chg > 5 and price_chg < 3:
        score = 1    # OBV leading price up
    elif obv_chg > 15:
        score = 1    # strong OBV momentum regardless
    elif obv_chg < -10:
        score = -1   # distribution
    else:
        score = 0
    return score, {"obv_chg_pct": round(obv_chg, 1), "price_chg_pct": round(price_chg, 1)}


def signal_bb_squeeze(close: pd.Series) -> tuple[int, dict]:
    """Bollinger Band width in bottom 25th percentile of last 60 days = volatility coiling before a move."""
    if len(close) < 65:
        return 0, {}
    bw       = _bb_width(close)
    current  = float(bw.iloc[-1])
    hist_60  = bw.iloc[-60:]
    pct_rank = float((hist_60 < current).mean()) * 100   # 0 = tightest, 100 = widest

    if pct_rank <= 25:
        score = 1    # coiled spring
    elif pct_rank >= 75:
        score = -1   # already expanded
    else:
        score = 0
    return score, {"bb_pct_rank": round(pct_rank, 0)}


def signal_higher_lows(close: pd.Series, high: pd.Series) -> tuple[int, dict]:
    """
    Ascending support lines (higher lows over 20 days) while approaching 52W high.
    This is the clearest price action confirmation that a stock is genuinely coiling
    for a breakout — each pullback holds at a higher level, compressing toward resistance.
    Only evaluated for stocks within 15% of their 52W high.
    """
    if len(close) < 20:
        return 0, {}

    price    = float(close.iloc[-1])
    high_52w = float(high.iloc[-252:].max()) if len(high) >= 252 else float(high.max())
    pct_from_high = (price - high_52w) / high_52w * 100

    if pct_from_high < -15:   # outside the breakout setup zone
        return 0, {}

    # Split last 20 bars into three segments; compare their minima
    recent   = close.iloc[-20:]
    seg1_low = float(recent.iloc[:7].min())    # oldest 7 bars
    seg2_low = float(recent.iloc[7:14].min())  # middle 7 bars
    seg3_low = float(recent.iloc[14:].min())   # newest 6 bars

    higher_lows = seg2_low > seg1_low and seg3_low > seg2_low

    range_20d  = (float(recent.max()) - float(recent.min())) / price * 100
    tight_base = range_20d < 8.0   # volatility contracting near resistance

    if higher_lows and tight_base:
        score = 1   # ascending support + volatility squeeze = breakout imminent
    elif higher_lows and pct_from_high > -8:
        score = 1   # higher lows within 8% of 52W high = strong setup
    elif not higher_lows and pct_from_high < -10:
        score = -1  # lower lows and still far from high = not setting up
    else:
        score = 0

    return score, {
        "higher_lows":   higher_lows,
        "tight_base":    tight_base,
        "range_20d_pct": round(range_20d, 1),
    }


# ── Liquidity filter ──────────────────────────────────────────────────────────

def passes_liquidity(volume: pd.Series, close: pd.Series, min_value_cr=5.0) -> bool:
    """Reject stocks with less than ₹5 Cr average daily traded value."""
    avg_vol   = float(volume.iloc[-21:].mean())
    avg_close = float(close.iloc[-21:].mean())
    adtv_cr   = (avg_vol * avg_close) / 1e7   # ₹ crore
    return adtv_cr >= min_value_cr

# ── Dynamic target floor ─────────────────────────────────────────────────────

def _fundamental_upside_pct(fundamentals: dict) -> float:
    """
    Returns a minimum target floor in percent (12–18) driven by fundamental quality.
    Base is MIN_TARGET_PCT (the 1:2 R:R minimum against a 2-ATR stop); strong metrics
    push higher, because a wider stop needs more upside to stay worth taking.
    """
    pct = MIN_TARGET_PCT

    earn_g = fundamentals.get("earnings_growth")
    rev_g  = fundamentals.get("rev_growth")
    roe    = fundamentals.get("roe")
    margin = fundamentals.get("profit_margin")
    de     = fundamentals.get("debt_to_equity")

    if earn_g is not None:
        if earn_g > 30:   pct += 2.0
        elif earn_g > 15: pct += 1.0

    if rev_g is not None:
        if rev_g > 20:   pct += 1.0
        elif rev_g > 10: pct += 0.5

    if roe is not None:
        if roe > 20:   pct += 1.0
        elif roe > 15: pct += 0.5

    if margin is not None and margin > 20:
        pct += 0.5

    if de is not None:
        if de < 0.3:  pct += 0.5
        elif de > 2:  pct -= 1.0

    return min(pct, 18.0)


def _apply_target(r: dict, new_target: float) -> None:
    """Update target and all derived fields in-place."""
    price = r["price"]
    r["target"]     = round(new_target, 2)
    r["target_pct"] = round((new_target - price) / price * 100, 1)
    risk = price - r["stop_loss"]
    if risk > 0:
        r["rr_ratio"] = round((new_target - price) / risk, 2)
    atr = r.get("atr_14", 0)
    if atr > 0:
        r["target_days_est"] = max(5, min(45, round((new_target - price) / atr * 2)))


# ── Per-stock screening ───────────────────────────────────────────────────────

def screen_stock(ticker: str, bench_close: pd.Series) -> dict | None:
    try:
        info_obj = yf.Ticker(ticker)
        df = info_obj.history(period="1y", actions=False)
        if df.empty or len(df) < 50:
            log.warning(f"{ticker}: insufficient data")
            return None

        # Strip TZ-aware DatetimeIndex and coerce to float64.
        # yfinance returns a DatetimeTZDtype index; pd.concat/ewm in _atr() can
        # SIGBUS the process in the C extension when the index carries timezone info.
        df = df.reset_index(drop=True)
        for col in ("Close", "High", "Low", "Volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Close", "High", "Low", "Volume"])
        if len(df) < 50:
            return None

        close  = df["Close"]
        high   = df["High"]
        low    = df["Low"]
        volume = df["Volume"]

        if not passes_liquidity(volume, close):
            log.info(f"{ticker}: failed liquidity filter")
            return None

        # Run signals
        s_trend,  d_trend  = signal_trend(close)
        s_mom,    d_mom    = signal_momentum(close)
        s_vol,    d_vol    = signal_volume(volume)
        s_break,  d_break  = signal_breakout(close, high)
        s_rs,     d_rs     = signal_rel_strength(close, bench_close)

        composite = (
            s_trend  * SIGNAL_WEIGHTS["trend"]  +
            s_mom    * SIGNAL_WEIGHTS["momentum"] +
            s_vol    * SIGNAL_WEIGHTS["volume"]  +
            s_break  * SIGNAL_WEIGHTS["breakout"] +
            s_rs     * SIGNAL_WEIGHTS["rel_strength"]
        )

        info    = info_obj.info
        company = info.get("longName") or info.get("shortName") or ticker
        sector  = info.get("sector") or info.get("industry") or "N/A"
        price   = float(close.iloc[-1])

        def _pct(v):
            return round(v * 100, 1) if v is not None else None

        cap_raw = info.get("marketCap")
        fundamentals = {
            "market_cap_cr":    round(cap_raw / 1e7) if cap_raw else None,
            "pe":               round(info["trailingPE"], 1) if info.get("trailingPE") else None,
            "pe_fwd":           round(info["forwardPE"], 1)  if info.get("forwardPE")  else None,
            "eps":              round(info["trailingEps"], 2) if info.get("trailingEps") else None,
            "rev_growth":       _pct(info.get("revenueGrowth")),
            "earnings_growth":  _pct(info.get("earningsGrowth")),
            "profit_margin":    _pct(info.get("profitMargins")),
            "gross_margin":     _pct(info.get("grossMargins")),
            "roe":              _pct(info.get("returnOnEquity")),
            "debt_to_equity":   round(info["debtToEquity"], 2) if info.get("debtToEquity") else None,
        }

        fundamentals["summary"] = generate_fundamentals_summary(fundamentals)
        trade = compute_trade_levels(close, high, low)

        # Lift target floor based on fundamental quality (6–12%)
        floor_pct  = _fundamental_upside_pct(fundamentals)
        fund_floor = price * (1 + floor_pct / 100)
        if fund_floor > trade["target"]:
            risk = price - trade["stop_loss"]
            trade["target"]          = round(fund_floor, 2)
            trade["target_pct"]      = round(floor_pct, 1)
            trade["rr_ratio"]        = round((fund_floor - price) / risk, 2) if risk > 0 else 2.0
            atr = trade["atr_14"]
            trade["target_days_est"] = max(5, min(45, round((fund_floor - price) / atr * 2))) if atr > 0 else 10

        return {
            "fundamental_floor_pct": floor_pct,
            "ticker":       ticker.replace(".NS", ""),
            "company":      company,
            "sector":       sector,
            "price":        round(price, 2),
            "score":        round(composite, 4),
            "signals": {
                "trend":        {"score": s_trend,  **d_trend},
                "momentum":     {"score": s_mom,    **d_mom},
                "volume":       {"score": s_vol,    **d_vol},
                "breakout":     {"score": s_break,  **d_break},
                "rel_strength": {"score": s_rs,     **d_rs},
            },
            "fundamentals": fundamentals,
            **trade,
        }

    except Exception as e:
        log.error(f"{ticker}: error — {e}")
        return None

# ── Interpretation generators ────────────────────────────────────────────────

def generate_rationale(result: dict) -> str:
    s = result["signals"]
    parts = []

    trend = s["trend"]
    if trend["score"] == 1:
        parts.append(f"Trading above both 50DMA (₹{trend.get('sma50','?')}) and 200DMA (₹{trend.get('sma200','?')}), confirming uptrend.")
    elif trend["score"] == -1:
        parts.append("Below key moving averages — trend is weak.")

    mom = s["momentum"]
    if mom["score"] == 1:
        if mom.get("macd_crossed"):
            parts.append(f"RSI at {mom['rsi']} with MACD histogram just flipped positive — early momentum shift before confirmation.")
        else:
            parts.append(f"RSI at {mom['rsi']} — in the healthy momentum zone, not overbought.")
    elif mom["score"] == -1:
        parts.append(f"RSI at {mom['rsi']} — caution, momentum extended or broken.")

    vol = s["volume"]
    if vol["score"] == 1:
        parts.append(f"Volume {vol.get('vol_ratio','?')}x the 20-day average — strong conviction in today's move.")

    brk = s["breakout"]
    if brk["score"] == 1:
        if brk.get("forming_base"):
            parts.append(f"Forming a tight base {abs(brk.get('pct_from_high', 0)):.1f}% below 52W high ₹{brk.get('52w_high','?')} — setting up for breakout.")
        else:
            parts.append(f"Price ₹{brk.get('price','?')} is within 5% of 52-week high ₹{brk.get('52w_high','?')} — breakout territory.")

    rs = s["rel_strength"]
    if rs["score"] == 1:
        parts.append(f"Outperforming Nifty by {rs.get('rel_strength','?')}% over the last month.")
    elif rs["score"] == -1:
        parts.append(f"Underperforming Nifty by {abs(rs.get('rel_strength',0))}% — relative weakness.")

    news_score = result.get("news_sentiment", 0)
    if news_score == 1:
        parts.append("Positive news sentiment in the last 5 days.")
    elif news_score == -1:
        parts.append("Caution: negative news sentiment detected in the last 5 days.")

    return " ".join(parts) if parts else "Mixed signals — monitor closely."

def generate_fundamentals_summary(f: dict) -> str:
    if not f:
        return ""
    parts = []
    pe     = f.get("pe")
    rev_g  = f.get("rev_growth")
    earn_g = f.get("earnings_growth")
    margin = f.get("profit_margin")
    roe    = f.get("roe")
    de     = f.get("debt_to_equity")

    if pe:
        if pe < 15:   parts.append(f"Attractively valued at {pe}× P/E.")
        elif pe < 30: parts.append(f"Reasonable valuation at {pe}× P/E.")
        else:         parts.append(f"Premium P/E of {pe}× — market pricing in strong growth.")

    if earn_g is not None:
        if earn_g > 50:   parts.append(f"Exceptional earnings growth of +{earn_g}% YoY.")
        elif earn_g > 15: parts.append(f"Solid earnings growth of +{earn_g}% YoY.")
        elif earn_g < 0:  parts.append(f"Earnings contracting {earn_g}% YoY — watch closely.")
    elif rev_g is not None and rev_g > 10:
        parts.append(f"Revenue growing at +{rev_g}% YoY.")

    qual = []
    if roe and roe > 15:       qual.append(f"return on equity of {roe}% signals efficient capital use")
    if margin and margin > 15: qual.append(f"healthy {margin}% profit margin")
    if qual: parts.append(". ".join(qual).capitalize() + ".")
    if de and de > 2:          parts.append(f"Elevated debt/equity of {de} — monitor leverage.")
    elif de is not None and de < 0.3: parts.append("Clean balance sheet with minimal debt.")

    return " ".join(parts[:3]) if parts else "Limited fundamental data available."

# ── Combined single-stock screen (ready + early, one yfinance fetch) ─────────

def screen_stock_combined(ticker: str, bench_close: pd.Series, _retry: bool = True) -> tuple[dict | None, dict | None]:
    """
    Single yfinance fetch. Evaluates both the main (confirmatory) signals and the early
    (leading) signals. Returns (main_result, early_result); at most one will be non-None.
    Stocks that meet the main 'ready' criteria skip early evaluation.
    """
    try:
        info_obj = yf.Ticker(ticker)
        df = info_obj.history(period="1y", actions=False)
        if df.empty or len(df) < 50:
            return None, None

        df = df.reset_index(drop=True)
        for col in ("Close", "High", "Low", "Volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Close", "High", "Low", "Volume"])
        if len(df) < 50:
            return None, None

        close  = df["Close"]
        high   = df["High"]
        low    = df["Low"]
        volume = df["Volume"]

        if not passes_liquidity(volume, close):
            return None, None

        price    = float(close.iloc[-1])
        high_52w = float(high.iloc[-252:].max()) if len(high) >= 252 else float(high.max())
        pct_from_high = (price - high_52w) / high_52w * 100

        # ── Pre-breakout zone gate ───────────────────────────────────────────
        # Only consider stocks that are 1–25% below their 52W high.
        # ≥ 0  → already broken out and running; skip.
        # < -25 → too far from the setup zone; skip.
        if pct_from_high >= 0 or pct_from_high < -25:
            return None, None

        # ── Shared fundamentals & trade levels ──────────────────────────────
        info    = info_obj.info
        company = info.get("longName") or info.get("shortName") or ticker
        sector  = info.get("sector") or info.get("industry") or "N/A"

        def _pct(v):
            return round(v * 100, 1) if v is not None else None

        cap_raw = info.get("marketCap")
        fundamentals = {
            "market_cap_cr":   round(cap_raw / 1e7) if cap_raw else None,
            "pe":              round(info["trailingPE"], 1) if info.get("trailingPE") else None,
            "pe_fwd":          round(info["forwardPE"], 1)  if info.get("forwardPE")  else None,
            "eps":             round(info["trailingEps"], 2) if info.get("trailingEps") else None,
            "rev_growth":      _pct(info.get("revenueGrowth")),
            "earnings_growth": _pct(info.get("earningsGrowth")),
            "profit_margin":   _pct(info.get("profitMargins")),
            "gross_margin":    _pct(info.get("grossMargins")),
            "roe":             _pct(info.get("returnOnEquity")),
            "debt_to_equity":  round(info["debtToEquity"], 2) if info.get("debtToEquity") else None,
        }
        fundamentals["summary"] = generate_fundamentals_summary(fundamentals)

        trade     = compute_trade_levels(close, high, low)
        floor_pct = _fundamental_upside_pct(fundamentals)
        fund_floor = price * (1 + floor_pct / 100)
        if fund_floor > trade["target"]:
            risk = price - trade["stop_loss"]
            trade["target"]          = round(fund_floor, 2)
            trade["target_pct"]      = round(floor_pct, 1)
            trade["rr_ratio"]        = round((fund_floor - price) / risk, 2) if risk > 0 else 2.0
            atr = trade["atr_14"]
            trade["target_days_est"] = max(5, min(45, round((fund_floor - price) / atr * 2))) if atr > 0 else 10

        base = {
            "ticker":                ticker.replace(".NS", ""),
            "company":               company,
            "sector":                sector,
            "price":                 round(price, 2),
            "fundamentals":          fundamentals,
            "fundamental_floor_pct": floor_pct,
            **trade,
        }

        # ── Signals ─────────────────────────────────────────────────────────
        s_trend,  d_trend  = signal_trend(close)
        s_mom,    d_mom    = signal_momentum(close)
        s_vol,    d_vol    = signal_volume(volume)
        s_break,  d_break  = signal_breakout(close, high)
        s_rs,     d_rs     = signal_rel_strength(close, bench_close)

        # Not-yet-moving filters: discard stocks whose momentum is already elevated
        # or where a volume breakout has already started.
        rsi_val   = d_mom.get("rsi", 50) or 50
        vol_ratio = d_vol.get("vol_ratio", 1.0) or 1.0
        if rsi_val > 60 or vol_ratio > 1.5:
            # Still evaluate for the early/watchlist tier below
            pass
        else:
            main_score = (
                s_trend  * SIGNAL_WEIGHTS["trend"]        +
                s_mom    * SIGNAL_WEIGHTS["momentum"]     +
                s_vol    * SIGNAL_WEIGHTS["volume"]       +
                s_break  * SIGNAL_WEIGHTS["breakout"]     +
                s_rs     * SIGNAL_WEIGHTS["rel_strength"]
            )

            if main_score > 0 and trade.get("target_pct", 0) >= MIN_TARGET_PCT:
                return {
                    **base,
                    "score": round(main_score, 4),
                    "signals": {
                        "trend":        {"score": s_trend,  **d_trend},
                        "momentum":     {"score": s_mom,    **d_mom},
                        "volume":       {"score": s_vol,    **d_vol},
                        "breakout":     {"score": s_break,  **d_break},
                        "rel_strength": {"score": s_rs,     **d_rs},
                    },
                }, None

        # ── Early (leading) signals — zone already guaranteed by outer gate ──
        # Outer gate ensures -25% ≤ pct_from_high < 0, so all remaining stocks
        # are genuinely approaching their 52W high.

        s_hl,   d_hl   = signal_higher_lows(close, high)
        s_macd, d_macd = signal_macd_crossover(close)
        s_rsi,  d_rsi  = signal_rsi_recovery(close)
        s_obv,  d_obv  = signal_obv_accumulation(close, volume)
        s_bb,   d_bb   = signal_bb_squeeze(close)

        early_score = (
            s_hl   * EARLY_SIGNAL_WEIGHTS["higher_lows"]      +
            s_macd * EARLY_SIGNAL_WEIGHTS["macd_crossover"]   +
            s_rsi  * EARLY_SIGNAL_WEIGHTS["rsi_recovery"]     +
            s_obv  * EARLY_SIGNAL_WEIGHTS["obv_accumulation"] +
            s_bb   * EARLY_SIGNAL_WEIGHTS["bb_squeeze"]       +
            s_rs   * EARLY_SIGNAL_WEIGHTS["rel_strength"]
        )

        if early_score <= 0:
            return None, None

        triggers = []
        if s_hl == 1:
            label = "Higher lows · Tight base" if d_hl.get("tight_base") else "Higher lows"
            triggers.append(label)
        if d_macd.get("just_crossed"): triggers.append("MACD crossed")
        elif s_macd == 1:              triggers.append("MACD positive")
        if s_rsi == 1:                 triggers.append("RSI recovering")
        if s_obv == 1:                 triggers.append("OBV accumulation")
        if s_bb  == 1:                 triggers.append("BB squeeze")
        setup_summary = " · ".join(triggers) if triggers else "Early signals"

        rsi_now = d_rsi.get("rsi", 50.0)
        watch_parts = []
        if rsi_now < 40:
            watch_parts.append(f"RSI > 45 (now {rsi_now:.0f})")
        if pct_from_high < -8:
            watch_parts.append(f"Price within 5% of 52W high ₹{round(high_52w, 0):.0f}")
        if s_hl == 1 and d_hl.get("tight_base"):
            watch_parts.append("Volume breakout above base")
        if not watch_parts:
            watch_parts.append("Confirm with volume breakout")

        return None, {
            **base,
            "early_score":       round(early_score, 4),
            "pct_from_52w_high": round(pct_from_high, 1),
            "setup_summary":     setup_summary,
            "watch_for":         " · ".join(watch_parts),
            "signals": {
                "higher_lows":      {"score": s_hl,   **d_hl},
                "macd_crossover":   {"score": s_macd, **d_macd},
                "rsi_recovery":     {"score": s_rsi,  **d_rsi},
                "obv_accumulation": {"score": s_obv,  **d_obv},
                "bb_squeeze":       {"score": s_bb,   **d_bb},
                "rel_strength":     {"score": s_rs,   **d_rs},
            },
        }

    except Exception as e:
        msg = str(e)
        if ("Too Many Requests" in msg or "429" in msg) and _retry:
            log.warning(f"{ticker}: rate limited — waiting 15s before retry")
            time.sleep(15)
            return screen_stock_combined(ticker, bench_close, _retry=False)
        log.error(f"{ticker}: combined screen error — {e}")
        return None, None


# ── Single-stock deep analysis ────────────────────────────────────────────────

def analyse_stock(ticker_sym: str) -> dict:
    """
    Deep analysis for a single NSE ticker (without .NS suffix).
    No liquidity filter — user explicitly requested this stock.
    Returns comprehensive technicals, fundamentals, news, and trade levels.
    """
    ticker = ticker_sym.upper().strip()
    full_sym = ticker + ".NS"

    try:
        info_obj = yf.Ticker(full_sym)
        df = info_obj.history(period="1y", actions=False)
        if df.empty or len(df) < 20:
            return {"error": f"No data found for {ticker}. Check the NSE symbol."}

        df = df.reset_index(drop=True)
        for col in ("Close", "High", "Low", "Volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Close", "High", "Low", "Volume"])
        if len(df) < 20:
            return {"error": f"Insufficient price history for {ticker}."}

        close  = df["Close"]
        high   = df["High"]
        low    = df["Low"]
        volume = df["Volume"]
        price  = float(close.iloc[-1])

        # Benchmark for relative strength
        bench_df = yf.Ticker(BENCHMARK).history(period="3mo", actions=False)
        if not bench_df.empty:
            bench_df = bench_df.reset_index(drop=True)
            bench_df["Close"] = pd.to_numeric(bench_df["Close"], errors="coerce")
        bench_close = bench_df["Close"].dropna() if not bench_df.empty else pd.Series(dtype=float)

        # Technical signals
        s_trend,  d_trend  = signal_trend(close)
        s_mom,    d_mom    = signal_momentum(close)
        s_vol,    d_vol    = signal_volume(volume)
        s_break,  d_break  = signal_breakout(close, high)
        s_rs,     d_rs     = signal_rel_strength(close, bench_close)

        composite = (
            s_trend  * SIGNAL_WEIGHTS["trend"]     +
            s_mom    * SIGNAL_WEIGHTS["momentum"]  +
            s_vol    * SIGNAL_WEIGHTS["volume"]    +
            s_break  * SIGNAL_WEIGHTS["breakout"]  +
            s_rs     * SIGNAL_WEIGHTS["rel_strength"]
        )

        # Extended technicals
        atr     = _atr(high, low, close)
        atr_pct = round(atr / price * 100, 2) if price > 0 else None
        sma_20  = round(_sma(close, 20),  2) if len(close) >= 20  else None
        high_52w = round(float(high.max()), 2)
        low_52w  = round(float(low.min()),  2)

        # Fundamentals
        info    = info_obj.info
        company = info.get("longName") or info.get("shortName") or ticker
        sector  = info.get("sector") or info.get("industry") or "N/A"
        cap_raw = info.get("marketCap")

        def _pct(v):
            return round(v * 100, 1) if v is not None else None

        fundamentals = {
            "market_cap_cr":    round(cap_raw / 1e7) if cap_raw else None,
            "pe":               round(info["trailingPE"], 1)   if info.get("trailingPE")    else None,
            "pe_fwd":           round(info["forwardPE"], 1)    if info.get("forwardPE")     else None,
            "eps":              round(info["trailingEps"], 2)  if info.get("trailingEps")   else None,
            "price_to_book":    round(info["priceToBook"], 2)  if info.get("priceToBook")   else None,
            "rev_growth":       _pct(info.get("revenueGrowth")),
            "earnings_growth":  _pct(info.get("earningsGrowth")),
            "profit_margin":    _pct(info.get("profitMargins")),
            "gross_margin":     _pct(info.get("grossMargins")),
            "operating_margin": _pct(info.get("operatingMargins")),
            "roe":              _pct(info.get("returnOnEquity")),
            "roa":              _pct(info.get("returnOnAssets")),
            "debt_to_equity":   round(info["debtToEquity"], 2) if info.get("debtToEquity") else None,
            "current_ratio":    round(info["currentRatio"], 2) if info.get("currentRatio") else None,
            "dividend_yield":   _pct(info.get("dividendYield")),
        }
        fundamentals["summary"] = generate_fundamentals_summary(fundamentals)

        # Trade levels with fundamental floor
        trade     = compute_trade_levels(close, high, low)
        floor_pct = _fundamental_upside_pct(fundamentals)
        fund_floor = price * (1 + floor_pct / 100)
        if fund_floor > trade["target"]:
            risk = price - trade["stop_loss"]
            trade["target"]          = round(fund_floor, 2)
            trade["target_pct"]      = round(floor_pct, 1)
            trade["rr_ratio"]        = round((fund_floor - price) / risk, 2) if risk > 0 else 2.0
            atr_v = trade["atr_14"]
            trade["target_days_est"] = max(5, min(45, round((fund_floor - price) / atr_v * 2))) if atr_v > 0 else 10

        # News
        articles, sentiment = fetch_news(full_sym)

        result = {
            "ticker":         ticker,
            "company":        company,
            "sector":         sector,
            "price":          round(price, 2),
            "score":          round(composite, 4),
            "news_sentiment": sentiment,
            "signals": {
                "trend":        {"score": s_trend, **d_trend},
                "momentum":     {"score": s_mom,   **d_mom},
                "volume":       {"score": s_vol,   **d_vol},
                "breakout":     {"score": s_break, **d_break},
                "rel_strength": {"score": s_rs,    **d_rs},
            },
            "fundamentals": fundamentals,
            "technicals": {
                "atr":               round(atr, 2),
                "atr_pct":          atr_pct,
                "sma_20":           sma_20,
                "sma_50":           round(d_trend["sma50"],  2) if d_trend.get("sma50")  else None,
                "sma_200":          round(d_trend["sma200"], 2) if d_trend.get("sma200") else None,
                "rsi":              d_mom.get("rsi"),
                "52w_high":         high_52w,
                "52w_low":          low_52w,
                "pct_from_52w_high": round((price - high_52w) / high_52w * 100, 1),
                "pct_from_52w_low":  round((price - low_52w)  / low_52w  * 100, 1),
                "vol_ratio":        d_vol.get("vol_ratio"),
            },
            "news":    articles,
            **trade,
        }
        result["rationale"] = generate_rationale(result)
        return result

    except (Exception, SystemError) as e:
        log.error(f"analyse_stock: {full_sym} — {e}")
        return {"error": f"Analysis failed for {ticker}: {e}"}


# ── Main entry point ──────────────────────────────────────────────────────────

def run_screening(log_cb=None, abort_event=None) -> list[dict]:
    def emit(msg):
        log.info(msg)
        if log_cb:
            log_cb(msg)

    emit("=== Starting Nifty 500 screening ===")

    emit("Loading Nifty 500 universe...")
    watchlist = get_watchlist()
    emit(f"Universe: {len(watchlist)} stocks")

    emit("Fetching Nifty 50 benchmark data...")
    bench_df = yf.Ticker(BENCHMARK).history(period="3mo", actions=False)
    bench_close = bench_df["Close"] if not bench_df.empty else pd.Series(dtype=float)
    if bench_df.empty:
        emit("Warning: could not fetch benchmark data, relative strength signals will be skipped.")

    total = len(watchlist)
    results = []
    for i, ticker in enumerate(watchlist):
        if abort_event and abort_event.is_set():
            emit("Aborted by user.")
            break

        emit(f"[{i+1}/{total}] {ticker}...")
        r = screen_stock(ticker, bench_close)
        if r:
            emit(f"  ✓ {ticker} — score {r['score']:+.3f} | RSI {r['signals']['momentum'].get('rsi','?')}")
            results.append(r)
        else:
            emit(f"  — {ticker} skipped (insufficient data or liquidity)")

    if not results:
        emit("No results from screening.")
        return []

    # Rank by score, then by upside — among equally-scored setups, take the one
    # with more room to run rather than whichever happened to sort first.
    results.sort(key=lambda x: (x["score"], x.get("target_pct", 0)), reverse=True)
    candidates = [
        r for r in results
        if r["score"] > 0 and r.get("target_pct", 0) >= MIN_TARGET_PCT
    ][:10]

    if not candidates:
        emit(f"No stock met minimum score + {MIN_TARGET_PCT:.0f}% target threshold today.")
        return []

    # Fetch news for top 10 candidates and fold into score
    emit(f"Fetching news for top {len(candidates)} candidates...")
    for r in candidates:
        sym = r["ticker"] + ".NS"
        articles, sentiment = fetch_news(sym)
        r["news"] = articles
        r["news_sentiment"] = sentiment
        r["score"] = round(r["score"] + sentiment * NEWS_WEIGHT, 4)
        # Positive news: add 1% to target; negative news: keep as-is (score already penalised)
        if sentiment == 1:
            _apply_target(r, r["target"] + r["price"] * 0.01)
            r["fundamental_floor_pct"] = round(r.get("fundamental_floor_pct", 6.0) + 1.0, 1)
        if articles:
            emit(f"  {sym}: {len(articles)} article(s), sentiment {'+' if sentiment > 0 else ''}{sentiment}")

    candidates.sort(key=lambda x: x["score"], reverse=True)

    # De-duplicate: prefer tickers not already picked in the last 3 days
    from database import get_recently_picked_tickers
    recent = get_recently_picked_tickers(days=3)
    if recent:
        emit(f"Excluding {len(recent)} recently picked ticker(s): {', '.join(sorted(recent))}")
    fresh = [r for r in candidates if r["ticker"] not in recent]
    stale = [r for r in candidates if r["ticker"] in recent]
    top3  = (fresh + stale)[:3]

    run_at = datetime.now().isoformat()
    for rank, pick in enumerate(top3, start=1):
        pick["rank"]          = rank
        pick["rationale"]     = generate_rationale(pick)
        pick["screened_count"] = len(results)
        pick["run_at"]        = run_at

    emit("=== Top 3 Picks ===")
    for p in top3:
        emit(f"  #{p['rank']} {p['ticker']} | Score {p['score']:+.4f} | {p['sector']}")

    return top3


def run_screening_combined(log_cb=None, abort_event=None) -> tuple[list[dict], list[dict]]:
    """
    Single-pass screening over Nifty 500. One yfinance fetch per stock.
    Returns (ready_picks [top 3], watchlist_picks [top 5]).
    ready_picks  — confirmatory signals met; trade now.
    watchlist_picks — leading indicators firing; wait for trigger before entering.
    """
    def emit(msg):
        log.info(msg)
        if log_cb:
            log_cb(msg)

    emit("=== Starting combined screening (Ready + Setting Up) ===")
    emit("Loading Nifty 500 universe...")
    watchlist = get_watchlist()
    emit(f"Universe: {len(watchlist)} stocks")

    emit("Fetching Nifty 50 benchmark data...")
    bench_df = yf.Ticker(BENCHMARK).history(period="3mo", actions=False)
    bench_close = bench_df["Close"] if not bench_df.empty else pd.Series(dtype=float)
    if bench_df.empty:
        emit("Warning: benchmark unavailable, relative strength skipped.")

    total = len(watchlist)
    ready_candidates = []
    early_candidates = []

    for i, ticker in enumerate(watchlist):
        if abort_event and abort_event.is_set():
            emit("Aborted by user.")
            break

        emit(f"[{i+1}/{total}] {ticker}...")
        main_r, early_r = screen_stock_combined(ticker, bench_close)
        time.sleep(0.5)

        if main_r:
            emit(f"  ✓ READY {ticker} — score {main_r['score']:+.3f} | RSI {main_r['signals']['momentum'].get('rsi','?')}")
            ready_candidates.append(main_r)
        elif early_r:
            emit(f"  ◈ SETUP {ticker} — early {early_r['early_score']:+.3f} | {early_r['setup_summary']}")
            early_candidates.append(early_r)
        else:
            emit(f"  — {ticker} skipped")

    if not ready_candidates and not early_candidates:
        emit("No results from screening.")
        return [], []

    # ── Ready picks (top 5) ───────────────────────────────────────────────────
    # Score first, upside as the tiebreak — prefer the setup with more room to run.
    ready_candidates.sort(key=lambda x: (x["score"], x.get("target_pct", 0)), reverse=True)
    candidates = [r for r in ready_candidates if r.get("target_pct", 0) >= MIN_TARGET_PCT][:15]

    if candidates:
        emit(f"Fetching news for top {len(candidates)} candidates...")
        for r in candidates:
            sym = r["ticker"] + ".NS"
            articles, sentiment = fetch_news(sym)
            r["news"] = articles
            r["news_sentiment"] = sentiment
            r["score"] = round(r["score"] + sentiment * NEWS_WEIGHT, 4)
            if sentiment == 1:
                _apply_target(r, r["target"] + r["price"] * 0.01)
                r["fundamental_floor_pct"] = round(r.get("fundamental_floor_pct", 10.0) + 1.0, 1)
            if articles:
                emit(f"  {sym}: {len(articles)} article(s), sentiment {'+' if sentiment > 0 else ''}{sentiment}")
        candidates.sort(key=lambda x: (x["score"], x.get("target_pct", 0)), reverse=True)

    from database import get_recently_picked_tickers
    recent = get_recently_picked_tickers(days=3)
    if recent:
        emit(f"Excluding {len(recent)} recently picked: {', '.join(sorted(recent))}")
    fresh = [r for r in candidates if r["ticker"] not in recent]
    stale = [r for r in candidates if r["ticker"] in recent]
    top5  = (fresh + stale)[:5]

    run_at = datetime.now().isoformat()
    for rank, pick in enumerate(top5, start=1):
        pick["rank"]           = rank
        pick["rationale"]      = generate_rationale(pick)
        pick["screened_count"] = total
        pick["run_at"]         = run_at

    # ── Watchlist / setting-up picks (saved to DB for history tab) ───────────
    ready_tickers = {p["ticker"] for p in top5}
    early_candidates.sort(key=lambda x: x["early_score"], reverse=True)
    top_setup = [r for r in early_candidates if r["ticker"] not in ready_tickers][:5]

    if top_setup:
        emit(f"Fetching news for {len(top_setup)} setting-up candidates...")
        for r in top_setup:
            sym = r["ticker"] + ".NS"
            articles, sentiment = fetch_news(sym)
            r["news"]           = articles
            r["news_sentiment"] = sentiment
            r["early_score"]    = round(r["early_score"] + sentiment * WATCHLIST_NEWS_WEIGHT, 4)
            if sentiment == 1:
                wf = r.get("watch_for", "")
                r["watch_for"] = (wf + " · Positive news catalyst").lstrip(" · ")
            elif sentiment == -1:
                r["setup_summary"] = r.get("setup_summary", "") + " ⚠ Neg news"
            if articles:
                emit(f"  {sym}: {len(articles)} article(s), sentiment {'+' if sentiment > 0 else ''}{sentiment}")
        top_setup.sort(key=lambda x: x["early_score"], reverse=True)

    for rank, pick in enumerate(top_setup, start=1):
        pick["rank"]           = rank
        pick["run_at"]         = run_at
        pick["screened_count"] = total

    emit(f"=== Results: {len(top5)} picks, {len(top_setup)} setting up ===")
    for p in top5:
        emit(f"  #{p['rank']} PICK   {p['ticker']} | Score {p['score']:+.4f} | {p['sector']}")
    for p in top_setup:
        emit(f"  #{p['rank']} SETUP  {p['ticker']} | Early {p['early_score']:+.4f} | {p['setup_summary']}")

    return top5, top_setup
