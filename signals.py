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
import statistics

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

def compute_trade_levels(close: pd.Series, high: pd.Series, low: pd.Series,
                         entry_price: float | None = None) -> dict:
    """
    ATR-calibrated entry, stop, and target levels.

    Stop Loss  — STOP_ATR_MULT × ATR below CMP, extended to the 5-day swing low when
                 that sits lower, then capped at MAX_RISK_PCT worst-case risk. Sizing
                 in ATR units keeps the stop outside the stock's daily noise band.
    Target     — 2× the actual risk (1:2 R:R), floored at MIN_TARGET_PCT. No 52W high
                 cap — that cap was squashing targets to <3% while stops stayed at 10%,
                 inverting R:R. 52W high is noted as resistance, not a hard ceiling.

    entry_price pins the levels to an entry already on record instead of deriving it
    from the last close. Backfilling historical picks needs this: a pick made
    mid-session was recorded at the price at that moment, not at the day's close, so
    re-deriving the entry would silently move it.
    """
    price          = float(entry_price) if entry_price is not None else float(close.iloc[-1])
    day_high       = float(high.iloc[-1])
    atr            = _atr(high, low, close)

    entry_cmp      = price
    entry_breakout = round(day_high * 1.002, 2)

    # Exactly STOP_ATR_MULT ATRs, then bounded by MAX_RISK_PCT. There used to be a
    # min() against the 5-day swing low, but that only ever WIDENED the stop, and the
    # MAE study says no eventual winner drew more than 1.46 ATR — so extra width buys
    # no additional winner retention, only larger losses. It also broke badly when the
    # entry is pinned above the pick-day close (backfill), where the swing low sits far
    # below and dragged stops out to 3 ATR.
    stop_loss = round(max(price - STOP_ATR_MULT * atr,
                          price * (1 - MAX_RISK_PCT / 100)), 2)

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
            "price_to_book":    round(info["priceToBook"], 2) if info.get("priceToBook") else None,
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

# ── Stage 2: valuation deep-dive ─────────────────────────────────────────────
# Runs only on the shortlist, not the whole universe — it needs statements, which
# cost an extra fetch per stock. Everything here is derived from data yfinance
# actually returns for NSE names (verified: priceToBook, bookValue, enterpriseValue,
# marketCap, totalCash, totalDebt, trailing/forwardPE, income_stmt, balance_sheet).
#
# Deliberately NOT modelled: order books, management guidance, credit ratings,
# promoter-stake changes, segment splits. None of those are in any feed we have,
# and holder percentages that yfinance does return are unreliable for Indian
# stocks, so they are not used to draw conclusions.
_CR = 1e7   # 1 crore
PEER_MIN_N = 8   # cohort size below which a peer median is too noisy to price off

def _stmt_row(df, *names):
    """First matching row of a yfinance statement, newest-first as a list."""
    if df is None or getattr(df, "empty", True):
        return []
    for n in names:
        if n in df.index:
            return [None if v != v else float(v) for v in df.loc[n].values]
    return []


def _cagr(series: list) -> float | None:
    """CAGR from the oldest to newest non-null value. Needs both ends positive."""
    vals = [(i, v) for i, v in enumerate(series) if v is not None]
    if len(vals) < 2:
        return None
    newest, oldest = vals[0][1], vals[-1][1]
    years = vals[-1][0] - vals[0][0]
    if years <= 0 or oldest <= 0 or newest <= 0:
        return None
    return round(((newest / oldest) ** (1 / years) - 1) * 100, 1)


def build_peer_stats(results: list[dict]) -> dict:
    """Median trailing PE and P/B per sector, from the screened universe.

    This is the cohort the relative-value comparison is made against — the same
    "20x vs a peer on 55x" argument, but computed rather than hand-picked.
    """
    by_sector: dict[str, dict[str, list]] = {}
    for r in results:
        sector = r.get("sector")
        f      = r.get("fundamentals") or {}
        if not sector or sector == "N/A":
            continue
        slot = by_sector.setdefault(sector, {"pe": [], "pb": []})
        pe = f.get("pe")
        pb = f.get("price_to_book")
        if isinstance(pe, (int, float)) and 0 < pe < 300:
            slot["pe"].append(pe)
        if isinstance(pb, (int, float)) and 0 < pb < 50:
            slot["pb"].append(pb)

    out = {}
    for sector, v in by_sector.items():
        if len(v["pe"]) >= 3:
            out[sector] = {
                "pe_median": round(statistics.median(v["pe"]), 1),
                "pb_median": round(statistics.median(v["pb"]), 2) if len(v["pb"]) >= 3 else None,
                "n":         len(v["pe"]),
            }
    return out


def compute_valuation_case(info: dict, price: float, sector: str,
                           income_stmt=None, balance_sheet=None,
                           peer_stats: dict | None = None) -> dict:
    """Valuation evidence for one shortlisted stock, in the style of a written thesis.

    Returns partial results rather than failing — Yahoo's coverage of Indian
    small-caps is patchy (SAVITA.NS 404s entirely), so every field is optional and
    the summary only asserts what actually resolved.
    """
    v: dict = {}

    mcap = info.get("marketCap")
    ev   = info.get("enterpriseValue")
    cash = info.get("totalCash")
    debt = info.get("totalDebt")
    shares = info.get("sharesOutstanding")

    v["mcap_cr"] = round(mcap / _CR) if mcap else None
    v["ev_cr"]   = round(ev / _CR) if ev else None

    # Book value — the "trading below net worth" argument
    v["book_value"]    = round(info["bookValue"], 2) if info.get("bookValue") else None
    v["price_to_book"] = round(info["priceToBook"], 2) if info.get("priceToBook") else None

    # Net cash — the "debt-free, cash-rich" argument
    if cash is not None and debt is not None:
        net_cash = cash - debt
        v["net_cash_cr"] = round(net_cash / _CR)
        v["net_cash_per_share"] = round(net_cash / shares, 1) if shares else None
        v["net_cash_pct_mcap"]  = round(net_cash / mcap * 100, 1) if mcap else None

    # Trailing vs forward PE — the market's own re-rating expectation
    pe_t = info.get("trailingPE")
    pe_f = info.get("forwardPE")
    v["pe_trailing"] = round(pe_t, 1) if isinstance(pe_t, (int, float)) else None
    v["pe_forward"]  = round(pe_f, 1) if isinstance(pe_f, (int, float)) else None
    v["ev_ebitda"]   = round(info["enterpriseToEbitda"], 1) if info.get("enterpriseToEbitda") else None

    # Multi-year trajectory
    rev = _stmt_row(income_stmt, "Total Revenue", "Operating Revenue")
    pat = _stmt_row(income_stmt, "Net Income", "Net Income Common Stockholders")
    v["rev_cagr_pct"] = _cagr(rev)
    v["pat_cagr_pct"] = _cagr(pat)
    if rev and pat and rev[0] and pat[0]:
        v["net_margin_now"] = round(pat[0] / rev[0] * 100, 1)
        older = [(r, p) for r, p in zip(rev[1:], pat[1:]) if r and p]
        if older:
            v["net_margin_then"] = round(older[-1][1] / older[-1][0] * 100, 1)
    eq = _stmt_row(balance_sheet, "Stockholders Equity")
    v["net_worth_cr"] = round(eq[0] / _CR) if eq and eq[0] else None

    # Relative value against the sector cohort
    peer = (peer_stats or {}).get(sector)
    if peer and v["pe_trailing"]:
        v["peer_pe_median"] = peer["pe_median"]
        v["peer_n"]         = peer["n"]
        v["pe_vs_peer_pct"] = round((v["pe_trailing"] / peer["pe_median"] - 1) * 100, 0)
        # Peer-parity value: what the stock would be worth on the cohort's multiple.
        # This is an OBSERVATION, not a target — it assumes the gap is unwarranted,
        # which is precisely the thing that needs an argument (a rating upgrade, a
        # promoter change, a capacity ramp) that none of our feeds can supply. Only
        # emitted off a cohort big enough for the median to mean something.
        if peer["n"] >= PEER_MIN_N:
            parity = price * peer["pe_median"] / v["pe_trailing"]
            v["peer_parity_price"] = round(parity, 2)
            v["peer_parity_gap"]   = round((parity / price - 1) * 100, 1)
            # A cheap multiple alongside shrinking profits is usually cheap for a
            # reason. Say so rather than implying the gap is free money.
            v["parity_caveat"] = (v.get("pat_cagr_pct") is not None
                                  and v["pat_cagr_pct"] < 0)

    v["summary"] = _valuation_summary(v, sector)
    return v


def _valuation_summary(v: dict, sector: str) -> str:
    parts = []

    pb = v.get("price_to_book")
    if pb is not None:
        bv = f" (book ₹{v['book_value']})" if v.get("book_value") else ""
        if pb < 1:
            parts.append(f"Trades at {pb}x book{bv} — below net worth.")
        else:
            parts.append(f"Trades at {pb}x book{bv}.")

    ncp = v.get("net_cash_pct_mcap")
    if ncp is not None:
        if ncp > 0:
            per = f", ₹{v['net_cash_per_share']}/share" if v.get("net_cash_per_share") else ""
            parts.append(f"Net cash {ncp}% of mcap{per} — debt-free.")
        else:
            parts.append(f"Net debt {abs(ncp)}% of mcap.")

    if v.get("pe_trailing") and v.get("peer_pe_median"):
        gap  = v["pe_vs_peer_pct"]
        word = "discount to" if gap < 0 else "premium to"
        parts.append(f"PE {v['pe_trailing']}x vs {sector} peer median "
                     f"{v['peer_pe_median']}x (n={v['peer_n']}) — {abs(gap):.0f}% {word} peers.")

    if v.get("pe_trailing") and v.get("pe_forward") and v["pe_forward"] < v["pe_trailing"]:
        parts.append(f"Forward PE {v['pe_forward']}x vs trailing {v['pe_trailing']}x — "
                     f"earnings expected to grow into the multiple.")

    if v.get("rev_cagr_pct") is not None and v.get("pat_cagr_pct") is not None:
        parts.append(f"Revenue CAGR {v['rev_cagr_pct']:+.1f}%, PAT CAGR {v['pat_cagr_pct']:+.1f}%.")
    if v.get("net_margin_now") is not None and v.get("net_margin_then") is not None:
        d = "widening" if v["net_margin_now"] > v["net_margin_then"] else "compressing"
        parts.append(f"Net margin {v['net_margin_then']}% → {v['net_margin_now']}% ({d}).")

    if v.get("peer_parity_gap") is not None:
        parts.append(f"At the cohort multiple it would be ₹{v['peer_parity_price']} "
                     f"({v['peer_parity_gap']:+.1f}%) — a relative-value observation, not a "
                     f"target, and not the ATR-based trade levels.")
        if v.get("parity_caveat"):
            parts.append("Profits are shrinking, so the discount may well be deserved.")

    return " ".join(parts) if parts else "Insufficient valuation data from the feed."


def enrich_valuation(shortlist: list[dict], universe: list[dict], emit=None) -> int:
    """Run the Stage-2 valuation deep-dive over the shortlist, in place.

    Statements are an extra fetch per stock, so this runs on the shortlist only —
    never the full universe. The peer cohort, by contrast, is built from everything
    screened, which is what makes the relative-value comparison meaningful.

    Failures are per-stock and non-fatal: a missing valuation must never cost a pick.
    """
    def say(m):
        log.info(m)
        if emit:
            emit(m)

    if not shortlist:
        return 0

    peer_stats = build_peer_stats(universe)
    say(f"Valuation deep-dive on {len(shortlist)} shortlisted "
        f"({len(peer_stats)} sector cohorts from {len(universe)} screened)...")

    done = 0
    for r in shortlist:
        sym = r["ticker"] + ".NS"
        try:
            tk   = yf.Ticker(sym)
            info = tk.info or {}
            if not info.get("marketCap"):
                r["valuation"] = {"summary": "No valuation data available for this symbol."}
                continue
            r["valuation"] = compute_valuation_case(
                info, r["price"], r.get("sector") or "",
                income_stmt=tk.income_stmt, balance_sheet=tk.balance_sheet,
                peer_stats=peer_stats,
            )
            done += 1
        except (Exception, SystemError) as e:
            log.warning(f"valuation: {sym} — {e}")
            r["valuation"] = {"summary": "Valuation lookup failed."}
    say(f"Valuation deep-dive complete: {done}/{len(shortlist)} resolved.")
    return done


_SCORE_LABELS = {
    "trend":        "trend",
    "momentum":     "momentum",
    "volume":       "volume",
    "breakout":     "breakout",
    "rel_strength": "rel strength",
}


def generate_score_basis(result: dict) -> str:
    """One line showing where the composite score came from.

    The score is a weighted sum of five ±1 signals, so the honest justification is
    the arithmetic itself: which signals fired, what each was worth, and what held
    it back. Reads off the stored signals, so it also works for historical picks.
    """
    signals = result.get("signals") or {}
    if not isinstance(signals, dict):
        return ""

    contrib = []
    for key, weight in SIGNAL_WEIGHTS.items():
        block = signals.get(key)
        raw   = block.get("score") if isinstance(block, dict) else None
        if raw is None:
            continue
        contrib.append((_SCORE_LABELS.get(key, key), raw * weight, raw))
    if not contrib:
        return ""

    contrib.sort(key=lambda c: -c[1])
    up   = [f"{n} {v:+.2f}" for n, v, r in contrib if r > 0]
    down = [f"{n} {v:+.2f}" for n, v, r in contrib if r < 0]
    flat = [n for n, v, r in contrib if r == 0]

    bits = []
    if up:   bits.append("lifted by " + ", ".join(up))
    if down: bits.append("dragged by " + ", ".join(down))
    if flat: bits.append("neutral on " + ", ".join(flat))

    total     = sum(v for _, v, _ in contrib)
    sentiment = result.get("news_sentiment")
    if sentiment:
        news_contrib = sentiment * NEWS_WEIGHT
        total += news_contrib
        bits.append(f"news sentiment {news_contrib:+.2f}")

    tail  = f" {len(up)} of {len(contrib)} signals positive."
    score = result.get("score")
    if not isinstance(score, (int, float)):
        return f"Score {total:+.2f} of a possible +1.00 — {'; '.join(bits)}.{tail}"

    # Signal weights have been rebalanced over time, so a pick scored under the old
    # weights will not reconcile with a breakdown computed at today's. Say so rather
    # than printing components that visibly fail to add up to the recorded score.
    if abs(total - score) < 0.005:
        head = f"Score {score:+.2f} of a possible +1.00"
        return f"{head} — {'; '.join(bits)}.{tail}"
    return (f"Score {score:+.2f} as recorded; these signals sum to {total:+.2f} at today's "
            f"weights — {'; '.join(bits)}.{tail}")


# ── Rationale: the written call ──────────────────────────────────────────────
# This used to emit one canned sentence per firing signal, which meant every pick
# read identically and none of them committed to anything. A call is only worth
# reading if it names the setup, says what triggered it *now*, grades its own
# confidence against the evidence, and states the price that proves it wrong.
# Each helper below writes one of those beats.
#
# Conviction here means falsifiability, not volume: the tier is earned by counting
# evidence, and every call carries the level that kills it. Everything is read
# defensively off `result` — the plain screen_stock() path attaches no news or
# valuation block, and analyse_stock() fills valuation in only after this runs.

# Lenders carry structurally high debt/equity; flagging it there is noise.
_FINANCIAL_SECTORS = {"Financial Services", "Financials", "Financial", "Banks"}


def _num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _join(items: list[str]) -> str:
    """Join clauses readably. Several of these carry their own commas and dashes,
    so comma-joining them runs the list together into one unparseable sentence."""
    if len(items) == 1:
        return items[0]
    sep = "; " if any("," in i for i in items) else ", "
    return sep.join(items[:-1]) + sep.strip() + " and " + items[-1]


def _conviction_evidence(result: dict) -> tuple[int, list[tuple[str, str]], list[str]]:
    """Weigh the evidence for and against the call.

    Returns a point total plus the arguments on each side, so the written call can
    put its own case and then argue against itself in the same breath. Supporting
    arguments are tagged so the caller can drop any the lead or trigger already made
    — repeating the same fact three times reads as padding, not as conviction.
    """
    s      = result.get("signals") or {}
    f      = result.get("fundamentals") or {}
    v      = result.get("valuation") or {}
    sector = result.get("sector") or ""

    tr  = s.get("trend")        or {}
    mom = s.get("momentum")     or {}
    vol = s.get("volume")       or {}
    brk = s.get("breakout")     or {}
    rs  = s.get("rel_strength") or {}

    pts = 0
    supports: list[tuple[str, str]] = []
    objections: list[str] = []

    # ── Structure: the breakout and the trend carry the setup ────────────────
    if brk.get("score") == 1:
        pts += 2
    elif brk.get("score") == -1:
        pts -= 2
        objections.append(f"price is {abs(brk.get('pct_from_high', 0)):.0f}% below the 52-week "
                          f"high, too deep for a base to have formed yet")

    if tr.get("score") == 1:
        pts += 1
    elif tr.get("score") == -1:
        pts -= 2
        objections.append("price is under both the 50- and 200-day averages, which makes this a "
                          "counter-trend bet")

    if mom.get("score") == 1:
        pts += 2 if mom.get("macd_crossed") else 1
    elif mom.get("score") == -1:
        rsi = mom.get("rsi")
        pts -= 2
        objections.append(f"RSI at {rsi} is outside the workable band" if _num(rsi)
                          else "momentum has broken down")

    diff = rs.get("rel_strength")
    if rs.get("score") == 1 and _num(diff):
        pts += 1
        if diff >= 10:
            pts += 1
            supports.append(("rel_strength",
                             f"{diff:+.1f}% relative strength over Nifty in a month — this is "
                             f"already a leader, not a candidate"))
        else:
            supports.append(("rel_strength", f"{diff:+.1f}% ahead of Nifty over the past month"))
    elif rs.get("score") == -1 and _num(diff):
        pts -= 1
        objections.append(f"it is lagging Nifty by {abs(diff):.1f}% over the month")

    # Volume only means something against the setup it appears in: heavy volume
    # confirms a breakout, but in a base it says the move has already been found.
    ratio = vol.get("vol_ratio")
    if _num(ratio):
        if brk.get("forming_base") and ratio < 1.0:
            pts += 1
            supports.append(("volume",
                             f"the base is tightening on {ratio}x volume — quiet accumulation, "
                             f"the version of this setup that has not been front-run"))
        elif ratio >= 1.5:
            pts += 1
            supports.append(("volume", f"{ratio}x the 20-day average volume behind the move"))
        elif ratio < 0.7:
            objections.append(f"volume at {ratio}x average is thin — nobody is voting on this yet")

    # ── Business: does the company back what the chart is doing? ─────────────
    eg = f.get("earnings_growth")
    if _num(eg):
        if eg >= 20:
            pts += 1
            supports.append(("earnings", f"earnings compounding {eg:+.1f}% YoY"))
        elif eg < 0:
            pts -= 1
            objections.append(f"earnings are contracting {eg:+.1f}% YoY — the chart is ahead of "
                              f"the business")

    roe = f.get("roe")
    if _num(roe) and roe >= 18:
        pts += 1
        supports.append(("roe", f"{roe:.1f}% return on equity"))

    pe, pe_fwd = f.get("pe"), f.get("pe_fwd")
    if _num(pe) and _num(pe_fwd) and pe_fwd < pe:
        pts += 1
        supports.append(("pe", f"forward PE of {pe_fwd}x against {pe}x trailing — the multiple "
                               f"de-rates as earnings land"))

    dte = f.get("debt_to_equity")
    if _num(dte) and dte > 150 and sector not in _FINANCIAL_SECTORS:
        pts -= 1
        objections.append(f"debt/equity of {dte:.0f} is heavy for a non-lender")

    gap = v.get("pe_vs_peer_pct")
    if _num(gap) and v.get("peer_n"):
        if gap <= -15 and not v.get("parity_caveat"):
            pts += 1
            supports.append(("peer", f"{abs(gap):.0f}% cheaper than the {sector} peer median on "
                                     f"PE (n={v['peer_n']})"))
        elif gap >= 40:
            objections.append(f"it is {gap:.0f}% more expensive than sector peers — the quality "
                              f"is already in the price")

    pat_cagr = v.get("pat_cagr_pct")
    if _num(pat_cagr) and pat_cagr < 0:
        pts -= 1
        objections.append(f"multi-year PAT CAGR is negative at {pat_cagr:+.1f}%")

    # ── News and payoff ──────────────────────────────────────────────────────
    news = result.get("news_sentiment")
    if news == 1:
        pts += 1
        articles = result.get("news") or []
        head = articles[0].get("title") if articles and isinstance(articles[0], dict) else None
        # Sentiment here is a keyword match over headlines, not a read of the news, so
        # the headline is quoted for the reader to judge rather than asserted as a catalyst.
        supports.append(("news", f'headlines reading positive in the last five days, led by '
                                 f'"{head}"' if head
                                 else "positive news flow in the last five days"))
    elif news == -1:
        pts -= 2
        objections.append("negative news flow in the last five days — the tape may know something "
                          "the chart has not priced")

    rr = result.get("rr_ratio")
    if _num(rr):
        if rr >= 2.5:
            pts += 1
            supports.append(("rr", f"{rr}:1 reward-to-risk"))
        elif rr < 1.5:
            pts -= 1
            objections.append(f"only {rr}:1 reward-to-risk — thin payoff for the money at risk")

    return pts, supports[:4], objections[:3]


# Calibrated against the 48 picks on record, which score 4–11 without the news and
# peer-valuation points that a live run also has. A screener that publishes only its
# best candidates will happily call everything high conviction — which is the same
# undifferentiated mush as having no tier at all — so the top band is set where only
# a live pick carrying news and valuation support can reach it.
def _conviction_tier(pts: int) -> str:
    if pts >= 11:
        return "High conviction"
    if pts >= 9:
        return "Constructive"
    if pts >= 7:
        return "Tactical — size it small"
    return "Marginal — the weakest call this screen will publish"


def _setup_line(result: dict, s: dict) -> tuple[str, str]:
    """The lead: what this setup *is*, stated once and without hedging.

    Returns the sentence and the beat it used, so the trigger does not re-tell it.
    """
    tr    = s.get("trend")    or {}
    mom   = s.get("momentum") or {}
    brk   = s.get("breakout") or {}
    price = result.get("price")

    high52, from_high = brk.get("52w_high"), brk.get("pct_from_high")

    if brk.get("score") == 1 and _num(from_high) and from_high >= -5:
        return (f"This is a breakout, not a bounce: ₹{price} sits {abs(from_high):.1f}% off the "
                f"52-week high of ₹{high52}, with a full year of overhead supply already absorbed "
                f"below it.", "breakout")
    if brk.get("forming_base") and _num(from_high):
        return (f"This is a coiled base: ₹{price} has held a range tighter than 8% while sitting "
                f"{abs(from_high):.1f}% under the 52-week high of ₹{high52}. Buying the quiet "
                f"stretch before the breakout is the whole premise of this screen.", "base")
    if mom.get("macd_crossed"):
        return (f"This is a momentum turn caught early: the MACD histogram flipped positive within "
                f"the last five sessions with RSI at {mom.get('rsi')} — before the move is obvious "
                f"on the chart.", "momentum")
    if tr.get("score") == 1:
        return (f"This is trend continuation: ₹{price} is holding above both the 50-day "
                f"(₹{tr.get('sma50')}) and the 200-day (₹{tr.get('sma200')}) average.", "trend")
    return (f"The chart is unremarkable at ₹{price} — this one is on the list for its numbers, "
            f"not its setup.", "none")


def _trigger_line(result: dict, s: dict, lead: str) -> tuple[str, str]:
    """Why today and not last month. The freshest thing in the data leads, skipping
    whatever the setup sentence has already said."""
    mom = s.get("momentum")     or {}
    vol = s.get("volume")       or {}
    brk = s.get("breakout")     or {}
    rs  = s.get("rel_strength") or {}

    if mom.get("macd_crossed") and lead != "momentum":
        return (f"The trigger is this week: MACD crossed up with RSI at {mom.get('rsi')}, the "
                f"earliest confirmation this model will act on.", "momentum")

    ratio, avg = vol.get("vol_ratio"), vol.get("avg_vol_20d")
    if _num(ratio) and ratio >= 2 and _num(avg):
        return (f"The trigger is today's tape: {ratio}x volume against a normal {avg:,.0f} shares "
                f"a day — that is real money arriving, not drift.", "volume")

    from_high = brk.get("pct_from_high")
    if brk.get("forming_base") and _num(from_high) and lead != "base":
        return (f"The trigger is the compression itself: the range has narrowed to within 8% while "
                f"holding {abs(from_high):.1f}% off the high, and bases that tight resolve rather "
                f"than persist.", "base")

    diff = rs.get("rel_strength")
    if _num(diff) and diff > 3:
        stock, bench = rs.get("stock_1m_ret"), rs.get("nifty_1m_ret")
        if _num(stock) and _num(bench):
            return (f"The trigger is divergence: {stock:+.1f}% over the month against Nifty's "
                    f"{bench:+.1f}% — money is rotating in while the index goes nowhere.",
                    "rel_strength")
    return "", "none"


def _invalidation_line(result: dict) -> str:
    """The falsifiable half of the call — the price that ends it."""
    stop, target = result.get("stop_loss"), result.get("target")
    if not _num(stop) or not _num(target):
        return ""

    stop_pct = result.get("stop_pct")
    atr      = result.get("atr_14")
    price    = result.get("price")

    width = ""
    if _num(atr) and atr > 0 and _num(price):
        width = f", {(price - stop) / atr:.1f} ATR"
    risk = f" ({stop_pct}% down{width})" if _num(stop_pct) else ""

    bits = [f"The call is wrong below ₹{stop}{risk} — a close under that breaks the structure the "
            f"whole thesis rests on, and the position goes with it."]

    t1, target_pct, days = result.get("target_short"), result.get("target_pct"), result.get("target_days_est")
    if _num(t1):
        horizon = f" over an estimated {days} sessions" if _num(days) else ""
        bits.append(f"₹{t1} is first confirmation, ₹{target}"
                    f"{f' ({target_pct}%)' if _num(target_pct) else ''} the objective{horizon}.")
    return " ".join(bits)


# A trigger sentence already makes the case its own supporting clause would repeat.
_TRIGGER_COVERS = {"volume": {"volume"}, "rel_strength": {"rel_strength"}}


def generate_rationale(result: dict) -> str:
    """The written call: setup, trigger, graded confidence, and what kills it."""
    s = result.get("signals")
    if not isinstance(s, dict) or not s:
        return "No signal data — no call."

    pts, supports, objections = _conviction_evidence(result)
    setup, lead   = _setup_line(result, s)
    trigger, kind = _trigger_line(result, s, lead)

    covered = _TRIGGER_COVERS.get(kind, set())
    backing = [text for tag, text in supports if tag not in covered]

    parts = [f"{_conviction_tier(pts)}. {setup}", trigger]

    if backing:
        parts.append("Backing it: " + _join(backing) + ".")
    if objections:
        parts.append("Against it: " + _join(objections) + ".")
    else:
        parts.append("Nothing in the screened data argues against it — any objection has to come "
                     "from outside what this model can see.")

    parts.append(_invalidation_line(result))
    return " ".join(p for p in parts if p)

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
            "price_to_book":   round(info["priceToBook"], 2) if info.get("priceToBook") else None,
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
            "score_basis":    "",   # filled in below, once signals are assembled
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
        result["rationale"]   = generate_rationale(result)
        result["score_basis"] = generate_score_basis(result)
        # No peer cohort here — that is built from a full screen, so the on-demand
        # analysis gets the absolute valuation block without the relative comparison.
        try:
            result["valuation"] = compute_valuation_case(
                info, price, sector,
                income_stmt=info_obj.income_stmt, balance_sheet=info_obj.balance_sheet,
            )
        except (Exception, SystemError) as e:
            log.warning(f"analyse_stock valuation: {full_sym} — {e}")
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

    enrich_valuation(candidates, results, emit)

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

    enrich_valuation(candidates, ready_candidates + early_candidates, emit)

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
