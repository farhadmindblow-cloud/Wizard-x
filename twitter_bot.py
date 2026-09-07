"""
twitter_bot.py — WIZRAD Scan Twitter (the "2nd bot").

WHAT THIS DOES
1. Listens in a dedicated Telegram SUBMIT group. Whenever someone pastes a
   tweet/x.com link there, the bot fetches the full post (text, author,
   likes, retweets, followers) via X's free public FxTwitter API — no
   login, no API key, no risk to your real X accounts — and posts a
   formatted card to your WIZRAD Scan Twitter channel.
2. Every ~8 minutes it re-checks every tracked tweet and posts an update
   the moment retweets cross the next multiple of 50, or the author's
   followers cross the next multiple of 100.
3. It runs the SAME kind of token-call detection as the main Telegram bot
   (including the recap/chart-update filter) on the tweet text, and if a
   fresh contract-address call is found, tracks it and posts 2x/5x/10x...
   X-alerts the same way the Telegram side does.

SETUP (env vars)
  TWITTER_BOT_TOKEN     - BotFather token for the 2nd bot
  TWITTER_SUBMIT_CHAT   - numeric chat id of the private submission group
  TWITTER_CHANNEL       - @handle or numeric id of the WIZRAD Scan Twitter channel
  DATA_DIR              - same data folder as the main bot (optional, defaults to ".")

RUN
  python twitter_bot.py
  (deploy as its own Railway service, or start it as a background task
   from the main bot the same way api_server.py is started — either works)
"""

import os
import re
import json
import time
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, MessageHandler, ContextTypes, filters

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("wizrad_twitter")

# ── Config ───────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("TWITTER_BOT_TOKEN", "")
SUBMIT_CHAT  = int(os.environ.get("TWITTER_SUBMIT_CHAT", "0") or 0)
TARGET_CHAN  = os.environ.get("TWITTER_CHANNEL", "")
DATA_DIR     = os.environ.get("DATA_DIR", ".")

RETWEET_STEP  = int(os.environ.get("RETWEET_STEP", "50"))    # feature 2: every 50 retweets
FOLLOWER_STEP = int(os.environ.get("FOLLOWER_STEP", "100"))  # feature 2: every 100 followers
POLL_SECONDS  = int(os.environ.get("TWITTER_POLL_SECONDS", "480"))  # ~8 min
MIN_ALERT_X   = 2  # matches the Telegram bot's own 2x alert threshold

def _dp(name):
    return os.path.join(DATA_DIR, name)

TRACKED_TWEETS_FILE = _dp("twitter_tracked_tweets.json")   # engagement tracking (feature 1+2)
TRACKED_CALLS_FILE  = _dp("twitter_tracked_calls.json")    # token-call tracking (feature 3)

def load_json(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

tracked_tweets = load_json(TRACKED_TWEETS_FILE, {})   # tweet_id -> {...}
tracked_calls  = load_json(TRACKED_CALLS_FILE, {})    # call_key -> {...}

# ── Tweet link detection ────────────────────────────────────────────────
TWEET_URL_RE = re.compile(
    r"https?://(?:www\.|mobile\.)?(?:twitter|x)\.com/([A-Za-z0-9_]{1,20})/status(?:es)?/(\d+)",
    re.IGNORECASE,
)

def extract_tweet_links(text):
    """Returns a list of (screen_name, tweet_id) found in a message."""
    if not text:
        return []
    return TWEET_URL_RE.findall(text)

# ── FxTwitter fetch (free, no login, no API key) ────────────────────────
async def fetch_tweet(tweet_id):
    """Fetch a tweet's full text + live engagement stats via the FxTwitter
    v2 public API. Returns a flat dict, or None if unavailable."""
    url = f"https://api.fxtwitter.com/2/status/{tweet_id}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status != 200:
                    logger.warning(f"FxTwitter status {resp.status} for {tweet_id}")
                    return None
                data = await resp.json()
    except Exception as e:
        logger.warning(f"FxTwitter fetch failed for {tweet_id}: {e}")
        return None

    status = data.get("status")
    if not status or status.get("type") == "tombstone":
        return None

    author = status.get("author") or {}
    verification = author.get("verification") or {}

    return {
        "id":         status.get("id", tweet_id),
        "url":        status.get("url", f"https://x.com/i/status/{tweet_id}"),
        "text":       status.get("text", ""),
        "likes":      int(status.get("likes", 0) or 0),
        "retweets":   int(status.get("reposts", 0) or 0),
        "replies":    int(status.get("replies", 0) or 0),
        "views":      status.get("views"),
        "screen_name": author.get("screen_name", "unknown"),
        "name":        author.get("name", ""),
        "followers":   int(author.get("followers", 0) or 0),
        "verified":    bool(verification.get("verified", False)),
        "avatar_url":  author.get("avatar_url"),
    }

# ── Recap/chart-update detector (mirrors the Telegram bot's filter) ─────
_RECAP_PHRASES = [
    "chart update", "price update", "update:", "reminder:",
    "look where it went", "look how far", "look at it now", "look at this now",
    "since our call", "since we called", "since the call",
    "from our call", "from our original call", "from the call",
    "we called this", "we called it", "called this one", "already called",
    "delivering a move", "delivering an impressive", "delivered a", "delivered an",
    "and look where", "and look how", "early positioning matters",
    "recap", "revisit", "throwback", "flashback",
    "more opportunities like this are on the way",
]

def _is_recap_post(text):
    if not text:
        return False
    tl = text.lower()
    return any(p in tl for p in _RECAP_PHRASES)

# ── Contract-address extraction (SOL / EVM) ──────────────────────────────
ETH_CA_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
SOL_CA_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")

def extract_ca(text):
    """Returns (chain, ca) or None."""
    if not text:
        return None
    m = ETH_CA_RE.search(text)
    if m:
        return ("EVM", m.group(0))
    for cand in SOL_CA_RE.findall(text):
        if 32 <= len(cand) <= 44 and not cand.startswith("0x"):
            return ("SOL", cand)
    return None

def is_call_message(text):
    if not text or len(text) < 10:
        return False
    tl = text.lower()
    kw = ["buy", "bought", "long", "entry", "target", "gem", "moon", "call",
          "ape", "snipe", "load", "bag", "enter", "watch", "launch", "listed",
          "presale", "mc ", "mc:", "mcap", "market cap", "fdv", "liquidity",
          "ca ", "ca:", "contract", "token", "address", "bullish", "early",
          "dexscreener", "dextools", "birdeye", "pump.fun", "solana", "sol ",
          "ethereum", "base", "bnb", "fire", "hot", "gem", "100x", "moonshot"]
    return any(k in tl for k in kw) or bool(ETH_CA_RE.search(text)) or bool(SOL_CA_RE.search(text))

# ── Dexscreener (free, no key) — same source the main bot uses ──────────
async def fetch_dex(ca):
    url = f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
    except Exception as e:
        logger.warning(f"Dexscreener fetch failed for {ca}: {e}")
        return None

    pairs = data.get("pairs") or []
    if not pairs:
        return None
    best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
    mc = float(best.get("fdv") or best.get("marketCap") or 0)
    return {
        "mc": mc,
        "symbol": (best.get("baseToken") or {}).get("symbol", "TOKEN"),
        "chain": (best.get("chainId") or "").upper(),
        "dex_url": best.get("url", ""),
    }

def fmt_mc(v):
    v = float(v or 0)
    if v <= 0: return "N/A"
    if v >= 1_000_000_000: return f"${v/1_000_000_000:.2f}B"
    if v >= 1_000_000:     return f"${v/1_000_000:.2f}M"
    if v >= 1_000:         return f"${v/1_000:.1f}K"
    return f"${v:.0f}"

# ── Feature 1: post a fresh tweet to the channel ─────────────────────────
def build_signal_card(tweet):
    quote = tweet["text"].strip()
    if len(quote) > 600:
        quote = quote[:600].rsplit(" ", 1)[0] + "…"
    verified_mark = " ✅" if tweet["verified"] else ""
    return (
        f'🔮 <b>WIZRAD X SIGNAL</b> — @{tweet["screen_name"]}{verified_mark}\n\n'
        f'<blockquote>{quote}</blockquote>\n\n'
        f'🔗 <a href="{tweet["url"]}">View on X</a>'
    )

async def handle_submission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != SUBMIT_CHAT:
        return
    text = update.message.text or update.message.caption or ""
    links = extract_tweet_links(text)
    if not links:
        return

    for screen_name, tweet_id in links:
        if tweet_id in tracked_tweets:
            await update.message.reply_text(f"⏭️ Already posted: tweet {tweet_id}")
            continue

        tweet = await fetch_tweet(tweet_id)
        if not tweet:
            await update.message.reply_text(f"⚠️ Couldn't fetch tweet {tweet_id} (deleted/private/rate-limited).")
            continue

        # Feature 1 — post the full signal card to the channel
        card = build_signal_card(tweet)
        try:
            sent = await context.bot.send_message(
                TARGET_CHAN, card, parse_mode=ParseMode.HTML, disable_web_page_preview=False)
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to post to channel: {e}")
            continue

        # Save for the engagement-milestone job (feature 2)
        tracked_tweets[tweet_id] = {
            "screen_name":     tweet["screen_name"],
            "url":             tweet["url"],
            "channel_msg_id":  sent.message_id,
            "last_retweet_step":  (tweet["retweets"] // RETWEET_STEP) * RETWEET_STEP,
            "last_follower_step": (tweet["followers"] // FOLLOWER_STEP) * FOLLOWER_STEP,
            "added_at": datetime.now(timezone.utc).isoformat(),
        }
        save_json(TRACKED_TWEETS_FILE, tracked_tweets)

        # Feature 3 — token-call detection (with the same recap guard)
        ca_result = extract_ca(tweet["text"])
        if ca_result and is_call_message(tweet["text"]) and not _is_recap_post(tweet["text"]):
            chain, ca = ca_result
            call_key = f'{tweet["screen_name"]}_{ca}'
            if call_key not in tracked_calls:
                dex = await fetch_dex(ca)
                entry_mc = dex["mc"] if dex else 0
                tracked_calls[call_key] = {
                    "screen_name": tweet["screen_name"],
                    "tweet_url":   tweet["url"],
                    "ca": ca,
                    "chain": chain,
                    "symbol": (dex or {}).get("symbol", "TOKEN"),
                    "entry_mc": entry_mc,
                    "last_milestone": 1,
                    "tracked_since": datetime.now(timezone.utc).isoformat(),
                }
                save_json(TRACKED_CALLS_FILE, tracked_calls)
                logger.info(f"📡 New X call tracked: {call_key} @ {fmt_mc(entry_mc)}")

        await update.message.reply_text(f"✅ Posted @{tweet['screen_name']}'s tweet to the channel.")

# ── Feature 2: retweet / follower milestone job ──────────────────────────
async def engagement_job(context: ContextTypes.DEFAULT_TYPE):
    if not tracked_tweets:
        return
    for tweet_id, info in list(tracked_tweets.items()):
        tweet = await fetch_tweet(tweet_id)
        if not tweet:
            continue

        rt_step  = (tweet["retweets"] // RETWEET_STEP) * RETWEET_STEP
        fol_step = (tweet["followers"] // FOLLOWER_STEP) * FOLLOWER_STEP

        if rt_step > info.get("last_retweet_step", 0) and rt_step > 0:
            try:
                await context.bot.send_message(
                    TARGET_CHAN,
                    f'🔮 <b>WIZRAD X SIGNAL — RETWEET UPDATE</b>\n\n'
                    f'@{tweet["screen_name"]}\'s post just crossed <b>{rt_step:,} retweets</b> 🚀\n\n'
                    f'🔗 <a href="{tweet["url"]}">View on X</a>',
                    parse_mode=ParseMode.HTML)
                info["last_retweet_step"] = rt_step
            except Exception as e:
                logger.warning(f"Retweet update failed for {tweet_id}: {e}")

        if fol_step > info.get("last_follower_step", 0) and fol_step > 0:
            try:
                await context.bot.send_message(
                    TARGET_CHAN,
                    f'🔮 <b>WIZRAD X SIGNAL — FOLLOWER MILESTONE</b>\n\n'
                    f'@{tweet["screen_name"]} just crossed <b>{fol_step:,} followers</b> 📈\n\n'
                    f'🔗 <a href="{tweet["url"]}">View on X</a>',
                    parse_mode=ParseMode.HTML)
                info["last_follower_step"] = fol_step
            except Exception as e:
                logger.warning(f"Follower update failed for {tweet_id}: {e}")

        tracked_tweets[tweet_id] = info
        await asyncio.sleep(1.5)  # gentle pacing, avoid hammering the free API

    save_json(TRACKED_TWEETS_FILE, tracked_tweets)

# ── Feature 3b: X-alert milestone job (2x / 5x / 10x ...) ────────────────
MILESTONES = [2, 3, 5, 10, 20, 50, 100, 250, 500, 1000]

async def x_alert_job(context: ContextTypes.DEFAULT_TYPE):
    if not tracked_calls:
        return
    for call_key, call in list(tracked_calls.items()):
        entry_mc = float(call.get("entry_mc", 0) or 0)
        if entry_mc <= 0:
            continue
        dex = await fetch_dex(call["ca"])
        if not dex or dex["mc"] <= 0:
            continue
        ratio = dex["mc"] / entry_mc
        last_milestone = call.get("last_milestone", 1)

        hit = None
        for m in MILESTONES:
            if ratio >= m and m > last_milestone:
                hit = m
        if hit:
            try:
                await context.bot.send_message(
                    TARGET_CHAN,
                    f'🔮 <b>@{call["screen_name"]} X CALL Hit {hit}X+</b>\n\n'
                    f'${call["symbol"]} {call["chain"]} play called at {fmt_mc(entry_mc)}. '
                    f'Current MC stands at {fmt_mc(dex["mc"])}.\n\n'
                    f'Ca: <code>{call["ca"]}</code>\n\n'
                    f'🔗 <a href="{call["tweet_url"]}">Original tweet</a>',
                    parse_mode=ParseMode.HTML)
                call["last_milestone"] = hit
                tracked_calls[call_key] = call
            except Exception as e:
                logger.warning(f"X-alert failed for {call_key}: {e}")
        await asyncio.sleep(1.0)

    save_json(TRACKED_CALLS_FILE, tracked_calls)

# ── Entry point ────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN or not SUBMIT_CHAT or not TARGET_CHAN:
        raise SystemExit(
            "Missing config — set TWITTER_BOT_TOKEN, TWITTER_SUBMIT_CHAT, TWITTER_CHANNEL")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT | filters.CAPTION, handle_submission))
    app.job_queue.run_repeating(engagement_job, interval=POLL_SECONDS, first=30)
    app.job_queue.run_repeating(x_alert_job,     interval=POLL_SECONDS, first=60)

    logger.info("✅ WIZRAD Scan Twitter bot started")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
