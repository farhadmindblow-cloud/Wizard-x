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
import uuid
import asyncio
import logging
import html as html_lib
from datetime import datetime, timezone

import aiohttp
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, MessageHandler, CommandHandler, CallbackQueryHandler,
    ContextTypes, filters,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("wizrad_twitter")

# ── Config ───────────────────────────────────────────────────────────────
BOT_TOKEN    = os.environ.get("TWITTER_BOT_TOKEN", "")
SUBMIT_CHAT  = int(os.environ.get("TWITTER_SUBMIT_CHAT", "0") or 0)
TARGET_CHAN  = os.environ.get("TWITTER_CHANNEL", "")
DATA_DIR     = os.environ.get("DATA_DIR", ".")

# ── Owner — restricted to a single Telegram user ID ──────────────────────
OWNER_ID   = int(os.environ.get("OWNER_ID", "6018602211") or 0)
OWNER_ID_2 = int(os.environ.get("OWNER_ID_2", "0") or 0)
OWNER_IDS  = [oid for oid in [OWNER_ID, OWNER_ID_2] if oid]

RETWEET_STEP  = int(os.environ.get("RETWEET_STEP", "50"))    # feature 2: every 50 retweets
FOLLOWER_STEP = int(os.environ.get("FOLLOWER_STEP", "100"))  # feature 2: every 100 followers
POLL_SECONDS  = int(os.environ.get("TWITTER_POLL_SECONDS", "480"))  # ~8 min
MIN_ALERT_X   = 2  # matches the Telegram bot's own 2x alert threshold

# feature 4: auto-watch /addx KOL handles for brand-new posts (no manual
# submission needed). See fetch_user_recent_tweets() for important caveats —
# this relies on an UNOFFICIAL, undocumented endpoint that X can throttle or
# kill without notice. Manual submission (group/DM) keeps working regardless.
KOL_POLL_SECONDS = int(os.environ.get("KOL_POLL_SECONDS", "180"))  # ~3 min
KOL_TWEET_LIMIT  = 10  # how many recent tweets to pull per handle per check

def _dp(name):
    return os.path.join(DATA_DIR, name)

TRACKED_TWEETS_FILE = _dp("twitter_tracked_tweets.json")   # engagement tracking (feature 1+2)
TRACKED_CALLS_FILE  = _dp("twitter_tracked_calls.json")    # token-call tracking (feature 3)
KOLS_FILE           = _dp("twitter_kols.json")             # owner-curated X handle list
KOL_LAST_SEEN_FILE  = _dp("twitter_kol_last_seen.json")    # feature 4: last tweet id posted per handle
CUSTOM_CMDS_FILE    = _dp("twitter_custom_commands.json")  # owner-built /commands
PENDING_REQ_FILE    = _dp("twitter_pending_requests.json") # public submit requests
START_CFG_FILE      = _dp("twitter_start_config.json")     # /start text+media override
CARD_TEMPLATE_FILE  = _dp("twitter_card_template.json")    # channel signal-card template

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
tracked_kols   = load_json(KOLS_FILE, [])             # list[str] handles, no @, lowercase-checked
kol_last_seen  = load_json(KOL_LAST_SEEN_FILE, {})    # handle(lower) -> last posted tweet id (str)
custom_cmds    = load_json(CUSTOM_CMDS_FILE, {})      # name -> {text, media_file_id, media_type, buttons}
pending_reqs   = load_json(PENDING_REQ_FILE, {})      # req_id -> {...}
start_cfg      = load_json(START_CFG_FILE, {})        # {text, media_file_id, media_type}
card_template  = load_json(CARD_TEMPLATE_FILE, {})    # {"text": "..."}

owner_state = {}  # uid -> {"state": str, "data": {...}}  (wizard steps for /setstart, /newcommand)

RESERVED_CMDS = {
    "start", "help", "mystatus", "cancel", "ownerhelp",
    "addx", "removex", "listx", "checkx", "pending",
    "setstart", "setcard", "showcard", "resetcard",
    "newcommand", "listcommands", "removecommand",
}

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

# Profile link (no /status/...), e.g. https://x.com/FlameBornDragon
PROFILE_URL_RE = re.compile(
    r"https?://(?:www\.|mobile\.)?(?:twitter|x)\.com/([A-Za-z0-9_]{1,20})/?(?:[?#].*)?$",
    re.IGNORECASE,
)

def parse_handle_input(raw):
    """Accepts a full profile link (https://x.com/name), a status link, a
    bare @name, or plain name — returns the lowercase handle, or None."""
    if not raw:
        return None
    raw = raw.strip()
    m = TWEET_URL_RE.search(raw)
    if m:
        return m.group(1).lower()
    m = PROFILE_URL_RE.search(raw)
    if m:
        return m.group(1).lower()
    return raw.lstrip("@").strip().lower() or None

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
    tpl = card_template.get("text")
    if tpl:
        try:
            return tpl.format(
                screen_name=tweet["screen_name"], verified_mark=verified_mark,
                quote=quote, url=tweet["url"],
            )
        except Exception as e:
            logger.warning(f"Custom card template failed to format, using default: {e}")
    return (
        f'🔮 <b>WIZRAD X SIGNAL</b> — @{tweet["screen_name"]}{verified_mark}\n\n'
        f'<blockquote>{quote}</blockquote>\n\n'
        f'🔗 <a href="{tweet["url"]}">View on X</a>'
    )

async def _post_accepted_tweet(tweet, context):
    """Shared posting logic used by both the group-submit flow and the
    accept/reject request flow — posts the channel card, saves tracking,
    and runs the token-call detector."""
    card = build_signal_card(tweet)
    sent = await context.bot.send_message(
        TARGET_CHAN, card, parse_mode=ParseMode.HTML, disable_web_page_preview=False)

    tracked_tweets[tweet["id"]] = {
        "screen_name":     tweet["screen_name"],
        "url":             tweet["url"],
        "channel_msg_id":  sent.message_id,
        "last_retweet_step":  (tweet["retweets"] // RETWEET_STEP) * RETWEET_STEP,
        "last_follower_step": (tweet["followers"] // FOLLOWER_STEP) * FOLLOWER_STEP,
        "added_at": datetime.now(timezone.utc).isoformat(),
    }
    save_json(TRACKED_TWEETS_FILE, tracked_tweets)

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
                "ca": ca, "chain": chain,
                "symbol": (dex or {}).get("symbol", "TOKEN"),
                "entry_mc": entry_mc,
                "last_milestone": 1,
                "tracked_since": datetime.now(timezone.utc).isoformat(),
            }
            save_json(TRACKED_CALLS_FILE, tracked_calls)
            logger.info(f"📡 New X call tracked: {call_key} @ {fmt_mc(entry_mc)}")
    return sent

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

        try:
            await _post_accepted_tweet(tweet, context)
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to post to channel: {e}")
            continue

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

# ── Feature 4: auto-watch KOL handles for brand-new posts ────────────────
# IMPORTANT CAVEAT: unlike fetch_tweet() (single-status lookup via FxTwitter,
# which is a maintained, reliable free API), there is NO reliable free/no-
# login API in 2026 that lists "a user's latest tweets". This uses X's old
# embed-widget "syndication" endpoint, which is UNOFFICIAL, UNDOCUMENTED,
# and has been heavily gated since 2024 — X can return empty/blocked
# responses at any time without notice. When that happens this job just
# quietly finds nothing new; it will never crash the bot. Manual submission
# (the group/DM flow above) is unaffected and remains the reliable fallback.
async def fetch_user_recent_tweets(screen_name, limit=KOL_TWEET_LIMIT):
    """Best-effort fetch of a user's most recent public tweets, newest first.
    Returns a list of dicts in the same shape as fetch_tweet(), or [] if the
    endpoint is unavailable/blocked/empty."""
    url = (
        "https://cdn.syndication.twimg.com/timeline/profile"
        f"?screen_name={screen_name}&dnt=true&showReplies=false"
    )
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=12),
                headers={"User-Agent": "Mozilla/5.0 (compatible; WizradScanBot/1.0)"},
            ) as resp:
                if resp.status != 200:
                    logger.info(f"KOL watch: syndication status {resp.status} for @{screen_name}")
                    return []
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    return []
    except Exception as e:
        logger.info(f"KOL watch: fetch failed for @{screen_name}: {e}")
        return []

    tweets_map = ((data or {}).get("globalObjects") or {}).get("tweets") or {}
    users_map  = ((data or {}).get("globalObjects") or {}).get("users") or {}
    if not tweets_map:
        return []

    out = []
    for tid, t in tweets_map.items():
        try:
            uid = str(t.get("user_id_str") or t.get("user_id") or "")
            author = users_map.get(uid, {})
            out.append({
                "id":  str(t.get("id_str") or tid),
                "url": f'https://x.com/{screen_name}/status/{t.get("id_str") or tid}',
                "text": t.get("full_text") or t.get("text") or "",
                "likes":    int(t.get("favorite_count", 0) or 0),
                "retweets": int(t.get("retweet_count", 0) or 0),
                "replies":  int(t.get("reply_count", 0) or 0),
                "views": None,
                "screen_name": author.get("screen_name", screen_name),
                "name":        author.get("name", ""),
                "followers":   int(author.get("followers_count", 0) or 0),
                "verified":    bool(author.get("verified") or author.get("is_blue_verified")),
                "avatar_url":  author.get("profile_image_url_https"),
            })
        except Exception:
            continue

    out.sort(key=lambda x: int(x["id"]), reverse=True)
    return out[:limit]

async def kol_watch_job(context: ContextTypes.DEFAULT_TYPE):
    """Every KOL_POLL_SECONDS, check each /addx handle for tweets newer than
    the last one we posted, and auto-post any found (oldest-first) through
    the same pipeline as manual submissions."""
    if not tracked_kols:
        return
    for handle in list(tracked_kols):
        h = handle.lower().lstrip("@")
        recent = await fetch_user_recent_tweets(h)
        if not recent:
            await asyncio.sleep(1.5)
            continue

        last_id = kol_last_seen.get(h)
        if last_id is None:
            # First time seeing this handle — baseline on its newest tweet
            # only, so we don't dump its whole recent history into the
            # channel at once.
            kol_last_seen[h] = recent[0]["id"]
            save_json(KOL_LAST_SEEN_FILE, kol_last_seen)
            await asyncio.sleep(1.5)
            continue

        new_ones = [t for t in recent if int(t["id"]) > int(last_id)]
        if not new_ones:
            await asyncio.sleep(1.5)
            continue

        for tweet in sorted(new_ones, key=lambda x: int(x["id"])):  # oldest first
            if tweet["id"] in tracked_tweets:
                continue
            try:
                await _post_accepted_tweet(tweet, context)
                logger.info(f"📡 Auto-posted new tweet from @{h}: {tweet['id']}")
            except Exception as e:
                logger.warning(f"KOL watch: failed to post {tweet['id']} from @{h}: {e}")

        kol_last_seen[h] = new_ones[-1]["id"]
        save_json(KOL_LAST_SEEN_FILE, kol_last_seen)
        await asyncio.sleep(1.5)

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

# ═════════════════════════════════════════════════════════════════════════
# PUBLIC COMMANDS
# ═════════════════════════════════════════════════════════════════════════
DEFAULT_START_TEXT = (
    "🔮 <b>Welcome to WIZRAD Scan Twitter</b>\n\n"
    "Follow the top calls and trending posts from X (Twitter) here — with live "
    "retweet/follower milestones and token-call X-alerts.\n\n"
    "📤 Want to submit a tweet? Just send the link right here (in this DM) — "
    "our team will review it and post it to the channel.\n\n"
    "❓ Send /help for the full command list."
)

PUBLIC_HELP_TEXT = (
    "❓ <b>PUBLIC COMMANDS</b>\n\n"
    "/start — bot intro\n"
    "/help — this list\n"
    "/mystatus — status of your last submitted request\n\n"
    "📤 Send any X/Twitter post link right here (in this DM) — "
    "we'll review it and accept or reject it."
)

def _extract_media(message):
    if message.photo:
        return message.photo[-1].file_id, "photo"
    if message.video:
        return message.video.file_id, "video"
    if message.animation:
        return message.animation.file_id, "animation"
    if message.document:
        return message.document.file_id, "document"
    return None, None

def _kb_from_buttons(buttons):
    if not buttons:
        return None
    return InlineKeyboardMarkup([[InlineKeyboardButton(b[0], url=b[1])] for b in buttons])

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = start_cfg.get("text", DEFAULT_START_TEXT)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📤 Submit a Tweet", callback_data="pub:howsubmit")],
        [InlineKeyboardButton("❓ Help", callback_data="pub:help")],
    ])
    media_id = start_cfg.get("media_file_id")
    if media_id:
        mtype = start_cfg.get("media_type", "photo")
        sender = {"photo": update.message.reply_photo, "video": update.message.reply_video,
                   "animation": update.message.reply_animation}.get(mtype, update.message.reply_photo)
        await sender(media_id, caption=text, parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(PUBLIC_HELP_TEXT, parse_mode=ParseMode.HTML)

async def cmd_mystatus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    mine = [r for r in pending_reqs.values() if r.get("user_id") == uid]
    if not mine:
        await update.message.reply_text("You haven't submitted any tweet yet.")
        return
    last = mine[-1]
    status_map = {"pending": "⏳ Pending review", "accepted": "✅ Accepted & posted",
                  "rejected": "❌ Rejected"}
    await update.message.reply_text(f"Your last request: {status_map.get(last['status'], last['status'])}")

async def cb_public(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == "pub:help":
        await q.message.reply_text(PUBLIC_HELP_TEXT, parse_mode=ParseMode.HTML)
    elif q.data == "pub:howsubmit":
        await q.message.reply_text(
            "📤 Just send a tweet or x.com link right here (in this DM).\n"
            "Our team will review it and accept or reject it — check /mystatus "
            "for the status.", parse_mode=ParseMode.HTML)

# ═════════════════════════════════════════════════════════════════════════
# SUBMIT REQUEST FLOW (public DM → owner Accept/Reject)
# ═════════════════════════════════════════════════════════════════════════
def _save_pending():
    save_json(PENDING_REQ_FILE, pending_reqs)

async def handle_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id

    # Owner mid-wizard (setstart / newcommand) — route there instead
    if uid in OWNER_IDS and uid in owner_state:
        await _route_owner_state(update, context)
        return

    text = update.message.text or ""
    links = extract_tweet_links(text)
    if not links:
        return

    screen_name, tweet_id = links[0]
    if tweet_id in tracked_tweets:
        await update.message.reply_text("⏭️ This tweet has already been posted to the channel.")
        return
    for r in pending_reqs.values():
        if r.get("tweet_id") == tweet_id and r.get("status") == "pending":
            await update.message.reply_text("⏳ This request is already under review, please wait.")
            return

    req_id = uuid.uuid4().hex[:10]
    user = update.effective_user
    is_kol = screen_name.lower() in [k.lower() for k in tracked_kols]
    link = text.strip()
    pending_reqs[req_id] = {
        "tweet_id": tweet_id, "link": link, "screen_name": screen_name,
        "user_id": uid, "user_name": user.full_name, "username": user.username or "",
        "status": "pending", "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _save_pending()

    kol_tag = "✅ Listed KOL" if is_kol else "⚠️ Not in KOL list"
    card = (
        f"📥 <b>NEW SUBMIT REQUEST</b>\n\n"
        f"👤 From: {html_lib.escape(user.full_name)} "
        f"(@{user.username or 'no_username'}) — <code>{uid}</code>\n"
        f"🐦 @{screen_name} — {kol_tag}\n"
        f"🔗 <a href=\"{link}\">{link}</a>"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Accept", callback_data=f"req:acc:{req_id}"),
        InlineKeyboardButton("❌ Reject", callback_data=f"req:rej:{req_id}"),
    ]])
    if not OWNER_IDS:
        logger.warning("OWNER_ID not set — submit request could not be delivered to anyone.")
    for oid in OWNER_IDS:
        try:
            await context.bot.send_message(oid, card, parse_mode=ParseMode.HTML,
                                            reply_markup=kb, disable_web_page_preview=True)
        except Exception as e:
            logger.warning(f"Could not DM owner {oid}: {e}")

    await update.message.reply_text("✅ Your request has been sent — the owner will review it. Thank you!")

async def cb_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid = q.from_user.id
    if uid not in OWNER_IDS:
        await q.answer("🚫 Owner only.", show_alert=True)
        return

    _, action, req_id = q.data.split(":")
    req = pending_reqs.get(req_id)
    if not req or req.get("status") != "pending":
        await q.answer("This request has already been handled.", show_alert=True)
        return

    if action == "acc":
        tweet = await fetch_tweet(req["tweet_id"])
        if not tweet:
            await q.answer("⚠️ Couldn't fetch the tweet (deleted/private).", show_alert=True)
            return
        try:
            await _post_accepted_tweet(tweet, context)
        except Exception as e:
            await q.answer(f"❌ Channel post failed: {e}", show_alert=True)
            return
        req["status"] = "accepted"
        _save_pending()
        await q.edit_message_text(
            (q.message.text_html or q.message.text) + "\n\n✅ <b>ACCEPTED & POSTED</b>",
            parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(
                req["user_id"], f"✅ Your tweet has been posted to the channel!\n{tweet['url']}")
        except Exception:
            pass
    else:
        req["status"] = "rejected"
        _save_pending()
        await q.edit_message_text(
            (q.message.text_html or q.message.text) + "\n\n❌ <b>REJECTED</b>",
            parse_mode=ParseMode.HTML)
        try:
            await context.bot.send_message(req["user_id"], "❌ Sorry, your request was not accepted.")
        except Exception:
            pass
    await q.answer()

# ═════════════════════════════════════════════════════════════════════════
# OWNER COMMANDS
# ═════════════════════════════════════════════════════════════════════════
def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else None
        if uid not in OWNER_IDS:
            await update.message.reply_text("🚫 This command is for the owner only.")
            return
        return await func(update, context)
    return wrapper

OWNER_HELP_TEXT = (
    "🔮 <b>OWNER CONTROL PANEL — WIZRAD Scan Twitter</b>\n\n"
    "📥 <b>Requests</b>\n"
    "/pending — view all open requests again (with buttons)\n\n"
    "📡 <b>X / KOL List</b>\n"
    "/addx handle_or_link · /removex handle_or_link · /listx · /checkx handle\n\n"
    "🎛 <b>Start Menu</b>\n"
    "/setstart — set /start's text + media (wizard)\n\n"
    "🧩 <b>Channel Signal Card</b>\n"
    "/setcard &lt;template&gt; — HTML template\n"
    "Placeholders: <code>{screen_name} {verified_mark} {quote} {url}</code>\n"
    "/showcard · /resetcard\n\n"
    "➕ <b>Your Custom Commands</b>\n"
    "/newcommand — create a new /command (your own text + media + buttons, HTML allowed)\n"
    "/listcommands · /removecommand name\n\n"
    "🚫 /cancel — stop any active wizard"
)

def _oh_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📥 Pending Requests", callback_data="oh:pending")],
        [InlineKeyboardButton("📡 KOL List", callback_data="oh:kols")],
        [InlineKeyboardButton("🎛 Start Menu", callback_data="oh:start")],
        [InlineKeyboardButton("🧩 Signal Card", callback_data="oh:card")],
        [InlineKeyboardButton("➕ Custom Commands", callback_data="oh:cmds")],
    ])

@owner_only
async def cmd_ownerhelp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(OWNER_HELP_TEXT, parse_mode=ParseMode.HTML, reply_markup=_oh_kb())

async def cb_ownerhelp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q.from_user.id not in OWNER_IDS:
        await q.answer("🚫", show_alert=True)
        return
    await q.answer()
    key = q.data.split(":")[1]
    tips = {
        "pending": "👉 Send /pending to see all open requests.",
        "kols": "👉 /addx handle_or_link · /removex handle_or_link · /listx · /checkx handle",
        "start": "👉 Send /setstart — write the text, then media or /skip.",
        "card": "👉 Send /setcard &lt;template&gt;, /showcard, /resetcard.",
        "cmds": "👉 Use /newcommand to create a new command, /listcommands · /removecommand name.",
    }
    await q.message.reply_text(tips.get(key, "..."), parse_mode=ParseMode.HTML)

@owner_only
async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    open_reqs = {k: v for k, v in pending_reqs.items() if v.get("status") == "pending"}
    if not open_reqs:
        await update.message.reply_text("✅ No pending requests.")
        return
    for req_id, req in list(open_reqs.items())[:15]:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Accept", callback_data=f"req:acc:{req_id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"req:rej:{req_id}"),
        ]])
        await update.message.reply_text(
            f"👤 {html_lib.escape(req['user_name'])} (@{req.get('username') or 'no_username'})\n"
            f"{req['link']}",
            reply_markup=kb, disable_web_page_preview=True)

@owner_only
async def cmd_addx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "Usage: /addx handle_or_link\n"
            "e.g. /addx FlameBornDragon  or  /addx https://x.com/FlameBornDragon")
        return
    h = parse_handle_input(context.args[0])
    if not h:
        await update.message.reply_text("Couldn't read that handle/link — try again.")
        return
    if h not in [k.lower() for k in tracked_kols]:
        tracked_kols.append(h)
        save_json(KOLS_FILE, tracked_kols)
    await update.message.reply_text(
        f"✅ @{h} (https://x.com/{h}) added to the KOL list. Auto-watching for "
        f"new posts every ~{KOL_POLL_SECONDS // 60} min (best-effort — see /ownerhelp).\n"
        f"Test it with /checkx {h}."
    )

@owner_only
async def cmd_removex(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global tracked_kols
    if not context.args:
        await update.message.reply_text("Usage: /removex handle_or_link")
        return
    h = parse_handle_input(context.args[0])
    if not h:
        await update.message.reply_text("Couldn't read that handle/link — try again.")
        return
    before = len(tracked_kols)
    tracked_kols = [k for k in tracked_kols if k.lower() != h]
    save_json(KOLS_FILE, tracked_kols)
    kol_last_seen.pop(h, None)
    save_json(KOL_LAST_SEEN_FILE, kol_last_seen)
    if len(tracked_kols) < before:
        await update.message.reply_text(f"🗑 @{h} removed from the list.")
    else:
        await update.message.reply_text("That handle wasn't in the list.")

@owner_only
async def cmd_listx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not tracked_kols:
        await update.message.reply_text("No KOLs in the list yet.")
        return
    await update.message.reply_text(
        "📡 <b>KOL List</b>\n" +
        "\n".join(f'• <a href="https://x.com/{k}">@{k}</a>' for k in tracked_kols),
        parse_mode=ParseMode.HTML, disable_web_page_preview=True)

@owner_only
async def cmd_checkx(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Diagnostic: manually runs the same fetch the auto-watch job uses,
    right now, and reports exactly what came back — so you can see whether
    the unofficial endpoint is currently working for a given handle."""
    if not context.args:
        await update.message.reply_text("Usage: /checkx handle_or_link")
        return
    h = parse_handle_input(context.args[0])
    if not h:
        await update.message.reply_text("Couldn't read that handle/link — try again.")
        return
    await update.message.reply_text(f"🔎 Checking @{h} live…")
    recent = await fetch_user_recent_tweets(h)
    if not recent:
        await update.message.reply_text(
            f"❌ Got 0 tweets back for @{h} right now. The unofficial endpoint is "
            f"most likely blocked/empty from this server — auto-watch won't catch "
            f"new posts for this handle until that changes. Manual submission "
            f"(paste the link in the submit group) still works.")
        return
    lines = [f"✅ Got {len(recent)} tweet(s) for @{h}. Newest:"]
    for t in recent[:3]:
        preview = (t["text"][:80] + "…") if len(t["text"]) > 80 else t["text"]
        lines.append(f'• {t["id"]} — {preview}')
    last_seen = kol_last_seen.get(h)
    lines.append(f"\nLast seen id on file: {last_seen or '(none yet — will baseline on next auto-watch cycle)'}")
    await update.message.reply_text("\n".join(lines))

@owner_only
async def cmd_setstart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_state[update.effective_user.id] = {"state": "start_text", "data": {}}
    await update.message.reply_text(
        "✏️ Send the new /start text (HTML allowed — <b>bold</b>, <i>italic</i>, "
        "<code>&lt;a href='...'&gt;link&lt;/a&gt;</code>).\n🚫 /cancel to stop.",
        parse_mode=ParseMode.HTML)

@owner_only
async def cmd_setcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.partition(" ")[2].strip()
    if not text:
        await update.message.reply_text(
            "Usage:\n<code>/setcard &lt;template&gt;</code>\n\n"
            "Placeholders: <code>{screen_name} {verified_mark} {quote} {url}</code>\n\n"
            "Example:\n"
            "<code>/setcard 🔮 &lt;b&gt;WIZRAD X SIGNAL&lt;/b&gt; — "
            "@{screen_name}{verified_mark}\n\n&lt;blockquote&gt;{quote}&lt;/blockquote&gt;\n\n"
            "🔗 &lt;a href=\"{url}\"&gt;View on X&lt;/a&gt;</code>",
            parse_mode=ParseMode.HTML)
        return
    card_template["text"] = text
    save_json(CARD_TEMPLATE_FILE, card_template)
    await update.message.reply_text("✅ Signal card template updated.")

@owner_only
async def cmd_showcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = card_template.get("text")
    if not t:
        await update.message.reply_text("Currently using the default template (no custom one set).")
        return
    await update.message.reply_text(f"<code>{html_lib.escape(t)}</code>", parse_mode=ParseMode.HTML)

@owner_only
async def cmd_resetcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    card_template.pop("text", None)
    save_json(CARD_TEMPLATE_FILE, card_template)
    await update.message.reply_text("♻️ Reverted to the default template.")

@owner_only
async def cmd_newcommand(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_state[update.effective_user.id] = {"state": "newcmd_name", "data": {}}
    await update.message.reply_text(
        "➕ <b>New Command</b>\n\nSend the command name (without /, letters/numbers only), "
        "example: <code>rules</code>\n\n🚫 /cancel to stop.", parse_mode=ParseMode.HTML)

@owner_only
async def cmd_listcommands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not custom_cmds:
        await update.message.reply_text("No custom commands created yet.")
        return
    names = "\n".join(f"• /{n}" for n in custom_cmds)
    await update.message.reply_text(f"➕ <b>Custom Commands</b>\n{names}", parse_mode=ParseMode.HTML)

@owner_only
async def cmd_removecommand(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /removecommand name")
        return
    name = context.args[0].lstrip("/").lower()
    if name in custom_cmds:
        del custom_cmds[name]
        save_json(CUSTOM_CMDS_FILE, custom_cmds)
        await update.message.reply_text(f"🗑 /{name} removed.")
    else:
        await update.message.reply_text("That command doesn't exist.")

@owner_only
async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_state.pop(update.effective_user.id, None)
    await update.message.reply_text("🚫 Stopped.")

async def _route_owner_state(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step-by-step wizard router for /setstart and /newcommand."""
    uid = update.effective_user.id
    st = owner_state.get(uid, {})
    state = st.get("state")
    data = st.setdefault("data", {})
    text = (update.message.text or "").strip()

    if text == "/cancel":
        owner_state.pop(uid, None)
        await update.message.reply_text("🚫 Stopped.")
        return

    if state == "start_text":
        data["text"] = update.message.text_html or text
        st["state"] = "start_media"
        await update.message.reply_text("🖼 Now send media (photo/video/gif), or type /skip.")
        return

    if state == "start_media":
        if text == "/skip":
            start_cfg["text"] = data["text"]
            start_cfg.pop("media_file_id", None)
            start_cfg.pop("media_type", None)
        else:
            file_id, mtype = _extract_media(update.message)
            if not file_id:
                await update.message.reply_text("Couldn't read that media — try again or /skip.")
                return
            start_cfg["text"] = data["text"]
            start_cfg["media_file_id"] = file_id
            start_cfg["media_type"] = mtype
        save_json(START_CFG_FILE, start_cfg)
        owner_state.pop(uid, None)
        await update.message.reply_text("✅ /start updated.")
        return

    if state == "newcmd_name":
        name = text.lstrip("/").lower()
        if not name.isalnum():
            await update.message.reply_text("Letters/numbers only, no spaces/symbols. Send it again:")
            return
        if name in RESERVED_CMDS:
            await update.message.reply_text("That name is reserved, try another one:")
            return
        data["name"] = name
        st["state"] = "newcmd_text"
        await update.message.reply_text("✏️ Now send this command's text (HTML allowed):")
        return

    if state == "newcmd_text":
        data["text"] = update.message.text_html or text
        st["state"] = "newcmd_media"
        await update.message.reply_text("🖼 Send media (photo/video/gif), or /skip:")
        return

    if state == "newcmd_media":
        if text == "/skip":
            data["media_file_id"] = None
            data["media_type"] = None
        else:
            file_id, mtype = _extract_media(update.message)
            if not file_id:
                await update.message.reply_text("Couldn't read that media — try again or /skip.")
                return
            data["media_file_id"] = file_id
            data["media_type"] = mtype
        st["state"] = "newcmd_buttons"
        await update.message.reply_text(
            "🔘 Want buttons? One per line:\n<code>Label - https://link.com</code>\n"
            "Or /skip if you don't want any buttons.", parse_mode=ParseMode.HTML)
        return

    if state == "newcmd_buttons":
        buttons = []
        if text != "/skip":
            for line in text.splitlines():
                if " - " in line:
                    label, url = line.split(" - ", 1)
                    label, url = label.strip(), url.strip()
                    if label and url.startswith(("http://", "https://", "tg://")):
                        buttons.append([label, url])
        custom_cmds[data["name"]] = {
            "text": data["text"],
            "media_file_id": data.get("media_file_id"),
            "media_type": data.get("media_type"),
            "buttons": buttons,
        }
        save_json(CUSTOM_CMDS_FILE, custom_cmds)
        owner_state.pop(uid, None)
        await update.message.reply_text(f"✅ /{data['name']} created! Everyone can use it now.")
        return

async def dispatch_custom_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Low-priority handler (group=1) — serves owner-built /commands. Built-in
    commands always match first in group=0, so there's no collision."""
    if not update.message or not update.message.text:
        return
    cmd = update.message.text.split()[0][1:].split("@")[0].lower()
    entry = custom_cmds.get(cmd)
    if not entry:
        return
    kb = _kb_from_buttons(entry.get("buttons"))
    media_id = entry.get("media_file_id")
    if media_id:
        mtype = entry.get("media_type", "photo")
        sender = {"photo": update.message.reply_photo, "video": update.message.reply_video,
                   "animation": update.message.reply_animation,
                   "document": update.message.reply_document}.get(mtype, update.message.reply_photo)
        await sender(media_id, caption=entry["text"], parse_mode=ParseMode.HTML, reply_markup=kb)
    else:
        await update.message.reply_text(entry["text"], parse_mode=ParseMode.HTML, reply_markup=kb)

async def _post_init(application: Application):
    await application.bot.set_my_commands([
        BotCommand("start", "Bot intro"),
        BotCommand("help", "Commands list"),
        BotCommand("mystatus", "Status of your request"),
    ])

# ── Entry point ────────────────────────────────────────────────────────
def main():
    if not BOT_TOKEN or not SUBMIT_CHAT or not TARGET_CHAN:
        raise SystemExit(
            "Missing config — set TWITTER_BOT_TOKEN, TWITTER_SUBMIT_CHAT, TWITTER_CHANNEL")
    if not OWNER_IDS:
        logger.warning("⚠️ OWNER_ID not set — /ownerhelp, /addx, /newcommand etc. are disabled, "
                        "and submit requests will have nowhere to be delivered.")

    app = Application.builder().token(BOT_TOKEN).post_init(_post_init).build()

    # Group submission flow (unchanged) — scoped to SUBMIT_CHAT only, so it
    # never intercepts private DMs meant for the request/wizard flow below.
    app.add_handler(MessageHandler(
        filters.Chat(SUBMIT_CHAT) & (filters.TEXT | filters.CAPTION), handle_submission))

    # Public
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("mystatus", cmd_mystatus))
    app.add_handler(CallbackQueryHandler(cb_public, pattern=r"^pub:"))

    # Owner
    app.add_handler(CommandHandler("ownerhelp", cmd_ownerhelp))
    app.add_handler(CallbackQueryHandler(cb_ownerhelp, pattern=r"^oh:"))
    app.add_handler(CommandHandler("pending", cmd_pending))
    app.add_handler(CallbackQueryHandler(cb_request, pattern=r"^req:"))
    app.add_handler(CommandHandler("addx", cmd_addx))
    app.add_handler(CommandHandler("removex", cmd_removex))
    app.add_handler(CommandHandler("listx", cmd_listx))
    app.add_handler(CommandHandler("checkx", cmd_checkx))
    app.add_handler(CommandHandler("setstart", cmd_setstart))
    app.add_handler(CommandHandler("setcard", cmd_setcard))
    app.add_handler(CommandHandler("showcard", cmd_showcard))
    app.add_handler(CommandHandler("resetcard", cmd_resetcard))
    app.add_handler(CommandHandler("newcommand", cmd_newcommand))
    app.add_handler(CommandHandler("listcommands", cmd_listcommands))
    app.add_handler(CommandHandler("removecommand", cmd_removecommand))
    app.add_handler(CommandHandler("cancel", cmd_cancel))

    # Private DM text — owner wizard capture + public tweet-link submissions
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, handle_private_text))

    # Owner-built custom commands — lower-priority group so built-ins always win first
    app.add_handler(MessageHandler(filters.COMMAND, dispatch_custom_command), group=1)

    app.job_queue.run_repeating(engagement_job, interval=POLL_SECONDS, first=30)
    app.job_queue.run_repeating(x_alert_job,     interval=POLL_SECONDS, first=60)
    app.job_queue.run_repeating(kol_watch_job,   interval=KOL_POLL_SECONDS, first=45)

    logger.info(f"✅ WIZRAD Scan Twitter bot started — Owner(s): {OWNER_IDS}")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
