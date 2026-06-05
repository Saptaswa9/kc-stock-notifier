#!/usr/bin/env python3
"""
Krazy Caterpillar stock notifier
=================================
Watches Shopify collections on https://krazycaterpillar.com and alerts you when:
  - a NEW product is listed (e.g. new Hot Wheels / Majorette cars), and
  - a previously-seen product comes BACK IN STOCK (restock).

How it works
------------
Shopify exposes a public JSON feed for every collection at
    /collections/<handle>/products.json
This script reads that feed and keeps a small JSON file on disk recording every
product it has seen AND that product's last-known stock status. On each run it
reports product IDs it has never seen (new listings) and items whose status
flipped from sold out to in stock (restocks). The first run records a baseline
silently so you don't get spammed with the entire existing catalogue.

Zero third-party dependencies - only the Python 3 standard library.

Notification channels (all optional, pick any via environment variables):
  - Console / log file      (always on)
  - Discord webhook         DISCORD_WEBHOOK_URL
  - Telegram bot            TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID
  - Email (SMTP)            SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS,
                            EMAIL_FROM, EMAIL_TO

Usage
-----
  python stock_notifier.py              # check once (use with cron / Task Scheduler)
  python stock_notifier.py --loop       # keep running, checking every 15 min
  python stock_notifier.py --loop --interval 600   # check every 600s
  python stock_notifier.py --reset      # forget saved state (re-baseline next run)
"""

import argparse
import json
import os
import smtplib
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

# --------------------------------------------------------------------------- #
# CONFIG - edit these to taste
# --------------------------------------------------------------------------- #
BASE = "https://krazycaterpillar.com"

# Collections to watch. The value is the Shopify "handle" - the slug that
# appears in a collection URL: krazycaterpillar.com/collections/<handle>
# To add another brand/line, open its collection page and copy the slug here.
COLLECTIONS = {
    "Hot Wheels Mainline": "hot-wheels-mainline",
    "Hot Wheels Treasure Hunt": "hot-wheels-treasure-hunt",
    "Majorette": "majorette-france",
}

# Alert when a previously-seen, sold-out item comes back in stock.
ENABLE_RESTOCK_ALERTS = True

STATE_FILE = Path(os.environ.get("KC_STATE_FILE", "seen_products.json"))
LOG_FILE = Path(os.environ.get("KC_LOG_FILE", "stock_notifier.log"))

USER_AGENT = "Mozilla/5.0 (compatible; KCStockNotifier/1.1; personal use)"
REQUEST_DELAY = 1.0   # seconds to wait between paged requests (be polite)
TIMEOUT = 20          # seconds per HTTP request
MAX_RETRIES = 3
DEFAULT_INTERVAL = 900  # 15 minutes for --loop mode


# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
def log(msg):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line, flush=True)
    try:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass  # logging to file is best-effort


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def http_get_json(url):
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:  # noqa: BLE001 - retry on any transient failure
            last_err = e
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)
    raise RuntimeError(f"GET failed after {MAX_RETRIES} tries: {url} ({last_err})")


def http_post(url, data_bytes, headers):
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read()


def fetch_collection(handle):
    """Return all products in a collection, following pagination."""
    products = []
    page = 1
    while True:
        url = f"{BASE}/collections/{handle}/products.json?limit=250&page={page}"
        data = http_get_json(url)
        batch = data.get("products", [])
        if not batch:
            break
        products.extend(batch)
        if len(batch) < 250:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return products


# --------------------------------------------------------------------------- #
# Product handling
# --------------------------------------------------------------------------- #
def is_in_stock(product):
    return any(v.get("available") for v in (product.get("variants") or []))


def summarize(product):
    variants = product.get("variants", []) or []
    prices = []
    for v in variants:
        try:
            prices.append(float(v.get("price")))
        except (TypeError, ValueError):
            pass
    images = product.get("images") or []
    image = images[0].get("src", "") if images else ""
    return {
        "id": product.get("id"),
        "title": product.get("title", "Untitled"),
        "url": f"{BASE}/products/{product.get('handle', '')}",
        "price": min(prices) if prices else None,
        "in_stock": is_in_stock(product),
        "published_at": product.get("published_at"),
        "image": image,
    }


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            log("WARNING: state file unreadable; starting fresh.")
    return {}


def save_state(state):
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(state), encoding="utf-8")
    tmp.replace(STATE_FILE)  # atomic write


def _read_prior(prev):
    """Normalise a handle's stored state into (seen_ids, prev_stock).

    Supports two on-disk formats:
      - new format: {"<id>": <in_stock bool>, ...}
      - old format: ["<id>", ...]  (no stock history -> migrated silently)
    """
    if isinstance(prev, dict):
        prev_stock = {str(k): v for k, v in prev.items()}
        return set(prev_stock.keys()), prev_stock
    if isinstance(prev, list):  # old format from v1.0
        return set(str(x) for x in prev), {}
    return set(), {}


# --------------------------------------------------------------------------- #
# Core check
# --------------------------------------------------------------------------- #
def run_once(notify=True):
    state = load_state()
    first_run = not state
    new_items = []
    restocked_items = []

    for name, handle in COLLECTIONS.items():
        try:
            products = fetch_collection(handle)
        except Exception as e:  # noqa: BLE001
            log(f"ERROR fetching '{name}' ({handle}): {e}")
            continue

        seen_ids, prev_stock = _read_prior(state.get(handle))
        migrating = isinstance(state.get(handle), list)

        current_stock = {}
        fresh = 0
        back = 0
        for p in products:
            pid = p.get("id")
            if pid is None:
                continue
            key = str(pid)
            cur = is_in_stock(p)
            current_stock[key] = cur

            if key not in seen_ids:
                # Brand-new listing.
                fresh += 1
                if not first_run:
                    item = summarize(p)
                    item["collection"] = name
                    new_items.append(item)
            elif ENABLE_RESTOCK_ALERTS and prev_stock.get(key) is False and cur:
                # Seen before, was sold out, now available -> restock.
                back += 1
                item = summarize(p)
                item["collection"] = name
                restocked_items.append(item)

        state[handle] = current_stock
        if first_run:
            log(f"[baseline] {name}: tracking {len(current_stock)} products")
        elif migrating:
            log(f"{name}: {fresh} new (stock baseline added; restock alerts active next run)")
        else:
            log(f"{name}: {fresh} new, {back} restocked")

    save_state(state)

    if first_run:
        log("Baseline established. New listings and restocks will alert from now on.")
        return [], []

    # A product can appear in more than one watched collection; dedupe by id.
    new_items = list({it["id"]: it for it in new_items}.values())
    restocked_items = list({it["id"]: it for it in restocked_items}.values())
    # If something is both new and restocked (rare cross-collection case), keep it as new only.
    new_ids = {it["id"] for it in new_items}
    restocked_items = [it for it in restocked_items if it["id"] not in new_ids]

    if (new_items or restocked_items) and notify:
        send_notifications(new_items, restocked_items)
    elif not new_items and not restocked_items:
        log("Nothing new, nothing restocked.")
    return new_items, restocked_items


# --------------------------------------------------------------------------- #
# Notifications
# --------------------------------------------------------------------------- #
def _format_items(items):
    lines = []
    for it in items:
        price = f"Rs. {it['price']:.2f}" if it["price"] is not None else "price n/a"
        stock = "in stock" if it["in_stock"] else "sold out"
        lines.append(f"- [{it['collection']}] {it['title']} - {price} ({stock})\n  {it['url']}")
    return "\n".join(lines)


def build_message(new_items, restocked_items):
    parts = []
    if new_items:
        parts.append(f"{len(new_items)} new listing(s) on Krazy Caterpillar:")
        parts.append(_format_items(new_items))
    if restocked_items:
        parts.append(f"{len(restocked_items)} item(s) back in stock:")
        parts.append(_format_items(restocked_items))
    return "\n\n".join(parts)


def notify_discord(text):
    webhook = os.environ["DISCORD_WEBHOOK_URL"]
    for chunk in _chunks(text, 1900):  # Discord caps content at 2000 chars
        payload = json.dumps({"content": chunk}).encode("utf-8")
        http_post(webhook, payload, {"Content-Type": "application/json"})
        time.sleep(0.5)


def notify_telegram(text):
    token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chunk in _chunks(text, 4000):
        payload = urllib.parse.urlencode(
            {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": "true"}
        ).encode("utf-8")
        http_post(url, payload, {"Content-Type": "application/x-www-form-urlencoded"})
        time.sleep(0.5)


def notify_email(text):
    host = os.environ["SMTP_HOST"]
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASS")
    sender = os.environ.get("EMAIL_FROM", user)
    recipient = os.environ["EMAIL_TO"]

    msg = MIMEText(text, "plain", "utf-8")
    msg["Subject"] = "Krazy Caterpillar: new listings / restocks"
    msg["From"] = sender
    msg["To"] = recipient

    with smtplib.SMTP(host, port, timeout=TIMEOUT) as server:
        server.starttls()
        if user and password:
            server.login(user, password)
        server.sendmail(sender, [recipient], msg.as_string())


def send_notifications(new_items, restocked_items):
    text = build_message(new_items, restocked_items)
    log(text)

    channels = []
    if os.environ.get("DISCORD_WEBHOOK_URL"):
        channels.append(("Discord", notify_discord))
    if os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"):
        channels.append(("Telegram", notify_telegram))
    if os.environ.get("SMTP_HOST") and os.environ.get("EMAIL_TO"):
        channels.append(("Email", notify_email))

    if not channels:
        log("(No notification channel configured - see README. Printed above only.)")
        return

    for label, fn in channels:
        try:
            fn(text)
            log(f"Sent via {label}.")
        except Exception as e:  # noqa: BLE001
            log(f"{label} notification failed: {e}")


def _chunks(text, size):
    for i in range(0, len(text), size):
        yield text[i : i + size]


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
def main(argv=None):
    parser = argparse.ArgumentParser(description="Krazy Caterpillar stock notifier")
    parser.add_argument("--loop", action="store_true", help="run continuously")
    parser.add_argument(
        "--interval", type=int, default=DEFAULT_INTERVAL,
        help=f"seconds between checks in --loop mode (default {DEFAULT_INTERVAL})",
    )
    parser.add_argument("--reset", action="store_true", help="delete saved state and exit")
    args = parser.parse_args(argv)

    if args.reset:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
            log("State reset. Next run will re-establish the baseline.")
        else:
            log("No state file to reset.")
        return

    if args.loop:
        log(f"Starting in loop mode, every {args.interval}s. Ctrl-C to stop.")
        while True:
            try:
                run_once()
            except KeyboardInterrupt:
                log("Stopped.")
                break
            except Exception as e:  # noqa: BLE001 - keep the loop alive
                log(f"Unexpected error: {e}")
            time.sleep(args.interval)
    else:
        run_once()


if __name__ == "__main__":
    main()
