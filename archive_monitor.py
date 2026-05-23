import asyncio
import gc
import hashlib
import json
import os
import random
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError

# ================= CONFIG =================
api_id = 36784553
api_hash = "c463b506e987f1f82e211cef8c50f952"

SOURCE_GROUP = -1003172289496
ARCHIVE_GROUP = -5281572843
ARCHIVE_TITLE_HINT = "Техника Архив"

SESSION_NAME = os.environ.get("SESSION_NAME", "archive_monitor")
DB_NAME = os.environ.get("DB_NAME", "archive_map.db")
QUEUE_WORKERS = int(os.environ.get("QUEUE_WORKERS", "2"))
QUEUE_MAXSIZE = int(os.environ.get("QUEUE_MAXSIZE", "5000"))
TASHKENT_TZ = timezone(timedelta(hours=5), name="Asia/Tashkent")

# Telegram anti-ban safe limits
SEND_DELAY_MIN = float(os.environ.get("SEND_DELAY_MIN", "0.8"))
SEND_DELAY_MAX = float(os.environ.get("SEND_DELAY_MAX", "1.5"))
BURST_LIMIT = int(os.environ.get("BURST_LIMIT", "20"))
BURST_PAUSE_SECONDS = float(os.environ.get("BURST_PAUSE_SECONDS", "3"))

# IMPORTANT: archive group messages will NOT be deleted if source messages are deleted.
DELETE_SYNC_ENABLED = False

client = TelegramClient(SESSION_NAME, api_id, api_hash)
_db_lock = threading.Lock()
start_time = time.time()
archive_queue = None
runtime_state = {
    "status": "starting",
    "source": None,
    "archive": None,
    "last_event_at": None,
    "last_forward_at": None,
    "last_error": None,
    "queue_size": 0,
    "workers": QUEUE_WORKERS,
    "safe_sent_count": 0,
    "last_safe_pause_at": None,
}


_registered_handlers = []

# ================= SQLITE DATABASE =================
def db_connect():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

conn = db_connect()
safe_limit_lock = asyncio.Lock()
safe_sent_count = 0


def now_text():
    # Render server UTC bo‘lsa ham doim O‘zbekiston vaqti (UTC+5) qaytadi.
    return datetime.now(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S")


def tg_time_text(dt):
    # Telegram message.date odatda UTC bo‘ladi. Uni Tashkent vaqtiga majburiy o‘giramiz.
    if not dt:
        return now_text()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TASHKENT_TZ).strftime("%Y-%m-%d %H:%M:%S")


def init_db():
    with _db_lock:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_map (
                source_chat_id INTEGER NOT NULL,
                source_msg_id INTEGER NOT NULL,
                archive_chat_id INTEGER NOT NULL,
                archive_msg_id INTEGER NOT NULL,
                media_type TEXT,
                sender_name TEXT,
                sender_username TEXT,
                message_text TEXT,
                source_created_at TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (source_chat_id, source_msg_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_messages (
                source_chat_id INTEGER NOT NULL,
                source_msg_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (source_chat_id, source_msg_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS error_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                place TEXT NOT NULL,
                error TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                place TEXT NOT NULL,
                duration_ms INTEGER NOT NULL,
                queue_size INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS counters (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media_fingerprints (
                media_hash TEXT PRIMARY KEY,
                media_type TEXT NOT NULL,
                source_chat_id INTEGER NOT NULL,
                source_msg_id INTEGER NOT NULL,
                archive_msg_id INTEGER,
                sender_name TEXT,
                sender_username TEXT,
                first_seen_at TEXT NOT NULL,
                source_created_at TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_events (
                source_chat_id INTEGER NOT NULL,
                source_msg_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (source_chat_id, source_msg_id, event_type, content_hash)
            )
            """
        )


        # Safe migrations for older SQLite files already created on Render.
        for col_name, col_type in (
            ("sender_name", "TEXT"),
            ("sender_username", "TEXT"),
            ("message_text", "TEXT"),
            ("source_created_at", "TEXT"),
        ):
            try:
                conn.execute(f"ALTER TABLE message_map ADD COLUMN {col_name} {col_type}")
            except sqlite3.OperationalError:
                pass

        # Advanced indexes for speed on big archives.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_map_archive ON message_map (archive_chat_id, archive_msg_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_map_created ON message_map (created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_processed_created ON processed_messages (created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_error_created ON error_log (created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_perf_created ON performance_log (created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_type_seen ON media_fingerprints (media_type, first_seen_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_source ON media_fingerprints (source_chat_id, source_msg_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_events (created_at)")
        conn.commit()


def inc_counter(key, amount=1):
    with _db_lock:
        conn.execute(
            """
            INSERT INTO counters (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = value + excluded.value
            """,
            (key, amount),
        )
        conn.commit()


def get_counter(key):
    with _db_lock:
        row = conn.execute("SELECT value FROM counters WHERE key = ?", (key,)).fetchone()
    return int(row[0]) if row else 0


def log_error(place, error):
    text = repr(error)
    runtime_state["last_error"] = f"{place}: {text}"
    print(f"❌ {place}: {text}", flush=True)
    with _db_lock:
        conn.execute(
            "INSERT INTO error_log (created_at, place, error) VALUES (?, ?, ?)",
            (now_text(), place, text),
        )
        conn.commit()
    inc_counter("errors", 1)


def log_perf(place, started_at):
    duration_ms = int((time.perf_counter() - started_at) * 1000)
    qsize = archive_queue.qsize() if archive_queue else 0
    runtime_state["queue_size"] = qsize
    if duration_ms >= 500 or place in ("forward", "edit_audit"):
        print(f"⚡ PERF {place}: {duration_ms}ms | queue={qsize}", flush=True)
    with _db_lock:
        conn.execute(
            "INSERT INTO performance_log (created_at, place, duration_ms, queue_size) VALUES (?, ?, ?, ?)",
            (now_text(), place, duration_ms, qsize),
        )
        conn.commit()


def media_type_from_message(message):
    if getattr(message, "photo", None):
        return "photo"
    if getattr(message, "video", None):
        return "video"
    if getattr(message, "video_note", None):
        return "video_note"
    if getattr(message, "document", None):
        return "document"
    if getattr(message, "voice", None):
        return "voice"
    if getattr(message, "audio", None):
        return "audio"
    if getattr(message, "sticker", None):
        return "sticker"
    if getattr(message, "media", None):
        return "media"
    return "text"


def save_map(
    source_chat_id,
    source_msg_id,
    archive_chat_id,
    archive_msg_id,
    media_type,
    sender_name=None,
    sender_username=None,
    message_text=None,
    source_created_at=None,
):
    with _db_lock:
        conn.execute(
            """
            INSERT OR REPLACE INTO message_map
            (source_chat_id, source_msg_id, archive_chat_id, archive_msg_id, media_type,
             sender_name, sender_username, message_text, source_created_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_chat_id,
                source_msg_id,
                archive_chat_id,
                archive_msg_id,
                media_type,
                sender_name,
                sender_username,
                message_text,
                source_created_at,
                now_text(),
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO processed_messages
            (source_chat_id, source_msg_id, created_at)
            VALUES (?, ?, ?)
            """,
            (source_chat_id, source_msg_id, now_text()),
        )
        conn.commit()


def get_map_row(source_chat_id, source_msg_id):
    with _db_lock:
        row = conn.execute(
            """
            SELECT archive_msg_id, media_type, sender_name, sender_username, message_text, source_created_at
            FROM message_map
            WHERE source_chat_id = ? AND source_msg_id = ?
            """,
            (source_chat_id, source_msg_id),
        ).fetchone()
    if not row:
        return None
    return {
        "archive_msg_id": row[0],
        "media_type": row[1],
        "sender_name": row[2],
        "sender_username": row[3],
        "message_text": row[4],
        "source_created_at": row[5],
    }


def get_archive_msg_id(source_chat_id, source_msg_id):
    row = get_map_row(source_chat_id, source_msg_id)
    return row["archive_msg_id"] if row else None


def is_media_duplicate_supported(media_type):
    return media_type in ("photo", "video", "video_note")


async def media_hash_from_message(message, media_type):
    """Exact duplicate detector for photos/videos/video notes.
    It downloads only supported media and hashes bytes. This is the most reliable
    way to detect the same media even after it is uploaded again.
    """
    if not is_media_duplicate_supported(media_type):
        return None
    try:
        data = await asyncio.wait_for(client.download_media(message, bytes), timeout=90)
        if not data:
            return None
        return hashlib.sha256(data).hexdigest()
    except Exception as e:
        # Fallback by Telegram media id when byte download fails.
        try:
            if getattr(message, "photo", None):
                return f"photo_id:{message.photo.id}"
            if getattr(message, "document", None):
                return f"doc_id:{message.document.id}"
        except Exception:
            pass
        log_error("MEDIA HASH ERROR", e)
        return None


def get_media_fingerprint(media_hash):
    if not media_hash:
        return None
    with _db_lock:
        row = conn.execute(
            """
            SELECT media_hash, media_type, source_chat_id, source_msg_id, archive_msg_id,
                   sender_name, sender_username, first_seen_at, source_created_at
            FROM media_fingerprints
            WHERE media_hash = ?
            """,
            (media_hash,),
        ).fetchone()
    if not row:
        return None
    return {
        "media_hash": row[0],
        "media_type": row[1],
        "source_chat_id": row[2],
        "source_msg_id": row[3],
        "archive_msg_id": row[4],
        "sender_name": row[5],
        "sender_username": row[6],
        "first_seen_at": row[7],
        "source_created_at": row[8],
    }


def save_media_fingerprint(media_hash, media_type, source_chat_id, source_msg_id, archive_msg_id,
                           sender_name=None, sender_username=None, source_created_at=None):
    if not media_hash:
        return
    with _db_lock:
        conn.execute(
            """
            INSERT OR IGNORE INTO media_fingerprints
            (media_hash, media_type, source_chat_id, source_msg_id, archive_msg_id,
             sender_name, sender_username, first_seen_at, source_created_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                media_hash,
                media_type,
                source_chat_id,
                source_msg_id,
                archive_msg_id,
                sender_name,
                sender_username,
                source_created_at or now_text(),
                source_created_at,
                now_text(),
            ),
        )
        conn.commit()


def format_media_duplicate_text(first_row):
    sender = first_row.get("sender_name") or "Номаълум"
    username = first_row.get("sender_username")
    if username and f"@{username}" not in sender:
        sender = f"{sender} (@{username})"
    first_time = first_row.get("source_created_at") or first_row.get("first_seen_at") or "Номаълум"
    media_type = first_row.get("media_type") or "media"
    return (
        f"♻️ Бу {media_type} аввал ҳам ташланган.\n"
        f"🕒 Биринчи ташланган вақт: {first_time}\n"
        f"👤 Биринчи ташлаган: {sender}\n"
        f"🆔 Биринчи хабар ID: {first_row.get('source_msg_id')}"
    )


def is_processed(source_chat_id, source_msg_id):
    with _db_lock:
        row = conn.execute(
            "SELECT 1 FROM processed_messages WHERE source_chat_id = ? AND source_msg_id = ?",
            (source_chat_id, source_msg_id),
        ).fetchone()
    return row is not None




def claim_source_message(source_chat_id, source_msg_id):
    """Atomic anti-duplicate claim.
    Message is marked before queueing, so multiple handlers/workers cannot forward it twice.
    """
    with _db_lock:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO processed_messages
            (source_chat_id, source_msg_id, created_at)
            VALUES (?, ?, ?)
            """,
            (source_chat_id, source_msg_id, now_text()),
        )
        conn.commit()
        return cur.rowcount == 1


def release_source_message_claim(source_chat_id, source_msg_id):
    """Release claim only when forward failed before mapping was saved."""
    with _db_lock:
        has_map = conn.execute(
            "SELECT 1 FROM message_map WHERE source_chat_id = ? AND source_msg_id = ?",
            (source_chat_id, source_msg_id),
        ).fetchone()
        if not has_map:
            conn.execute(
                "DELETE FROM processed_messages WHERE source_chat_id = ? AND source_msg_id = ?",
                (source_chat_id, source_msg_id),
            )
            conn.commit()


def claim_audit_event(source_chat_id, source_msg_id, event_type, content_hash):
    """Prevents repeated edit/delete audit messages for the same exact event."""
    with _db_lock:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO audit_events
            (source_chat_id, source_msg_id, event_type, content_hash, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (source_chat_id, source_msg_id, event_type, content_hash, now_text()),
        )
        conn.commit()
        return cur.rowcount == 1


def short_hash(value):
    return hashlib.sha256(str(value).encode("utf-8", errors="ignore")).hexdigest()

def get_stats():
    with _db_lock:
        total_archived = conn.execute("SELECT COUNT(*) FROM message_map").fetchone()[0]
        total_errors = conn.execute("SELECT COUNT(*) FROM error_log").fetchone()[0]
        by_media = dict(conn.execute("SELECT COALESCE(media_type, 'unknown'), COUNT(*) FROM message_map GROUP BY media_type").fetchall())
        avg_perf = conn.execute("SELECT COALESCE(AVG(duration_ms), 0) FROM performance_log WHERE place = 'forward'").fetchone()[0]
    uptime_seconds = int(time.time() - start_time)
    return {
        "status": runtime_state.get("status"),
        "uptime_seconds": uptime_seconds,
        "queue_size": archive_queue.qsize() if archive_queue else 0,
        "workers": QUEUE_WORKERS,
        "source_group": SOURCE_GROUP,
        "archive_group": ARCHIVE_GROUP,
        "delete_sync_enabled": DELETE_SYNC_ENABLED,
        "time_zone": "Asia/Tashkent UTC+5",
        "server_time_tashkent": now_text(),
        "total_archived": total_archived,
        "duplicates_blocked": get_counter("duplicates_blocked"),
        "media_duplicates_detected": get_counter("media_duplicates_detected"),
        "queued_messages": get_counter("queued_messages"),
        "forward_errors": get_counter("forward_errors"),
        "floodwaits": get_counter("floodwaits"),
        "total_errors": total_errors,
        "media": by_media,
        "avg_forward_ms": int(avg_perf),
        "last_event_at": runtime_state.get("last_event_at"),
        "last_forward_at": runtime_state.get("last_forward_at"),
        "safe_sent_count": runtime_state.get("safe_sent_count"),
        "last_safe_pause_at": runtime_state.get("last_safe_pause_at"),
        "last_error": runtime_state.get("last_error"),
    }

# ================= RENDER KEEP-ALIVE / JSON API =================
class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def _send(self, status=200, body=b"OK", content_type="text/plain; charset=utf-8"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/health", "/ping"):
            self._send(200, b"Archive monitor is running")
        elif self.path in ("/live", "/stats"):
            body = json.dumps(get_stats(), ensure_ascii=False, indent=2).encode("utf-8")
            self._send(200, body, "application/json; charset=utf-8")
        else:
            self._send(404, b"Not found")

    def do_HEAD(self):
        self._send(200, b"")


def run_keep_alive_server():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"✅ KEEP ALIVE SERVER STARTED ON PORT {port}", flush=True)
    server.serve_forever()

# ================= SAFE TELEGRAM HELPERS =================
async def safe_call(place, func, *args, retries=5, **kwargs):
    for attempt in range(1, retries + 1):
        started = time.perf_counter()
        try:
            result = await func(*args, **kwargs)
            log_perf(place, started)
            return result
        except FloodWaitError as e:
            inc_counter("floodwaits", 1)
            wait_seconds = int(getattr(e, "seconds", 30)) + 2
            print(f"⏳ FLOODWAIT at {place}: waiting {wait_seconds}s", flush=True)
            await asyncio.sleep(wait_seconds)
        except (ConnectionError, TimeoutError, RPCError) as e:
            log_error(f"{place} attempt {attempt}/{retries}", e)
            if attempt == retries:
                raise
            await asyncio.sleep(min(5 * attempt, 45))
        except Exception as e:
            log_error(f"{place} attempt {attempt}/{retries}", e)
            if attempt == retries:
                raise
            await asyncio.sleep(min(5 * attempt, 45))


async def safe_telegram_pause(reason="send"):
    """Global anti-ban limiter: small random delay + 3s cooldown after every 20 Telegram sends."""
    global safe_sent_count
    async with safe_limit_lock:
        safe_sent_count += 1
        runtime_state["safe_sent_count"] = safe_sent_count

        delay = random.uniform(SEND_DELAY_MIN, SEND_DELAY_MAX)
        await asyncio.sleep(delay)

        if BURST_LIMIT > 0 and safe_sent_count % BURST_LIMIT == 0:
            runtime_state["last_safe_pause_at"] = now_text()
            print(
                f"🛡 SAFE MODE: {BURST_LIMIT} Telegram sends done → cooldown {BURST_PAUSE_SECONDS}s | reason={reason}",
                flush=True,
            )
            await asyncio.sleep(BURST_PAUSE_SECONDS)


def message_text_for_db(msg):
    text = msg.message or msg.text or msg.raw_text or ""
    if text:
        return text[:4000]
    if msg.media:
        return f"[{media_type_from_message(msg)} media/caption йўқ]"
    return "[матнсиз хабар]"


def sender_username_raw(user):
    return getattr(user, "username", None) if user else None


def format_deleted_original(row):
    if not row:
        return "⚠️ Ўчирилган хабар маълумоти database mapping’да топилмади. Бу хабар архив monitor ишламасидан олдин архивланган бўлиши мумкин."
    text = row.get("message_text") or "[матнсиз/media хабар]"
    sender = row.get("sender_name") or "Номаълум"
    if row.get("sender_username") and "@" not in sender:
        sender = f"{sender} (@{row.get('sender_username')})"
    source_created = row.get("source_created_at") or "Номаълум"
    media_type = row.get("media_type") or "unknown"
    return (
        f"📌 Ўчирилган хабар маълумоти:\n"
        f"👤 Ёзган: {sender}\n"
        f"🕒 Ёзилган вақт: {source_created}\n"
        f"📎 Тур: {media_type}\n\n"
        f"💬 Хабар:\n{text}"
    )


def sender_name_from_user(user):
    if not user:
        return "Номаълум"
    first = getattr(user, "first_name", None) or ""
    last = getattr(user, "last_name", None) or ""
    username = getattr(user, "username", None) or ""
    name = f"{first} {last}".strip() or "Номаълум"
    return f"{name} (@{username})" if username else name


async def find_dialog_by_id_or_title(group_id, title_hint=None):
    found_dialog = None
    async for dialog in client.iter_dialogs(limit=None):
        if dialog.id == group_id:
            found_dialog = dialog
            break
        if title_hint and (dialog.name or "").strip() == title_hint.strip():
            found_dialog = dialog
            break

    if not found_dialog:
        raise RuntimeError(f"Dialog топилмади: {group_id}")

    entity = found_dialog.entity
    input_entity = found_dialog.input_entity
    print(f"✅ Dialog топилди: {found_dialog.name} | dialog_id={found_dialog.id}", flush=True)
    print(f"   entity_type={type(entity).__name__} | input_type={type(input_entity).__name__}", flush=True)

    migrated_to = getattr(entity, "migrated_to", None)
    if migrated_to:
        print("🔁 Эски Chat supergroup/channel га migrate бўлган. Янги peer ишлатилади.", flush=True)
        migrated_entity = await safe_call("get migrated entity", client.get_entity, migrated_to)
        migrated_input_entity = await safe_call("get migrated input", client.get_input_entity, migrated_entity)
        print(
            f"✅ MIGRATED TARGET: {getattr(migrated_entity, 'title', 'unknown')} | "
            f"id={getattr(migrated_entity, 'id', None)} | input_type={type(migrated_input_entity).__name__}",
            flush=True,
        )
        return migrated_input_entity, migrated_entity, found_dialog

    return input_entity, entity, found_dialog


async def verify_can_send(archive_input):
    # Startup test message is intentionally disabled.
    # Old versions sent "Archive monitor PRO SAFE LIMIT test" into archive group on every deploy/reconnect.
    # That caused confusion and could trigger extra audit messages.
    print("✅ ARCHIVE PEER RESOLVED: startup test message disabled", flush=True)
    return True

# ================= ARCHIVE QUEUE / ASYNC WORKERS =================
async def archive_worker(worker_id, archive_input, archive_chat_id, source_chat_id):
    print(f"✅ Archive worker #{worker_id} started", flush=True)
    while True:
        event = await archive_queue.get()
        started = time.perf_counter()
        try:
            msg = event.message
            source_msg_id = msg.id
            media_type = media_type_from_message(msg)


            sender = await event.get_sender()
            sender_name = sender_name_from_user(sender)
            sender_username = sender_username_raw(sender)
            msg_text = message_text_for_db(msg)
            source_created_at = tg_time_text(getattr(msg, "date", None))

            media_hash = None
            first_media_row = None
            if is_media_duplicate_supported(media_type):
                media_hash = await media_hash_from_message(msg, media_type)
                first_media_row = get_media_fingerprint(media_hash) if media_hash else None

            await safe_telegram_pause("forward")
            forwarded = await safe_call("forward", client.forward_messages, archive_input, msg)
            forwarded_msg = forwarded[0] if isinstance(forwarded, list) else forwarded
            save_map(
                source_chat_id,
                source_msg_id,
                archive_chat_id,
                forwarded_msg.id,
                media_type,
                sender_name=sender_name,
                sender_username=sender_username,
                message_text=msg_text,
                source_created_at=source_created_at,
            )

            if media_hash and not first_media_row:
                save_media_fingerprint(
                    media_hash,
                    media_type,
                    source_chat_id,
                    source_msg_id,
                    forwarded_msg.id,
                    sender_name=sender_name,
                    sender_username=sender_username,
                    source_created_at=source_created_at,
                )
            elif first_media_row:
                duplicate_text = format_media_duplicate_text(first_media_row)
                await safe_telegram_pause("media_duplicate_audit")
                try:
                    await safe_call(
                        "media_duplicate_audit",
                        client.send_message,
                        archive_input,
                        duplicate_text,
                        reply_to=forwarded_msg.id,
                    )
                except Exception:
                    await safe_call("media_duplicate_audit_no_reply", client.send_message, archive_input, duplicate_text)
                inc_counter("media_duplicates_detected", 1)
                print(f"♻️ MEDIA DUPLICATE DETECTED: {source_msg_id} first={first_media_row.get('source_msg_id')}", flush=True)

            runtime_state["last_forward_at"] = now_text()
            inc_counter("archived", 1)
            print(f"✅ WORKER {worker_id}: FORWARDED {source_msg_id} -> {forwarded_msg.id} [{media_type}]", flush=True)

        except Exception as e:
            inc_counter("forward_errors", 1)
            try:
                release_source_message_claim(source_chat_id, source_msg_id)
            except Exception:
                pass
            log_error(f"WORKER {worker_id} FORWARD ERROR", e)
        finally:
            log_perf("worker_total", started)
            archive_queue.task_done()
            # Memory optimization: clean references after each task.
            del event
            if get_counter("archived") % 100 == 0:
                gc.collect()

# ================= MONITOR LOGIC =================
async def start_monitor_once():
    global archive_queue
    runtime_state["status"] = "connecting"
    await client.start()
    init_db()

    archive_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    source_input, source_entity, _ = await find_dialog_by_id_or_title(SOURCE_GROUP)
    archive_input, archive_entity, _ = await find_dialog_by_id_or_title(ARCHIVE_GROUP, title_hint=ARCHIVE_TITLE_HINT)

    source_chat_id = SOURCE_GROUP
    archive_chat_id = ARCHIVE_GROUP
    runtime_state["source"] = str(getattr(source_entity, "title", SOURCE_GROUP))
    runtime_state["archive"] = str(getattr(archive_entity, "title", ARCHIVE_GROUP))

    print("✅ Archive monitor PRO SAFE LIMIT NO DUPLICATE started", flush=True)
    print(f"✅ TASHKENT TIME NOW: {now_text()}", flush=True)
    print(f"✅ SOURCE input: {type(source_input).__name__}", flush=True)
    print(f"✅ ARCHIVE input: {type(archive_input).__name__}", flush=True)
    print("ℹ️ DELETE SYNC: OFF — асосий гуруҳдан ўчса ҳам архивдан ўчмайди", flush=True)
    print("ℹ️ MEDIA DUPLICATE: photo/video/video_note қайта ташланса, биринчи ташланган вақт reply қилинади", flush=True)
    print("ℹ️ TIME MODE: FORCE Asia/Tashkent UTC+5", flush=True)
    print(f"🛡 SAFE LIMIT: each send {SEND_DELAY_MIN}-{SEND_DELAY_MAX}s, every {BURST_LIMIT} sends cooldown {BURST_PAUSE_SECONDS}s", flush=True)

    await verify_can_send(archive_input)

    for i in range(QUEUE_WORKERS):
        asyncio.create_task(archive_worker(i + 1, archive_input, archive_chat_id, source_chat_id))

    async def enqueue_new_message(event):
        try:
            runtime_state["last_event_at"] = now_text()
            source_msg_id = event.message.id
            if not claim_source_message(source_chat_id, source_msg_id):
                inc_counter("duplicates_blocked", 1)
                print(f"♻️ DUPLICATE BEFORE QUEUE SKIPPED: {source_msg_id}", flush=True)
                return

            await archive_queue.put(event)
            inc_counter("queued_messages", 1)
            runtime_state["queue_size"] = archive_queue.qsize()
            print(f"📥 QUEUED: {source_msg_id} | queue={archive_queue.qsize()}", flush=True)
        except Exception as e:
            log_error("QUEUE ERROR", e)

    async def edited_message(event):
        started = time.perf_counter()
        try:
            original_id = event.message.id
            archive_message_id = get_archive_msg_id(source_chat_id, original_id)
            if not archive_message_id:
                print(f"EDIT SKIPPED, mapping not found: {original_id}", flush=True)
                return

            sender = await event.get_sender()
            sender_name = sender_name_from_user(sender)
            new_text = event.message.message or event.message.text or event.message.raw_text or ""
            if not new_text:
                new_text = "Матнсиз media/caption ўзгартирилган"

            audit_hash = short_hash(new_text)
            if not claim_audit_event(source_chat_id, original_id, "edit", audit_hash):
                print(f"♻️ DUPLICATE EDIT AUDIT SKIPPED: {original_id}", flush=True)
                return

            audit_text = (
                f"✏️ Маълумот ўзгартирилди\n"
                f"🕒 Вақт: {now_text()}\n"
                f"👤 Таҳрирлаган: {sender_name}\n\n"
                f"🆕 Янги маълумот:\n{new_text}"
            )
            await safe_telegram_pause("edit_audit")
            await safe_call("edit_audit", client.send_message, archive_input, audit_text, reply_to=archive_message_id)
            inc_counter("edit_audits", 1)
            print(f"✅ Edited audit sent: {original_id}", flush=True)
        except Exception as e:
            log_error("EDIT ERROR", e)
        finally:
            log_perf("edited_message", started)

    async def deleted_message(event):
        """
        Source group’dan xabar o‘chirilsa, archive group’dagi nusxa o‘chirilmaydi.
        Archive’dagi original xabarga faqat qisqa reply audit yuboriladi.
        """
        started = time.perf_counter()
        try:
            deleted_ids = list(getattr(event, "deleted_ids", []) or [])
            if not deleted_ids:
                return

            for deleted_id in deleted_ids:
                row = get_map_row(source_chat_id, deleted_id)

                reply_to = row.get("archive_msg_id") if row else None

                if row:
                    person_name = row.get("sender_name") or "Номаълум"
                    username = row.get("sender_username")
                    if username:
                        person_line = f"{person_name} (@{username})"
                    else:
                        person_line = person_name
                else:
                    person_line = "Номаълум"

                delete_hash = short_hash(f"delete:{deleted_id}")
                if not claim_audit_event(source_chat_id, deleted_id, "delete", delete_hash):
                    print(f"♻️ DUPLICATE DELETE AUDIT SKIPPED: {deleted_id}", flush=True)
                    continue

                # Qisqa professional reply alert. Archive nusxa o‘chirilmaydi.
                audit_text = (
                    "🗑 Маълумот ўчирилди\n"
                    f"🕒 Вақт: {now_text()}\n"
                    f"👤 Ўчирган: {person_line}"
                )

                await safe_telegram_pause("delete_audit")

                if reply_to:
                    try:
                        await safe_call(
                            "delete_audit_reply",
                            client.send_message,
                            archive_input,
                            audit_text,
                            reply_to=reply_to,
                        )
                    except Exception as e:
                        log_error("DELETE AUDIT REPLY ERROR, FALLBACK TO NORMAL MESSAGE", e)
                        await safe_call("delete_audit_no_reply", client.send_message, archive_input, audit_text)
                else:
                    await safe_call("delete_audit_no_map", client.send_message, archive_input, audit_text)

                inc_counter("delete_audits", 1)
                print(f"✅ Delete audit reply sent, archive copy kept: {deleted_id}", flush=True)

        except Exception as e:
            log_error("DELETE AUDIT ERROR", e)
        finally:
            log_perf("deleted_message", started)

    global _registered_handlers
    # Reconnect paytida eski handlerlar qolib ketmasin. Aks holda bitta edit/delete bir necha marta keladi.
    for handler, event_builder in list(_registered_handlers):
        try:
            client.remove_event_handler(handler, event_builder)
        except Exception:
            pass
    _registered_handlers = []

    new_builder = events.NewMessage(chats=source_input)
    edit_builder = events.MessageEdited(chats=source_input)
    delete_builder = events.MessageDeleted(chats=source_input)

    client.add_event_handler(enqueue_new_message, new_builder)
    client.add_event_handler(edited_message, edit_builder)
    client.add_event_handler(deleted_message, delete_builder)
    _registered_handlers.extend([
        (enqueue_new_message, new_builder),
        (edited_message, edit_builder),
        (deleted_message, delete_builder),
    ])

    # Delete-sync is OFF by design: source delete => archive copy stays, only audit alert is sent.
    runtime_state["status"] = "running"
    print("✅ Monitor active. SOURCE группага янги хабар ташлаб текширинг.", flush=True)
    await client.run_until_disconnected()

# ================= RECONNECT MANAGER =================
async def main_forever():
    threading.Thread(target=run_keep_alive_server, daemon=True).start()
    while True:
        try:
            await start_monitor_once()
        except FloodWaitError as e:
            inc_counter("floodwaits", 1)
            wait_seconds = int(getattr(e, "seconds", 60)) + 2
            print(f"⏳ MAIN FLOODWAIT: waiting {wait_seconds}s", flush=True)
            await asyncio.sleep(wait_seconds)
        except Exception as e:
            runtime_state["status"] = "reconnecting"
            log_error("MAIN CRASH - AUTO RECONNECT", e)
            traceback.print_exc()
            try:
                await client.disconnect()
            except Exception:
                pass
            gc.collect()
            await asyncio.sleep(15)
            print("🔁 Reconnecting archive monitor...", flush=True)

if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main_forever())
