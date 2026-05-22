import asyncio
import gc
import json
import os
import sqlite3
import threading
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo
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
LOCAL_TZ = ZoneInfo(os.environ.get("LOCAL_TZ", "Asia/Tashkent"))

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
}

# ================= SQLITE DATABASE =================
def db_connect():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn

conn = db_connect()


def now_text():
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


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

        # Advanced indexes for speed on big archives.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_map_archive ON message_map (archive_chat_id, archive_msg_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_map_created ON message_map (created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_processed_created ON processed_messages (created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_error_created ON error_log (created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_perf_created ON performance_log (created_at)")
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


def save_map(source_chat_id, source_msg_id, archive_chat_id, archive_msg_id, media_type):
    with _db_lock:
        conn.execute(
            """
            INSERT OR REPLACE INTO message_map
            (source_chat_id, source_msg_id, archive_chat_id, archive_msg_id, media_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source_chat_id, source_msg_id, archive_chat_id, archive_msg_id, media_type, now_text()),
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


def get_archive_msg_id(source_chat_id, source_msg_id):
    with _db_lock:
        row = conn.execute(
            "SELECT archive_msg_id FROM message_map WHERE source_chat_id = ? AND source_msg_id = ?",
            (source_chat_id, source_msg_id),
        ).fetchone()
    return row[0] if row else None


def is_processed(source_chat_id, source_msg_id):
    with _db_lock:
        row = conn.execute(
            "SELECT 1 FROM processed_messages WHERE source_chat_id = ? AND source_msg_id = ?",
            (source_chat_id, source_msg_id),
        ).fetchone()
    return row is not None


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
        "total_archived": total_archived,
        "duplicates_blocked": get_counter("duplicates_blocked"),
        "queued_messages": get_counter("queued_messages"),
        "forward_errors": get_counter("forward_errors"),
        "floodwaits": get_counter("floodwaits"),
        "total_errors": total_errors,
        "media": by_media,
        "avg_forward_ms": int(avg_perf),
        "last_event_at": runtime_state.get("last_event_at"),
        "last_forward_at": runtime_state.get("last_forward_at"),
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
    test_text = "✅ Archive monitor stability test: archive peer ишлаяпти"
    msg = await safe_call("archive_test_send", client.send_message, archive_input, test_text)
    print(f"✅ ARCHIVE TEST SEND OK: msg_id={msg.id}", flush=True)
    try:
        await safe_call("archive_test_delete", client.delete_messages, archive_input, [msg.id])
        print("✅ ARCHIVE TEST MESSAGE DELETED", flush=True)
    except Exception as e:
        print(f"⚠️ Test хабарни ўчира олмадим, лекин юбориш ишлади: {e!r}", flush=True)
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

            if is_processed(source_chat_id, source_msg_id):
                inc_counter("duplicates_blocked", 1)
                print(f"♻️ DUPLICATE SKIPPED: {source_msg_id}", flush=True)
                continue

            forwarded = await safe_call("forward", client.forward_messages, archive_input, msg)
            forwarded_msg = forwarded[0] if isinstance(forwarded, list) else forwarded
            save_map(source_chat_id, source_msg_id, archive_chat_id, forwarded_msg.id, media_type)
            runtime_state["last_forward_at"] = now_text()
            inc_counter("archived", 1)
            print(f"✅ WORKER {worker_id}: FORWARDED {source_msg_id} -> {forwarded_msg.id} [{media_type}]", flush=True)

        except Exception as e:
            inc_counter("forward_errors", 1)
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

    print("✅ Archive monitor STABILITY ONLY started", flush=True)
    print(f"✅ SOURCE input: {type(source_input).__name__}", flush=True)
    print(f"✅ ARCHIVE input: {type(archive_input).__name__}", flush=True)
    print("ℹ️ DELETE SYNC: OFF — асосий гуруҳдан ўчса ҳам архивдан ўчмайди", flush=True)

    await verify_can_send(archive_input)

    for i in range(QUEUE_WORKERS):
        asyncio.create_task(archive_worker(i + 1, archive_input, archive_chat_id, source_chat_id))

    async def enqueue_new_message(event):
        try:
            runtime_state["last_event_at"] = now_text()
            source_msg_id = event.message.id
            if is_processed(source_chat_id, source_msg_id):
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

            audit_text = (
                f"✏️ Маълумот ўзгартирилди\n"
                f"🕒 Вақт: {now_text()}\n"
                f"👤 Таҳрирлаган: {sender_name}\n\n"
                f"🆕 Янги маълумот:\n{new_text}"
            )
            await safe_call("edit_audit", client.send_message, archive_input, audit_text, reply_to=archive_message_id)
            inc_counter("edit_audits", 1)
            print(f"✅ Edited audit sent: {original_id}", flush=True)
        except Exception as e:
            log_error("EDIT ERROR", e)
        finally:
            log_perf("edited_message", started)

    client.add_event_handler(enqueue_new_message, events.NewMessage(chats=source_input))
    client.add_event_handler(edited_message, events.MessageEdited(chats=source_input))

    # No delete-sync handler is registered by design.
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
