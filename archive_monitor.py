import asyncio
import os
import sqlite3
import threading
import traceback
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage

# ================= CONFIG =================
api_id = 36784553
api_hash = "c463b506e987f1f82e211cef8c50f952"

# 1 ta yoki ko'p gruppa monitoring uchun.
# Hozir sizdagi ishlayotgan juftlik saqlab qolindi.
# Keyin yangi gruppa qo'shish uchun shu listga yangi dict qo'shiladi.
MONITOR_PAIRS = [
    {
        "name": "Техника",
        "source_group": -1003172289496,
        "archive_group": -5281572843,
        "archive_title_hint": "Техника Архив",
    },
]

SESSION_NAME = os.environ.get("SESSION_NAME", "archive_monitor")
DB_NAME = os.environ.get("DB_NAME", "archive_map.db")

client = TelegramClient(SESSION_NAME, api_id, api_hash)
_db_lock = threading.Lock()
_runtime_stats = {
    "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "forwarded": 0,
    "duplicates": 0,
    "edits": 0,
    "delete_audits": 0,
    "errors": 0,
}


# ================= RENDER KEEP-ALIVE HTTP SERVER =================
class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def _send(self, code=200, body=b"OK", content_type="text/plain; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/health", "/ping"):
            self._send(200, b"Archive monitor is running")
        elif self.path == "/stats":
            body = (
                "Archive monitor PRO v2\n"
                f"started_at={_runtime_stats['started_at']}\n"
                f"forwarded={_runtime_stats['forwarded']}\n"
                f"duplicates={_runtime_stats['duplicates']}\n"
                f"edits={_runtime_stats['edits']}\n"
                f"delete_audits={_runtime_stats['delete_audits']}\n"
                f"errors={_runtime_stats['errors']}\n"
            ).encode("utf-8")
            self._send(200, body)
        else:
            self._send(200, b"OK")

    def do_HEAD(self):
        self._send(200, b"")


def run_keep_alive_server():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"✅ KEEP ALIVE SERVER STARTED ON PORT {port}", flush=True)
    server.serve_forever()


threading.Thread(target=run_keep_alive_server, daemon=True).start()


# ================= SQLITE DATABASE =================
def db_connect():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


conn = db_connect()


def init_db():
    with _db_lock:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS message_map (
                source_chat_id INTEGER NOT NULL,
                source_msg_id INTEGER NOT NULL,
                archive_chat_id INTEGER NOT NULL,
                archive_msg_id INTEGER NOT NULL,
                pair_name TEXT,
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
                pair_name TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (source_chat_id, source_msg_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS delete_audit (
                source_chat_id INTEGER NOT NULL,
                source_msg_id INTEGER NOT NULL,
                archive_msg_id INTEGER,
                pair_name TEXT,
                created_at TEXT NOT NULL,
                status TEXT NOT NULL,
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
            CREATE TABLE IF NOT EXISTS archive_stats (
                day TEXT NOT NULL,
                pair_name TEXT NOT NULL,
                forwarded_count INTEGER NOT NULL DEFAULT 0,
                duplicate_count INTEGER NOT NULL DEFAULT 0,
                edit_count INTEGER NOT NULL DEFAULT 0,
                delete_audit_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (day, pair_name)
            )
            """
        )
        conn.commit()


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_text():
    return datetime.now().strftime("%Y-%m-%d")


def bump_stat(pair_name, column):
    allowed = {"forwarded_count", "duplicate_count", "edit_count", "delete_audit_count", "error_count"}
    if column not in allowed:
        return
    with _db_lock:
        conn.execute(
            """
            INSERT OR IGNORE INTO archive_stats (day, pair_name)
            VALUES (?, ?)
            """,
            (today_text(), pair_name),
        )
        conn.execute(
            f"UPDATE archive_stats SET {column} = {column} + 1 WHERE day = ? AND pair_name = ?",
            (today_text(), pair_name),
        )
        conn.commit()


def log_error(place, error, pair_name="system"):
    _runtime_stats["errors"] += 1
    bump_stat(pair_name, "error_count")
    text = repr(error)
    print(f"❌ {place}: {text}", flush=True)
    with _db_lock:
        conn.execute(
            "INSERT INTO error_log (created_at, place, error) VALUES (?, ?, ?)",
            (now_text(), place, text),
        )
        conn.commit()


def save_map(pair_name, source_chat_id, source_msg_id, archive_chat_id, archive_msg_id):
    with _db_lock:
        conn.execute(
            """
            INSERT OR REPLACE INTO message_map
            (source_chat_id, source_msg_id, archive_chat_id, archive_msg_id, pair_name, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source_chat_id, source_msg_id, archive_chat_id, archive_msg_id, pair_name, now_text()),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO processed_messages
            (source_chat_id, source_msg_id, pair_name, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (source_chat_id, source_msg_id, pair_name, now_text()),
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


def save_delete_audit(pair_name, source_chat_id, source_msg_id, archive_msg_id, status):
    with _db_lock:
        conn.execute(
            """
            INSERT OR REPLACE INTO delete_audit
            (source_chat_id, source_msg_id, archive_msg_id, pair_name, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source_chat_id, source_msg_id, archive_msg_id, pair_name, now_text(), status),
        )
        conn.commit()


# ================= SAFE TELEGRAM HELPERS =================
async def safe_call(place, func, *args, retries=3, pair_name="system", **kwargs):
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as e:
            wait_seconds = int(getattr(e, "seconds", 30)) + 2
            print(f"⏳ FLOODWAIT at {place}: waiting {wait_seconds}s", flush=True)
            await asyncio.sleep(wait_seconds)
        except (ConnectionError, TimeoutError, RPCError) as e:
            log_error(f"{place} attempt {attempt}/{retries}", e, pair_name=pair_name)
            if attempt == retries:
                raise
            await asyncio.sleep(min(5 * attempt, 30))
        except Exception as e:
            log_error(f"{place} attempt {attempt}/{retries}", e, pair_name=pair_name)
            if attempt == retries:
                raise
            await asyncio.sleep(min(5 * attempt, 30))


def sender_name_from_user(user):
    if not user:
        return "Номаълум"
    first = getattr(user, "first_name", None) or ""
    last = getattr(user, "last_name", None) or ""
    username = getattr(user, "username", None) or ""
    name = f"{first} {last}".strip() or "Номаълум"
    if username:
        name = f"{name} (@{username})"
    return name


async def find_dialog_by_id_or_title(group_id, title_hint=None, pair_name="system"):
    found_dialog = None

    async for dialog in client.iter_dialogs(limit=None):
        if dialog.id == group_id:
            found_dialog = dialog
            break
        if title_hint and (dialog.name or "").strip() == title_hint.strip():
            found_dialog = dialog
            break

    if not found_dialog:
        raise RuntimeError(f"Dialog topilmadi: {group_id}")

    entity = found_dialog.entity
    input_entity = found_dialog.input_entity

    print(f"✅ [{pair_name}] Dialog топилди: {found_dialog.name} | dialog_id={found_dialog.id}", flush=True)
    print(f"   entity_type={type(entity).__name__} | input_type={type(input_entity).__name__}", flush=True)

    migrated_to = getattr(entity, "migrated_to", None)
    if migrated_to:
        print(f"🔁 [{pair_name}] Эски Chat supergroup/channel га migrate бўлган. Янги peer ишлатилади.", flush=True)
        migrated_entity = await safe_call("get migrated entity", client.get_entity, migrated_to, pair_name=pair_name)
        migrated_input_entity = await safe_call("get migrated input", client.get_input_entity, migrated_entity, pair_name=pair_name)
        print(
            f"✅ [{pair_name}] MIGRATED TARGET: {getattr(migrated_entity, 'title', 'unknown')} | "
            f"id={getattr(migrated_entity, 'id', None)} | input_type={type(migrated_input_entity).__name__}",
            flush=True,
        )
        return migrated_input_entity, migrated_entity, found_dialog.id

    return input_entity, entity, found_dialog.id


async def verify_can_send(pair_name, archive_input):
    test_text = f"✅ Archive monitor PRO v2 test: {pair_name} archive peer ишлаяпти"
    msg = await safe_call("archive test send", client.send_message, archive_input, test_text, pair_name=pair_name)
    print(f"✅ [{pair_name}] ARCHIVE TEST SEND OK: msg_id={msg.id}", flush=True)
    try:
        await safe_call("archive test delete", client.delete_messages, archive_input, [msg.id], pair_name=pair_name)
        print(f"✅ [{pair_name}] ARCHIVE TEST MESSAGE DELETED", flush=True)
    except Exception as e:
        print(f"⚠️ [{pair_name}] Test хабарни ўчира олмадим, лекин юбориш ишлади: {e!r}", flush=True)
    return True


# ================= MONITOR LOGIC =================
async def check_deleted_messages(pair_name, source_input, source_chat_id, archive_input):
    while True:
        try:
            async for log_event in client.iter_admin_log(source_input, delete=True, limit=50):
                if not isinstance(log_event.action, ChannelAdminLogEventActionDeleteMessage):
                    continue

                deleted_msg = log_event.action.message
                source_msg_id = deleted_msg.id

                with _db_lock:
                    exists = conn.execute(
                        "SELECT 1 FROM delete_audit WHERE source_chat_id = ? AND source_msg_id = ?",
                        (source_chat_id, source_msg_id),
                    ).fetchone()
                if exists:
                    continue

                archive_message_id = get_archive_msg_id(source_chat_id, source_msg_id)
                if not archive_message_id:
                    print(f"[{pair_name}] DELETE SKIPPED, mapping not found: {source_msg_id}", flush=True)
                    save_delete_audit(pair_name, source_chat_id, source_msg_id, None, "mapping_not_found")
                    continue

                user_name = sender_name_from_user(log_event.user)
                await safe_call(
                    "delete audit send",
                    client.send_message,
                    archive_input,
                    f"🗑 Маълумот ўчирилди\n"
                    f"📂 Группа: {pair_name}\n"
                    f"🕒 Вақт: {now_text()}\n"
                    f"👤 Ўчирган: {user_name}",
                    reply_to=archive_message_id,
                    pair_name=pair_name,
                )
                save_delete_audit(pair_name, source_chat_id, source_msg_id, archive_message_id, "audit_sent")
                _runtime_stats["delete_audits"] += 1
                bump_stat(pair_name, "delete_audit_count")
                print(f"✅ [{pair_name}] Deleted audit sent: {source_msg_id}", flush=True)

        except FloodWaitError as e:
            wait_seconds = int(getattr(e, "seconds", 30)) + 2
            print(f"⏳ [{pair_name}] DELETE FLOODWAIT: waiting {wait_seconds}s", flush=True)
            await asyncio.sleep(wait_seconds)
        except Exception as e:
            log_error(f"[{pair_name}] DELETE LOOP ERROR", e, pair_name=pair_name)
            await asyncio.sleep(10)

        await asyncio.sleep(5)


async def setup_pair(pair):
    pair_name = pair["name"]
    source_input, source_entity, source_dialog_id = await find_dialog_by_id_or_title(
        pair["source_group"], pair_name=pair_name
    )
    archive_input, archive_entity, archive_dialog_id = await find_dialog_by_id_or_title(
        pair["archive_group"], title_hint=pair.get("archive_title_hint"), pair_name=pair_name
    )

    await verify_can_send(pair_name, archive_input)

    async def forward_message(event, pair_name=pair_name, source_chat_id=source_dialog_id, archive_chat_id=archive_dialog_id, archive_input=archive_input):
        try:
            source_msg_id = event.message.id
            if is_processed(source_chat_id, source_msg_id):
                _runtime_stats["duplicates"] += 1
                bump_stat(pair_name, "duplicate_count")
                print(f"♻️ [{pair_name}] DUPLICATE SKIPPED: {source_msg_id}", flush=True)
                return

            forwarded = await safe_call(
                "forward message",
                client.forward_messages,
                archive_input,
                event.message,
                pair_name=pair_name,
            )
            forwarded_msg = forwarded[0] if isinstance(forwarded, list) else forwarded
            save_map(pair_name, source_chat_id, source_msg_id, archive_chat_id, forwarded_msg.id)
            _runtime_stats["forwarded"] += 1
            bump_stat(pair_name, "forwarded_count")
            print(f"✅ [{pair_name}] FORWARDED AND MAPPED: {source_msg_id} -> {forwarded_msg.id}", flush=True)
        except Exception as e:
            log_error(f"[{pair_name}] FORWARD ERROR", e, pair_name=pair_name)

    async def edited_message(event, pair_name=pair_name, source_chat_id=source_dialog_id, archive_input=archive_input):
        try:
            original_id = event.message.id
            archive_message_id = get_archive_msg_id(source_chat_id, original_id)
            if not archive_message_id:
                print(f"[{pair_name}] EDIT SKIPPED, mapping not found: {original_id}", flush=True)
                return

            sender = await event.get_sender()
            sender_name = sender_name_from_user(sender)
            new_text = event.message.message or event.message.text or event.message.raw_text or ""
            if not new_text:
                new_text = "Матнсиз media/caption ўзгартирилган"

            audit_text = (
                f"✏️ Маълумот ўзгартирилди\n"
                f"📂 Группа: {pair_name}\n"
                f"🕒 Вақт: {now_text()}\n"
                f"👤 Таҳрирлаган: {sender_name}\n\n"
                f"🆕 Янги маълумот:\n{new_text}"
            )
            await safe_call("edit audit send", client.send_message, archive_input, audit_text, reply_to=archive_message_id, pair_name=pair_name)
            _runtime_stats["edits"] += 1
            bump_stat(pair_name, "edit_count")
            print(f"✅ [{pair_name}] Edited audit sent: {original_id}", flush=True)
        except Exception as e:
            log_error(f"[{pair_name}] EDIT ERROR", e, pair_name=pair_name)

    client.add_event_handler(forward_message, events.NewMessage(chats=source_input))
    client.add_event_handler(edited_message, events.MessageEdited(chats=source_input))
    asyncio.create_task(check_deleted_messages(pair_name, source_input, source_dialog_id, archive_input))

    print(f"✅ [{pair_name}] Monitor active", flush=True)


async def start_monitor_once():
    await client.start()
    init_db()

    print("✅ Archive monitor PRO v2 started", flush=True)
    print(f"✅ Monitoring pairs count: {len(MONITOR_PAIRS)}", flush=True)

    for pair in MONITOR_PAIRS:
        await setup_pair(pair)

    print("✅ Ҳамма monitoring pair active. SOURCE группага янги хабар ташлаб текширинг.", flush=True)
    await client.run_until_disconnected()


async def main_forever():
    while True:
        try:
            await start_monitor_once()
        except FloodWaitError as e:
            wait_seconds = int(getattr(e, "seconds", 60)) + 2
            print(f"⏳ MAIN FLOODWAIT: waiting {wait_seconds}s", flush=True)
            await asyncio.sleep(wait_seconds)
        except Exception as e:
            log_error("MAIN CRASH - AUTO RECONNECT", e)
            traceback.print_exc()
            try:
                await client.disconnect()
            except Exception:
                pass
            await asyncio.sleep(15)
            print("🔁 Reconnecting archive monitor...", flush=True)


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main_forever())
