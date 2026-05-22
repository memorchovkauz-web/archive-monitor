import asyncio
import sqlite3
from datetime import datetime

from telethon import TelegramClient, events
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage, Chat, Channel

api_id = 36784553
api_hash = "c463b506e987f1f82e211cef8c50f952"

SOURCE_GROUP = -1003172289496
ARCHIVE_GROUP = -5281572843

SESSION_NAME = "archive_monitor"
DB_NAME = "archive_map.db"

client = TelegramClient(SESSION_NAME, api_id, api_hash)

conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()
cursor.execute("""
CREATE TABLE IF NOT EXISTS message_map (
    source_msg_id INTEGER PRIMARY KEY,
    archive_msg_id INTEGER
)
""")
conn.commit()


def save_map(source_msg_id, archive_msg_id):
    cursor.execute(
        "INSERT OR REPLACE INTO message_map (source_msg_id, archive_msg_id) VALUES (?, ?)",
        (source_msg_id, archive_msg_id),
    )
    conn.commit()


def get_archive_msg_id(source_msg_id):
    cursor.execute("SELECT archive_msg_id FROM message_map WHERE source_msg_id = ?", (source_msg_id,))
    row = cursor.fetchone()
    return row[0] if row else None


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


async def find_dialog_by_id_or_title(group_id, title_hint=None):
    """
    Dialog topiladi. Agar eski oddiy Chat supergroup/channel ga migrate bo'lgan bo'lsa,
    migrated_to ichidagi yangi peer qaytariladi. Aynan sizdagi muammo shu joyda edi.
    """
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

    print(f"✅ Dialog топилди: {found_dialog.name} | dialog_id={found_dialog.id}")
    print(f"   entity_type={type(entity).__name__} | input_type={type(input_entity).__name__}")

    migrated_to = getattr(entity, "migrated_to", None)
    if migrated_to:
        print("🔁 Бу эски Chat supergroup/channel га migrate бўлган. Янги peer ишлатилади.")
        migrated_input = migrated_to
        migrated_entity = await client.get_entity(migrated_input)
        migrated_input_entity = await client.get_input_entity(migrated_entity)
        print(
            f"✅ MIGRATED TARGET: {getattr(migrated_entity, 'title', 'unknown')} | "
            f"id={getattr(migrated_entity, 'id', None)} | input_type={type(migrated_input_entity).__name__}"
        )
        return migrated_input_entity, migrated_entity, found_dialog

    return input_entity, entity, found_dialog


async def verify_can_send(archive_input):
    """Archive peer'ga test xabar yuborib, keyin o'chirishga harakat qiladi."""
    test_text = "✅ Archive monitor test: archive peer ишлаяпти"
    msg = await client.send_message(archive_input, test_text)
    print(f"✅ ARCHIVE TEST SEND OK: msg_id={msg.id}")
    try:
        await client.delete_messages(archive_input, [msg.id])
        print("✅ ARCHIVE TEST MESSAGE DELETED")
    except Exception as e:
        print(f"⚠️ Test хабарни ўчира олмадим, лекин юбориш ишлади: {e!r}")
    return True


last_deleted = set()


async def check_deleted_messages(source_input, archive_input):
    while True:
        try:
            async for log_event in client.iter_admin_log(source_input, delete=True, limit=50):
                if not isinstance(log_event.action, ChannelAdminLogEventActionDeleteMessage):
                    continue

                deleted_msg = log_event.action.message
                if deleted_msg.id in last_deleted:
                    continue
                last_deleted.add(deleted_msg.id)

                archive_message_id = get_archive_msg_id(deleted_msg.id)
                if not archive_message_id:
                    print(f"DELETE SKIPPED, mapping not found: {deleted_msg.id}")
                    continue

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                user_name = sender_name_from_user(log_event.user)

                await client.send_message(
                    archive_input,
                    f"🗑 Маълумот ўчирилди\n"
                    f"🕒 Вақт: {now}\n"
                    f"👤 Ўчирган: {user_name}",
                    reply_to=archive_message_id,
                )
                print(f"✅ Deleted audit sent: {deleted_msg.id}")

        except Exception as e:
            print(f"DELETE ERROR: {e!r}")

        await asyncio.sleep(5)


async def main():
    await client.start()

    # SOURCE аниқ топилади
    source_input, source_entity, _ = await find_dialog_by_id_or_title(SOURCE_GROUP)

    # ARCHIVE: аввал ID орқали, агар эски Chat бўлса migrated_to орқали тўғрилайди
    archive_input, archive_entity, _ = await find_dialog_by_id_or_title(ARCHIVE_GROUP, title_hint="Техника Архив")

    print("✅ Archive monitor started")
    print(f"✅ SOURCE input: {type(source_input).__name__}")
    print(f"✅ ARCHIVE input: {type(archive_input).__name__}")

    try:
        await verify_can_send(archive_input)
    except Exception as e:
        print(f"❌ ARCHIVE TEST SEND ERROR: {e!r}")
        print("❌ Бу ерда код тўхтайди, чунки архивга юборолмаса кейин маълумот йўқолиши мумкин.")
        print("ℹ️ Агар 'migrated_to' чиқмаган бўлса, Telegramда 'Техника Архив'ни supergroup қилиб қайта очиш керак.")
        return

    async def forward_message(event):
        try:
            forwarded = await client.forward_messages(archive_input, event.message)
            forwarded_msg = forwarded[0] if isinstance(forwarded, list) else forwarded
            save_map(event.message.id, forwarded_msg.id)
            print(f"✅ FORWARDED AND MAPPED: {event.message.id} -> {forwarded_msg.id}")
        except Exception as e:
            print(f"❌ FORWARD ERROR: {e!r}")

    async def edited_message(event):
        try:
            original_id = event.message.id
            archive_message_id = get_archive_msg_id(original_id)
            if not archive_message_id:
                print(f"EDIT SKIPPED, mapping not found: {original_id}")
                return

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sender = await event.get_sender()
            sender_name = sender_name_from_user(sender)
            new_text = event.message.message or event.message.text or event.message.raw_text or ""
            if not new_text:
                new_text = "Матнсиз media/caption ўзгартирилган"

            audit_text = (
                f"✏️ Маълумот ўзгартирилди\n"
                f"🕒 Вақт: {now}\n"
                f"👤 Таҳрирлаган: {sender_name}\n\n"
                f"🆕 Янги маълумот:\n{new_text}"
            )
            await client.send_message(archive_input, audit_text, reply_to=archive_message_id)
            print(f"✅ Edited audit sent: {original_id}")
        except Exception as e:
            print(f"EDIT ERROR: {e!r}")

    client.add_event_handler(forward_message, events.NewMessage(chats=source_input))
    client.add_event_handler(edited_message, events.MessageEdited(chats=source_input))
    asyncio.create_task(check_deleted_messages(source_input, archive_input))

    print("✅ Monitor active. SOURCE группага янги хабар ташлаб текширинг.")
    await client.run_until_disconnected()


if __name__ == "__main__":
    with client:
        client.loop.run_until_complete(main())
