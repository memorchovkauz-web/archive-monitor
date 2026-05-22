import asyncio
import sqlite3
from datetime import datetime

from telethon import TelegramClient, events
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage

api_id = 36784553
api_hash = "c463b506e987f1f82e211cef8c50f952"

SOURCE_GROUP = -1003172289496
ARCHIVE_GROUP = -5281572843

client = TelegramClient("archive_monitor", api_id, api_hash)

conn = sqlite3.connect("archive_map.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS message_map (
    source_msg_id INTEGER PRIMARY KEY,
    archive_msg_id INTEGER
)
""")
conn.commit()


def save_map(source_msg_id, archive_msg_id):
    cursor.execute("""
        INSERT OR REPLACE INTO message_map (source_msg_id, archive_msg_id)
        VALUES (?, ?)
    """, (source_msg_id, archive_msg_id))
    conn.commit()


def get_archive_msg_id(source_msg_id):
    cursor.execute("""
        SELECT archive_msg_id FROM message_map
        WHERE source_msg_id = ?
    """, (source_msg_id,))
    row = cursor.fetchone()
    return row[0] if row else None


source_entity = None
archive_entity = None


async def safe_forward(message):
    global archive_entity
    forwarded = await client.forward_messages(archive_entity, message)
    return forwarded[0] if isinstance(forwarded, list) else forwarded


@client.on(events.NewMessage(chats=SOURCE_GROUP))
async def forward_message(event):
    try:
        print(f"📩 NEW MESSAGE caught: {event.message.id}")

        forwarded_msg = await safe_forward(event.message)
        save_map(event.message.id, forwarded_msg.id)

        print(f"✅ FORWARDED AND MAPPED: {event.message.id} -> {forwarded_msg.id}")

    except Exception as e:
        print("❌ FORWARD ERROR:", repr(e))


@client.on(events.MessageEdited(chats=SOURCE_GROUP))
async def edited_message(event):
    try:
        original_id = event.message.id
        archive_message_id = get_archive_msg_id(original_id)

        if not archive_message_id:
            print(f"EDIT SKIPPED, mapping not found: {original_id}")
            return

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        sender = await event.get_sender()
        first = getattr(sender, "first_name", "") or ""
        last = getattr(sender, "last_name", "") or ""
        username = getattr(sender, "username", "") or ""

        sender_name = f"{first} {last}".strip() or "Номаълум"

        if username:
            sender_name = f"{sender_name} (@{username})"

        new_text = (
            event.message.message
            or event.message.text
            or event.message.raw_text
            or ""
        )

        if not new_text:
            new_text = "Матнсиз media/caption ўзгартирилган"

        audit_text = (
            f"✏️ Маълумот ўзгартирилди\n"
            f"🕒 Вақт: {now}\n"
            f"👤 Таҳрирлаган: {sender_name}\n\n"
            f"🆕 Янги маълумот:\n{new_text}"
        )

        await client.send_message(archive_entity, audit_text, reply_to=archive_message_id)
        print(f"✅ Edited audit sent: {original_id}")

    except Exception as e:
        print("❌ EDIT ERROR:", repr(e))


last_deleted = set()


async def check_deleted_messages():
    while True:
        try:
            async for event in client.iter_admin_log(source_entity, delete=True, limit=50):
                if not isinstance(event.action, ChannelAdminLogEventActionDeleteMessage):
                    continue

                deleted_msg = event.action.message

                if deleted_msg.id in last_deleted:
                    continue

                last_deleted.add(deleted_msg.id)
                archive_message_id = get_archive_msg_id(deleted_msg.id)

                if not archive_message_id:
                    print(f"DELETE SKIPPED, mapping not found: {deleted_msg.id}")
                    continue

                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                user_name = "Номаълум"

                if event.user:
                    first = getattr(event.user, "first_name", "") or ""
                    last = getattr(event.user, "last_name", "") or ""
                    username = getattr(event.user, "username", "") or ""
                    user_name = f"{first} {last}".strip() or "Номаълум"
                    if username:
                        user_name = f"{user_name} (@{username})".strip()

                await client.send_message(
                    archive_entity,
                    f"🗑 Маълумот ўчирилди\n"
                    f"🕒 Вақт: {now}\n"
                    f"👤 Ўчирган: {user_name}",
                    reply_to=archive_message_id
                )

                print(f"✅ Deleted audit sent: {deleted_msg.id}")

        except Exception as e:
            print("❌ DELETE ERROR:", repr(e))

        await asyncio.sleep(5)


async def main():
    global source_entity, archive_entity

    await client.start()

    source_entity = await client.get_input_entity(SOURCE_GROUP)
    archive_entity = await client.get_input_entity(ARCHIVE_GROUP)

    print("✅ Archive monitor started")
    print(f"✅ SOURCE_GROUP топилди: {SOURCE_GROUP}")
    print(f"✅ ARCHIVE_GROUP топилди: {ARCHIVE_GROUP}")

    # Startup test: if this message reaches archive group, ARCHIVE_GROUP is correct.
    await client.send_message(
        archive_entity,
        "✅ Archive monitor test: архив группага уланиш ишлаяпти. Энди SOURCE группага янги хабар ташланг."
    )
    print("✅ Startup test message sent to archive group")

    client.loop.create_task(check_deleted_messages())
    await client.run_until_disconnected()


if __name__ == "__main__":
    client.loop.run_until_complete(main())
