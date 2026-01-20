import os
import re
import asyncio
import sys
import json
import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Set, Optional, Tuple

from dotenv import load_dotenv

load_dotenv("/opt/tg_leads/.env")

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from telethon import TelegramClient
from telethon.tl.types import User as TgUser

from tg_to_sheets import (
    update_google_sheet,
    acquire_lock,
    release_lock,
    add_exclusion_entry,
    normalize_username
)

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_FILE = os.environ["SESSION_FILE"]
SESSION_LOCK = os.environ.get("TELETHON_SESSION_LOCK", f"{SESSION_FILE}.lock")
LOCK_PATH = os.environ.get("LOCK_PATH", "/opt/tg_leads/.update.lock")
EXPORT_LOCK_PATH = os.environ.get("EXPORT_LOCK_PATH", "/opt/tg_leads/.export.lock")
EXPORT_DIR = os.environ.get("EXPORT_DIR", "/opt/tg_leads/exports")
EXPORT_DAYS = int(os.environ.get("EXPORT_DAYS", "90"))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
WAITING_FOR_DATE: Set[int] = set()
WAITING_FOR_EXCLUDE: Set[int] = set()
AUTO_REPLY_PROCESS: Optional[subprocess.Popen] = None

AUTO_REPLY_PATH = os.environ.get("AUTO_REPLY_PATH", "auto_reply.py")
AUTO_REPLY_CMD = os.environ.get("AUTO_REPLY_CMD")
AUTO_REPLY_STATUS_PATH = os.environ.get("AUTO_REPLY_STATUS_PATH", "/opt/tg_leads/.auto_reply.status")


def kb_main():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📄 Оновити таблицю", callback_data="update"))
    kb.add(types.InlineKeyboardButton("📅 Історія за датою", callback_data="update_by_date"))
    kb.add(types.InlineKeyboardButton("🚫 Виключити з таблиці", callback_data="exclude_user"))
    kb.add(types.InlineKeyboardButton("▶️ Старт авто", callback_data="auto_start"))
    kb.add(types.InlineKeyboardButton("⏹ Стоп авто", callback_data="auto_stop"))
    kb.add(types.InlineKeyboardButton("📊 Статус авто", callback_data="auto_status"))
    kb.add(types.InlineKeyboardButton("🧠 Експорт чатів (3 міс.)", callback_data="export_chats"))
    return kb


def auto_reply_running() -> bool:
    return AUTO_REPLY_PROCESS is not None and AUTO_REPLY_PROCESS.poll() is None


def start_auto_reply() -> Tuple[bool, str]:
    global AUTO_REPLY_PROCESS
    if auto_reply_running():
        return False, "Автовідповідач вже запущено"

    if AUTO_REPLY_CMD:
        cmd = AUTO_REPLY_CMD.split()
    else:
        cmd = [sys.executable, AUTO_REPLY_PATH]
    try:
        AUTO_REPLY_PROCESS = subprocess.Popen(cmd)
        return True, "✅ Автовідповідач запущено"
    except Exception:
        AUTO_REPLY_PROCESS = None
        return False, "❌ Не вдалося запустити автовідповідач"


def stop_auto_reply() -> Tuple[bool, str]:
    global AUTO_REPLY_PROCESS
    if not auto_reply_running():
        AUTO_REPLY_PROCESS = None
        return False, "Автовідповідач не запущено"
    try:
        AUTO_REPLY_PROCESS.terminate()
        AUTO_REPLY_PROCESS.wait(timeout=5)
        AUTO_REPLY_PROCESS = None
        return True, "⏹ Автовідповідач зупинено"
    except Exception:
        return False, "❌ Не вдалося зупинити автовідповідач"


def read_auto_status() -> str:
    running = auto_reply_running()
    if not os.path.exists(AUTO_REPLY_STATUS_PATH):
        return "📊 Автовідповідач: " + ("працює" if running else "зупинено") + "\nДані про останню відправку відсутні"
    try:
        with open(AUTO_REPLY_STATUS_PATH, "r") as f:
            data = json.load(f)
    except Exception:
        return "📊 Автовідповідач: " + ("працює" if running else "зупинено") + "\nНе вдалося прочитати статус"

    last_at = data.get("last_sent_at", "—")
    peer_id = data.get("peer_id", "—")
    username = data.get("username", "")
    name = data.get("name", "")
    who = (f"@{username}" if username else "") or name or str(peer_id)
    preview = data.get("text_preview", "")
    return (
        "📊 Автовідповідач: "
        + ("працює" if running else "зупинено")
        + f"\nОстання відправка: {last_at}\nКому: {who}\nPeer ID: {peer_id}\nТекст: {preview}"
    )


def normalize_message_text(text: str) -> str:
    return " ".join((text or "").split())


async def export_recent_chats() -> Tuple[Optional[str], Optional[str]]:
    if not acquire_lock(EXPORT_LOCK_PATH, ttl_sec=1800):
        return None, "⏳ Експорт уже виконується. Спробуйте пізніше."
    if not acquire_lock(SESSION_LOCK, ttl_sec=300):
        release_lock(EXPORT_LOCK_PATH)
        return None, "⏳ Телеграм-сесія зайнята. Зупиніть авто і спробуйте ще раз."
    os.makedirs(EXPORT_DIR, exist_ok=True)
    tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
    cutoff = datetime.now(tz) - timedelta(days=EXPORT_DAYS)
    stamp = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(EXPORT_DIR, f"chats_export_{stamp}.txt")

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    try:
        await client.start()
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"Export generated: {datetime.now(tz).isoformat(timespec='seconds')}\n")
            f.write(f"Period: last {EXPORT_DAYS} days\n\n")
            async for dialog in client.iter_dialogs():
                if not dialog.is_user:
                    continue
                entity = dialog.entity
                if isinstance(entity, TgUser) and getattr(entity, "bot", False):
                    continue
                messages = []
                async for m in client.iter_messages(entity):
                    if not m.message:
                        continue
                    msg_dt = m.date.astimezone(tz) if m.date else None
                    if msg_dt and msg_dt < cutoff:
                        break
                    messages.append(m)
                if not messages:
                    continue
                name_parts = [
                    getattr(entity, "first_name", "") or "",
                    getattr(entity, "last_name", "") or "",
                ]
                name = " ".join(p for p in name_parts if p).strip() or (dialog.name or "")
                username = getattr(entity, "username", "") or ""
                header = f"=== CHAT: {name} {('@' + username) if username else ''} (id {entity.id}) ===\n"
                f.write(header)
                for m in reversed(messages):
                    msg_dt = m.date.astimezone(tz) if m.date else None
                    ts = msg_dt.strftime("%Y-%m-%d %H:%M") if msg_dt else "unknown time"
                    sender = "me" if m.out else "candidate"
                    text = normalize_message_text(m.message)
                    f.write(f"{ts} [{sender}]: {text}\n")
                f.write("\n")
        return out_path, None
    except Exception:
        return None, "❌ Не вдалося сформувати експорт."
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        release_lock(SESSION_LOCK)
        release_lock(EXPORT_LOCK_PATH)


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer("Готово 👇", reply_markup=kb_main())


@dp.callback_query_handler(lambda c: c.data == "update")
async def cb_update(call: types.CallbackQuery):
    if not acquire_lock(LOCK_PATH, ttl_sec=300):
        await call.answer("⏳ Вже оновлюється…", show_alert=True)
        return

    await call.answer("⏳ Оновлюю…")

    was_running = auto_reply_running()
    if was_running:
        stop_auto_reply()

    try:
        tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
        today = datetime.now(tz).date()
        sheet_title = today.strftime("%d.%m.%y")
        n, msg = await update_google_sheet(
            target_date=today,
            worksheet_override=sheet_title,
            replace_existing=True
        )
        if msg != "OK":
            await call.message.reply(msg)
        else:
            await call.message.reply(f"✅ Таблицю оновлено\nЛист: {sheet_title}\nДодано: {n}")
    except Exception:
        await call.message.reply("❌ Помилка оновлення")
    finally:
        if was_running:
            start_auto_reply()
        release_lock(LOCK_PATH)


def parse_date(text: str):
    cleaned = (text or "").strip()
    for fmt in ("%d.%m.%y", "%d.%m.%Y"):
        try:
            return cleaned, datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None, None


@dp.callback_query_handler(lambda c: c.data == "update_by_date")
async def cb_update_by_date(call: types.CallbackQuery):
    WAITING_FOR_EXCLUDE.discard(call.from_user.id)
    WAITING_FOR_DATE.add(call.from_user.id)
    await call.answer()
    await call.message.reply("Введіть дату у форматі ДД.ММ.РР або ДД.ММ.РРРР (наприклад, 19.08.25)")


def extract_exclusion_target(message: types.Message) -> Tuple[Optional[int], Optional[str]]:
    if message.forward_from:
        return message.forward_from.id, message.forward_from.username

    text = (message.text or "").strip()
    if not text:
        return None, None

    tg_id_match = re.search(r"tg://user\\?id=(\\d+)", text)
    if tg_id_match:
        return int(tg_id_match.group(1)), None

    tme_match = re.search(r"t\\.me/([A-Za-z0-9_]{5,})", text)
    if tme_match:
        return None, tme_match.group(1)

    at_match = re.search(r"@([A-Za-z0-9_]{5,})", text)
    if at_match:
        return None, at_match.group(1)

    id_match = re.search(r"\\b\\d{5,}\\b", text)
    if id_match:
        return int(id_match.group(0)), None

    return None, None


@dp.callback_query_handler(lambda c: c.data == "exclude_user")
async def cb_exclude_user(call: types.CallbackQuery):
    WAITING_FOR_DATE.discard(call.from_user.id)
    WAITING_FOR_EXCLUDE.add(call.from_user.id)
    await call.answer()
    await call.message.reply(
        "Надішліть username, user id, посилання t.me, tg://user?id або переслане повідомлення від користувача"
    )


@dp.callback_query_handler(lambda c: c.data == "auto_start")
async def cb_auto_start(call: types.CallbackQuery):
    ok, msg = start_auto_reply()
    await call.answer()
    await call.message.reply(msg)


@dp.callback_query_handler(lambda c: c.data == "auto_stop")
async def cb_auto_stop(call: types.CallbackQuery):
    ok, msg = stop_auto_reply()
    await call.answer()
    await call.message.reply(msg)


@dp.callback_query_handler(lambda c: c.data == "auto_status")
async def cb_auto_status(call: types.CallbackQuery):
    msg = read_auto_status()
    await call.answer()
    await call.message.reply(msg)


@dp.callback_query_handler(lambda c: c.data == "export_chats")
async def cb_export_chats(call: types.CallbackQuery):
    await call.answer()
    await call.message.reply("⏳ Готую експорт чатів за 3 місяці…")
    path, err = await export_recent_chats()
    if err:
        await call.message.reply(err)
        return
    try:
        await call.message.reply_document(types.InputFile(path), caption="✅ Експорт готовий")
    except Exception:
        await call.message.reply("❌ Не вдалося надіслати файл експорту")


@dp.message_handler(lambda m: m.from_user.id in WAITING_FOR_EXCLUDE)
async def handle_exclude_input(message: types.Message):
    peer_id, username = extract_exclusion_target(message)
    if peer_id is None and not username:
        await message.reply("Не зміг розпізнати користувача. Надішліть @username, id або переслане повідомлення.")
        return

    WAITING_FOR_EXCLUDE.discard(message.from_user.id)
    added_by = str(message.from_user.id)
    norm_username = normalize_username(username)
    ok, _ = add_exclusion_entry(peer_id, norm_username, added_by, source="manual")
    if ok:
        who = f"id={peer_id}" if peer_id is not None else f"@{norm_username}"
        await message.reply(f"✅ Додано у виключення: {who}")
    else:
        await message.reply("ℹ️ Користувач вже у списку виключень")


@dp.message_handler(lambda m: m.from_user.id in WAITING_FOR_DATE)
async def handle_date_input(message: types.Message):
    original_text, target_date = parse_date(message.text)
    if not target_date:
        await message.reply("Невірний формат дати. Спробуйте ще раз у форматі 19.08.25 або 19.08.2025")
        return

    WAITING_FOR_DATE.discard(message.from_user.id)
    year_part = original_text.split(".")[-1] if original_text else ""
    sheet_title = target_date.strftime("%d.%m.%Y") if len(year_part) == 4 else target_date.strftime("%d.%m.%y")

    if not acquire_lock(LOCK_PATH, ttl_sec=300):
        await message.reply("⏳ Оновлення вже виконується. Спробуйте пізніше.")
        return

    await message.reply(f"⏳ Формую лист \"{sheet_title}\"…")
    was_running = auto_reply_running()
    if was_running:
        stop_auto_reply()
    try:
        n, msg = await update_google_sheet(
            target_date=target_date,
            worksheet_override=sheet_title,
            replace_existing=True
        )
        if msg != "OK":
            await message.answer(msg)
        else:
            await message.answer(f"✅ Лист \"{sheet_title}\" оновлено\nДодано: {n}")
    except Exception:
        await message.answer("❌ Помилка оновлення за датою")
    finally:
        if was_running:
            start_auto_reply()
        release_lock(LOCK_PATH)


if __name__ == "__main__":
    async def scheduled_daily_update():
        tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
        while True:
            now = datetime.now(tz)
            target = now.replace(hour=23, minute=50, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())

            if not acquire_lock(LOCK_PATH, ttl_sec=300):
                continue
            was_running = auto_reply_running()
            if was_running:
                stop_auto_reply()
            try:
                today = datetime.now(tz).date()
                sheet_title = today.strftime("%d.%m.%y")
                await update_google_sheet(
                    target_date=today,
                    worksheet_override=sheet_title,
                    replace_existing=True
                )
            finally:
                if was_running:
                    start_auto_reply()
                release_lock(LOCK_PATH)

    async def on_startup(_):
        asyncio.create_task(scheduled_daily_update())

    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
