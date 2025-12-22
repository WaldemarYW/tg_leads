import os
import re
import asyncio
import sys
import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Set, Optional, Tuple

from dotenv import load_dotenv

load_dotenv("/opt/tg_leads/.env")

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

from tg_to_sheets import (
    update_google_sheet,
    acquire_lock,
    release_lock,
    add_exclusion_entry,
    normalize_username
)

BOT_TOKEN = os.environ["BOT_TOKEN"]
LOCK_PATH = os.environ.get("LOCK_PATH", "/opt/tg_leads/.update.lock")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
WAITING_FOR_DATE: Set[int] = set()
WAITING_FOR_EXCLUDE: Set[int] = set()
AUTO_REPLY_PROCESS: Optional[subprocess.Popen] = None

AUTO_REPLY_PATH = os.environ.get("AUTO_REPLY_PATH", "auto_reply.py")
AUTO_REPLY_CMD = os.environ.get("AUTO_REPLY_CMD")


def kb_main():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📄 Оновити таблицю", callback_data="update"))
    kb.add(types.InlineKeyboardButton("📅 Історія за датою", callback_data="update_by_date"))
    kb.add(types.InlineKeyboardButton("🚫 Виключити з таблиці", callback_data="exclude_user"))
    kb.add(types.InlineKeyboardButton("▶️ Старт авто", callback_data="auto_start"))
    kb.add(types.InlineKeyboardButton("⏹ Стоп авто", callback_data="auto_stop"))
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


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer("Готово 👇", reply_markup=kb_main())


@dp.callback_query_handler(lambda c: c.data == "update")
async def cb_update(call: types.CallbackQuery):
    if not acquire_lock(LOCK_PATH, ttl_sec=300):
        await call.answer("⏳ Вже оновлюється…", show_alert=True)
        return

    await call.answer("⏳ Оновлюю…")

    try:
        tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
        today = datetime.now(tz).date()
        sheet_title = today.strftime("%d.%m.%y")
        n, _ = await update_google_sheet(
            target_date=today,
            worksheet_override=sheet_title,
            replace_existing=True
        )
        await call.message.reply(f"✅ Таблицю оновлено\nЛист: {sheet_title}\nДодано: {n}")
    except Exception:
        await call.message.reply("❌ Помилка оновлення")
    finally:
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
    try:
        n, _ = await update_google_sheet(
            target_date=target_date,
            worksheet_override=sheet_title,
            replace_existing=True
        )
        await message.answer(f"✅ Лист \"{sheet_title}\" оновлено\nДодано: {n}")
    except Exception:
        await message.answer("❌ Помилка оновлення за датою")
    finally:
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
            try:
                today = datetime.now(tz).date()
                sheet_title = today.strftime("%d.%m.%y")
                await update_google_sheet(
                    target_date=today,
                    worksheet_override=sheet_title,
                    replace_existing=True
                )
            finally:
                release_lock(LOCK_PATH)

    async def on_startup(_):
        asyncio.create_task(scheduled_daily_update())

    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
