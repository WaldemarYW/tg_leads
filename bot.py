import os
from datetime import datetime
from typing import Set

from dotenv import load_dotenv

load_dotenv("/opt/tg_leads/.env")

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

from tg_to_sheets import update_google_sheet, acquire_lock, release_lock

BOT_TOKEN = os.environ["BOT_TOKEN"]
LOCK_PATH = os.environ.get("LOCK_PATH", "/opt/tg_leads/.update.lock")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
WAITING_FOR_DATE: Set[int] = set()


def kb_main():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📄 Оновити таблицю", callback_data="update"))
    kb.add(types.InlineKeyboardButton("📅 Історія за датою", callback_data="update_by_date"))
    return kb


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
        n, _ = await update_google_sheet()
        await call.message.reply(f"✅ Таблицю оновлено\nДодано: {n}")
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
    WAITING_FOR_DATE.add(call.from_user.id)
    await call.answer()
    await call.message.reply("Введіть дату у форматі ДД.ММ.РР або ДД.ММ.РРРР (наприклад, 19.08.25)")


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
    executor.start_polling(dp, skip_updates=True)
