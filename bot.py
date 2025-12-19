import os
from dotenv import load_dotenv

load_dotenv("/opt/tg_leads/.env")

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

from tg_to_sheets import update_google_sheet, acquire_lock, release_lock

BOT_TOKEN = os.environ["BOT_TOKEN"]
LOCK_PATH = os.environ.get("LOCK_PATH", "/opt/tg_leads/.update.lock")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)


def kb_main():
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📄 Оновити таблицю", callback_data="update"))
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


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
