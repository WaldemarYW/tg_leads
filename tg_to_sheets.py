import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List

from dotenv import load_dotenv
load_dotenv("/opt/tg_leads/.env")

from telethon import TelegramClient
from telethon.tl.types import User

import gspread
from google.oauth2.service_account import Credentials


ANKETA_TEXT = "Фінальний етап перед навчанням. Заповніть анкету"
REFERRAL_TEXT = "У нашій компанії діє реферальна програма"
CONFIRM_TEXT = "Дякую! Передаю вас на навчання"
HELLO_TEXT = "Доброго дня! 🙂 Мене звати Володимир"

WAIT_TRIGGERS = [
    "Супер! Чи готові ви перейти до навчання",
    "можу надіслати вам коротке відео",
    "як вам зручніше",
    "Можу надіслати вам коротке відео",
]

NEUTRAL_IN = {
    "ок", "ok", "добре", "хорошо", "зрозуміло",
    "я зрозуміла", "я зрозумів", "понятно", "ясно", ""
}


def normalize_text(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def classify_status(last_out: str, last_in: str) -> str:
    t_out = normalize_text(last_out)
    t_in = normalize_text(last_in)

    if normalize_text(CONFIRM_TEXT) in t_out:
        return "✅ Согласился (передан на обучение)"
    if normalize_text(ANKETA_TEXT) in t_out:
        return "📝 Анкета отправлена (ждём данныее)"
    if normalize_text(REFERRAL_TEXT) in t_out:
        return "❌ Холодный (рефералка)"

    if any(normalize_text(x) in t_out for x in WAIT_TRIGGERS):
        if t_in in NEUTRAL_IN:
            return "⏳ Ожидает ответа"

    if normalize_text(HELLO_TEXT) in t_out and not t_in:
        return "🆕 Новый"

    return "💬 В диалоге"


def build_chat_link_app(entity, peer_id: int) -> str:
    """
    Кликабельная ссылка в Google Sheets:
    - если есть username -> https://t.me/<username>
    - если нет -> tg://user?id=<id> (может открываться только в Telegram app)
    Для RU/UA локали в Sheets нужен разделитель ;, не ,
    """
    username = getattr(entity, "username", None)
    if username:
        url = f"https://t.me/{username}"
    else:
        uid = getattr(entity, "id", None) or peer_id
        url = f"tg://user?id={uid}"

    return f'=HYPERLINK("{url}";"Відкрити чат")'


def sheets_client(creds_path: str):
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def ensure_headers(ws, headers: List[str]):
    first = ws.row_values(1)
    if first != headers:
        ws.clear()
        ws.append_row(headers)


def acquire_lock(lock_path: str, ttl_sec: int = 300) -> bool:
    now = time.time()
    if os.path.exists(lock_path):
        try:
            if now - os.path.getmtime(lock_path) < ttl_sec:
                return False
        except Exception:
            pass
    try:
        with open(lock_path, "w") as f:
            f.write(str(now))
        return True
    except Exception:
        return False


def release_lock(lock_path: str):
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception:
        pass


async def update_google_sheet() -> Tuple[int, str]:
    api_id = int(os.environ["API_ID"])
    api_hash = os.environ["API_HASH"]
    session_file = os.environ["SESSION_FILE"]

    tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
    only_today = os.environ.get("ONLY_TODAY", "true").lower() == "true"
    today = datetime.now(tz).date()

    creds_path = os.environ["GOOGLE_CREDS"]
    sheet_name = os.environ["SHEET_NAME"]
    worksheet_name = os.environ.get("WORKSHEET", "Leads")

    gc = sheets_client(creds_path)
    sh = gc.open(sheet_name)
    ws = sh.worksheet(worksheet_name)

    headers = ["date", "name", "chat_link_app", "username", "status", "last_in", "last_out", "peer_id"]
    ensure_headers(ws, headers)

    client = TelegramClient(session_file, api_id, api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        await client.disconnect()
        return 0, "❌ Сессия не авторизована"

    rows = []

    async for dialog in client.iter_dialogs():
        if not dialog.is_user:
            continue

        entity = dialog.entity
        if getattr(entity, "bot", False):
            continue

        last_msg = dialog.message
        if not last_msg or not last_msg.date:
            continue

        msg_date = last_msg.date.astimezone(tz).date()
        if only_today and msg_date != today:
            continue

        peer_id = dialog.id
        name = getattr(entity, "first_name", "") or "Unknown"
        uname = getattr(entity, "username", "") or ""

        chat_link = build_chat_link_app(entity, peer_id)

        last_in = ""
        last_out = ""
        async for m in client.iter_messages(entity, limit=40):
            if not m.message:
                continue
            if m.out and not last_out:
                last_out = m.message
            if not m.out and not last_in:
                last_in = m.message
            if last_in and last_out:
                break

        status = classify_status(last_out, last_in)

        rows.append([
            str(msg_date),
            name,
            chat_link,
            ("@" + uname) if uname else "",
            status,
            (last_in or "")[:200],
            (last_out or "")[:200],
            peer_id
        ])

    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")

    await client.disconnect()
    return len(rows), "OK"


async def run_cli():
    n, msg = await update_google_sheet()
    print(f"✔ rows: {n} | {msg}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_cli())
