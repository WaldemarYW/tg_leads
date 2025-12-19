import os
import time
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List, Set

from dotenv import load_dotenv
load_dotenv("/opt/tg_leads/.env")

from telethon import TelegramClient
from telethon.tl.types import User

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound


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

SCRIPT_TEMPLATES = [
    ANKETA_TEXT,
    REFERRAL_TEXT,
    CONFIRM_TEXT,
    HELLO_TEXT,
    *WAIT_TRIGGERS,
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
        return "📝 Анкета отправлена (ждём данные)"
    if normalize_text(REFERRAL_TEXT) in t_out:
        return "❌ Холодный (рефералка)"

    if any(normalize_text(x) in t_out for x in WAIT_TRIGGERS):
        if t_in in NEUTRAL_IN:
            return "⏳ Ожидает ответа"

    if normalize_text(HELLO_TEXT) in t_out and not t_in:
        return "🆕 Новый"

    return "💬 В диалоге"


def is_script_template(message_text: str) -> bool:
    text = normalize_text(message_text)
    return any(normalize_text(t) in text for t in SCRIPT_TEMPLATES)


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


def get_or_create_worksheet(sh, title: str, rows: int, cols: int):
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def normalize_username(username: Optional[str]) -> str:
    return (username or "").strip().lstrip("@").lower()


def load_exclusions(sh, worksheet_name: str) -> Tuple[Set[int], Set[str]]:
    try:
        ws = sh.worksheet(worksheet_name)
    except WorksheetNotFound:
        return set(), set()

    values = ws.get_all_values()
    if not values:
        return set(), set()

    headers = [h.strip().lower() for h in values[0]]
    data = values[1:]
    peer_ids: Set[int] = set()
    usernames: Set[str] = set()

    def get_col(name: str) -> Optional[int]:
        try:
            return headers.index(name)
        except ValueError:
            return None

    peer_idx = get_col("peer_id")
    user_idx = get_col("username")

    for row in data:
        if peer_idx is not None and peer_idx < len(row):
            raw = row[peer_idx].strip()
            if raw.isdigit():
                peer_ids.add(int(raw))
        if user_idx is not None and user_idx < len(row):
            uname = normalize_username(row[user_idx])
            if uname:
                usernames.add(uname)

    return peer_ids, usernames


def add_exclusion_entry(peer_id: Optional[int], username: Optional[str], added_by: str) -> Tuple[bool, str]:
    creds_path = os.environ["GOOGLE_CREDS"]
    sheet_name = os.environ["SHEET_NAME"]
    worksheet_name = os.environ.get("EXCLUDED_WORKSHEET", "Excluded")

    headers = ["peer_id", "username", "added_at", "added_by"]
    gc = sheets_client(creds_path)
    sh = gc.open(sheet_name)
    ws = get_or_create_worksheet(sh, worksheet_name, rows=1000, cols=len(headers))
    ensure_headers(ws, headers)

    peer_ids, usernames = load_exclusions(sh, worksheet_name)
    norm_username = normalize_username(username)

    if peer_id is not None and peer_id in peer_ids:
        return False, "already"
    if norm_username and norm_username in usernames:
        return False, "already"

    added_at = datetime.now(ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))).isoformat(timespec="seconds")
    row = [
        str(peer_id) if peer_id is not None else "",
        ("@" + norm_username) if norm_username else "",
        added_at,
        added_by
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")
    return True, "ok"


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


async def update_google_sheet(
    target_date: Optional[date] = None,
    worksheet_override: Optional[str] = None,
    replace_existing: bool = False
) -> Tuple[int, str]:
    api_id = int(os.environ["API_ID"])
    api_hash = os.environ["API_HASH"]
    session_file = os.environ["SESSION_FILE"]

    tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
    env_only_today = os.environ.get("ONLY_TODAY", "true").lower() == "true"
    filter_today = target_date is not None or env_only_today
    today = target_date or datetime.now(tz).date()

    creds_path = os.environ["GOOGLE_CREDS"]
    sheet_name = os.environ["SHEET_NAME"]
    worksheet_name = worksheet_override or os.environ.get("WORKSHEET", "Leads")

    gc = sheets_client(creds_path)
    sh = gc.open(sheet_name)
    headers = ["date", "name", "chat_link_app", "username", "status", "last_in", "last_out", "peer_id"]
    ws = get_or_create_worksheet(sh, worksheet_name, rows=1000, cols=len(headers))

    if replace_existing:
        ws.clear()
        ws.append_row(headers)
    else:
        ensure_headers(ws, headers)

    excluded_ids, excluded_usernames = load_exclusions(
        sh, os.environ.get("EXCLUDED_WORKSHEET", "Excluded")
    )

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
        if filter_today and msg_date != today:
            continue

        peer_id = dialog.id
        name = getattr(entity, "first_name", "") or "Unknown"
        uname = getattr(entity, "username", "") or ""
        norm_uname = normalize_username(uname)

        if peer_id in excluded_ids or (norm_uname and norm_uname in excluded_usernames):
            continue

        chat_link = build_chat_link_app(entity, peer_id)

        last_in = ""
        last_out = ""
        has_script_template = False
        async for m in client.iter_messages(entity, limit=40):
            if not m.message:
                continue
            if m.out and not last_out:
                last_out = m.message
            if m.out and not has_script_template and is_script_template(m.message):
                has_script_template = True
            if not m.out and not last_in:
                last_in = m.message
            if last_in and last_out:
                break

        if not has_script_template:
            continue
        if not last_in and not last_out:
            continue

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
