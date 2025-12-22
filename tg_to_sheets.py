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


CONTACT_TEXT = (
    "Доброго дня 🙂\n"
    "Мене звати Володимир, я HR компанії «Furioza».\n\n"
    "Ви залишали відгук на вакансію менеджера чату.\n"
    "Підкажіть, будь ласка, пошук роботи для вас зараз актуальний?"
)
INTEREST_TEXT = (
    "Чудово 🙌\n"
    "Тоді коротко розповім, що саме ми пропонуємо.\n\n"
    "Наша компанія — це холдингова компанія, яка працює у сфері дейтингу."
)
DATING_TEXT = (
    "Що таке дейтинг?\n\n"
    "Це платне спілкування в текстових чатах.\n"
    "Користувачі самі вирішують, чи продовжувати діалог і купують послуги для спілкування.\n\n"
    "Без дзвінків.\n"
    "Без відео.\n"
    "Тільки текстові чати."
)
DUTIES_TEXT = (
    "Ваші основні завдання:\n"
    "– Вести текстові чати з користувачами платформи\n"
    "– Відповідати на вхідні повідомлення\n"
    "– Працювати з листами та інвайтами за готовими шаблонами\n\n"
    "Наша мета — підтримувати активне спілкування користувачів,\n"
    "щоб вони продовжували діалог і користувалися платними функціями платформи."
)
CLARIFY_TEXT = (
    "Скажіть, будь ласка, чи все зрозуміло на цьому етапі?\n"
    "Можливо, вже є питання?"
)
SHIFTS_TEXT = (
    "Компанія пропонує 3 зміни на вибір — ви обираєте одну і працюєте на постійній основі:\n\n"
    "Ранкова зміна 8:00 - 17:00\n"
    "Денна зміна: 14:00 – 23:00\n"
    "Нічна зміна: 23:00 – 08:00\n\n"
    "На кожній зміні передбачено:\n"
    "– 1 година основної перерви\n"
    "– Короткі міні-перерви по 5 хвилин\n\n"
    "Чому нічна зміна вигідніша?\n"
    "У нічний час активність користувачів вища,\n"
    "тому дохід у середньому більший."
)
SHIFT_QUESTION_TEXT = "Яка зміна вам була б зручніша?"
FORMAT_TEXT = (
    "Я можу надіслати коротке відео з поясненням вакансії\n"
    "або влаштувати для вас онлайн-співбесіду 👥.\n\n"
    "Будь ласка, підкажіть, який формат Вам зручніший, і я організую все необхідне!"
)
FORMAT_QUESTION_TEXT = "Як вам зручніше?"
VIDEO_FOLLOWUP_TEXT = (
    "Якщо після перегляду відео у вас залишаться запитання, я з радістю на них відповім 😊"
)
TRAINING_TEXT = (
    "Навчання проходить онлайн на нашому сайті\n"
    "та займає приблизно 3 години.\n\n"
    "Формат навчання:\n"
    "– короткі текстові блоки\n"
    "– відеоуроки\n"
    "– невеликі тести після кожного блоку\n\n"
    "Проходите у зручному для вас темпі."
)
TRAINING_QUESTION_TEXT = "Чи готові ви перейти до навчання?"
FORM_TEXT = (
    "Фінальний етап перед стартом навчання — заповнення анкети.\n"
    "Будь ласка, надішліть мені наступну інформацію:\n\n"
    "1. ПІБ\n"
    "2. Дата народження\n"
    "3. Контактний номер телефону\n"
    "4. Посилання на Telegram\n"
    "5. Чи є у вас діти до 3 років\n"
    "6. Обрана зміна\n"
    "7. Дата, з якої готові розпочати стажування\n"
    "8. Місто проживання\n"
    "9. Електронна пошта\n"
    "10. Скріншот документа для підтвердження віку\n\n"
    "Документ потрібен лише для підтвердження віку\n"
    "та внутрішньої перевірки компанії.\n"
    "Інформація не передається третім особам."
)
CONFIRM_TEXT = "Дякую! 🙌\nПередаю вас на етап навчання"
REFERRAL_TEXT = "Також хочу повідомити, що в нашій компанії діє реферальна програма 💰."

SCRIPT_TEMPLATES = [
    CONTACT_TEXT,
    INTEREST_TEXT,
    DATING_TEXT,
    DUTIES_TEXT,
    CLARIFY_TEXT,
    SHIFTS_TEXT,
    SHIFT_QUESTION_TEXT,
    FORMAT_TEXT,
    FORMAT_QUESTION_TEXT,
    VIDEO_FOLLOWUP_TEXT,
    TRAINING_TEXT,
    TRAINING_QUESTION_TEXT,
    FORM_TEXT,
    CONFIRM_TEXT,
    REFERRAL_TEXT,
]


def normalize_text(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def classify_status(
    template_out: str,
    last_msg_from_me: Optional[bool],
    consecutive_out: int
) -> str:
    t_out = normalize_text(template_out)
    if normalize_text(CONFIRM_TEXT) in t_out:
        return "✅ Погодився Дякую! 🙌 Передаю вас на етап навчання"
    if normalize_text(REFERRAL_TEXT) in t_out:
        return "🎁 Реферал Також хочу повідомити, що в нашій компанії діє реферальна програма 💰."
    if last_msg_from_me is False:
        return "📨 Останнє повідомлення від кандидата"
    if consecutive_out >= 3:
        return "🔁 3+ повідомлення від нас без відповіді"

    if normalize_text(CONTACT_TEXT) in t_out:
        return "👋 Привітання"
    if normalize_text(INTEREST_TEXT) in t_out:
        return "🏢 Знайомство з компанією"
    if normalize_text(DATING_TEXT) in t_out:
        return "🎥 Більше інформації"
    if normalize_text(DUTIES_TEXT) in t_out:
        return "🎥 Більше інформації"
    if normalize_text(CLARIFY_TEXT) in t_out:
        return "🏢 Знайомство з компанією"
    if normalize_text(SHIFTS_TEXT) in t_out:
        return "🕒 Графік"
    if normalize_text(SHIFT_QUESTION_TEXT) in t_out:
        return "🕒 Графік"
    if normalize_text(FORMAT_TEXT) in t_out:
        return "🎥 Більше інформації"
    if normalize_text(FORMAT_QUESTION_TEXT) in t_out:
        return "🎥 Більше інформації"
    if normalize_text(VIDEO_FOLLOWUP_TEXT) in t_out:
        return "🎥 Відео"
    if normalize_text(TRAINING_TEXT) in t_out:
        return "🎓 Навчання"
    if normalize_text(TRAINING_QUESTION_TEXT) in t_out:
        return "🎓 Навчання"
    if normalize_text(FORM_TEXT) in t_out:
        return "📝 Анкета"

    return "💬 У діалозі"


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


def ensure_headers(ws, headers: List[str], strict: bool = True):
    first = ws.row_values(1)
    if not first:
        ws.append_row(headers)
        return
    if strict:
        if first != headers:
            ws.clear()
            ws.append_row(headers)
        return
    existing = [h.strip() for h in first]
    if any(h not in existing for h in headers):
        new_headers = first[:]
        for h in headers:
            if h not in existing:
                new_headers.append(h)
        ws.update("1:1", [new_headers])


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


def add_exclusion_entry(
    peer_id: Optional[int],
    username: Optional[str],
    added_by: str,
    source: str,
    name: Optional[str] = None,
    chat_link_app: Optional[str] = None
) -> Tuple[bool, str]:
    creds_path = os.environ["GOOGLE_CREDS"]
    sheet_name = os.environ["SHEET_NAME"]
    worksheet_name = os.environ.get("EXCLUDED_WORKSHEET", "Excluded")

    headers = ["peer_id", "username", "name", "chat_link_app", "added_at", "added_by", "source"]
    gc = sheets_client(creds_path)
    sh = gc.open(sheet_name)
    ws = get_or_create_worksheet(sh, worksheet_name, rows=1000, cols=len(headers))
    ensure_headers(ws, headers, strict=False)

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
        name or "",
        chat_link_app or "",
        added_at,
        added_by,
        source
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
        template_out = ""
        last_msg_from_me: Optional[bool] = None
        has_referral_template = False
        has_confirm_status = False
        saw_incoming_no_question = False
        consecutive_out = 0
        counting_consecutive_out = True
        async for m in client.iter_messages(entity, limit=40):
            if not m.message:
                continue
            if last_msg_from_me is None:
                last_msg_from_me = m.out
            if counting_consecutive_out:
                if m.out:
                    consecutive_out += 1
                else:
                    counting_consecutive_out = False

            if not m.out and not saw_incoming_no_question:
                if "?" not in m.message:
                    saw_incoming_no_question = True

            if m.out and not last_out:
                last_out = m.message
            if not m.out and not last_in:
                last_in = m.message
            if m.out and not template_out and is_script_template(m.message):
                template_out = m.message
            if m.out and not has_referral_template:
                if normalize_text(REFERRAL_TEXT) in normalize_text(m.message):
                    has_referral_template = True
            if m.out and not has_confirm_status:
                if normalize_text(TRAINING_QUESTION_TEXT) in normalize_text(m.message) and saw_incoming_no_question:
                    has_confirm_status = True
            if last_in and last_out and template_out and not counting_consecutive_out:
                break

        if not template_out:
            add_exclusion_entry(
                peer_id=peer_id,
                username=norm_uname or None,
                added_by="auto",
                source="auto",
                name=name,
                chat_link_app=chat_link
            )
            continue
        if not last_in and not last_out:
            continue

        if has_referral_template:
            status = "🎁 Реферал Також хочу повідомити, що в нашій компанії діє реферальна програма 💰."
        elif has_confirm_status:
            status = "✅ Погодився Дякую! 🙌 Передаю вас на етап навчання"
        else:
            status = classify_status(template_out, last_msg_from_me, consecutive_out)

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
