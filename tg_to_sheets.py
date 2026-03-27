import json
import os
import socket
import time
import uuid
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List, Set

# Legacy module: manual sheet update/exclusion flows are preserved for compatibility,
# but runtime auto-reply now writes directly via auto_reply.py to the active monthly leads sheet model.
LEGACY_SHEETS_WRITE_ENABLED = os.environ.get("LEGACY_SHEETS_WRITE_ENABLED", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

from dotenv import load_dotenv
load_dotenv("/opt/tg_leads/.env")

from telethon import TelegramClient
from telethon.tl.types import User

import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound


RU_MONTHS = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}


CONTACT_TEXT = (
    "Доброго дня.\n"
    "Мене звати Володимир, я HR компанії «Furioza».\n\n"
    "Ви залишали відгук на вакансію чат-менеджера.\n"
    "Підкажіть, будь ласка, пошук роботи для Вас зараз актуальний?"
)
INTEREST_TEXT = (
    "Дякую.\n"
    "Тоді коротко зорієнтую по вакансії.\n\n"
    "Це віддалена full-time робота у сфері дейтингу, де ми ведемо текстове спілкування від імені анкет."
)
DATING_TEXT = (
    "Що таке дейтинг?\n\n"
    "Це платне спілкування в текстових чатах.\n"
    "Користувачі самі вирішують, чи продовжувати діалог, і оплачують доступні сервіси платформи.\n\n"
    "Без дзвінків.\n"
    "Без відео.\n"
    "Тільки текстові чати.\n\n"
    "Ваші основні завдання:\n"
    "– Вести текстові чати з користувачами платформи\n"
    "– Відповідати на вхідні повідомлення\n"
    "– Працювати з листами та інвайтами\n\n"
    "Наша мета — підтримувати активне й цікаве спілкування,\n"
    "щоб користувачі продовжували діалог і взаємодію на платформі."
)
DUTIES_TEXT = (
    "Цей шаблон збережено для сумісності зі старими діалогами."
)
CLARIFY_TEXT = (
    "Якщо на цьому етапі є питання - напишіть, я коротко поясню.\n"
    "Якщо такий формат Вам у цілому підходить, можемо переходити далі."
)
SHIFTS_TEXT = (
    "Ми пропонуємо 2 зміни на вибір — Ви обираєте одну і працюєте за цим графіком на постійній основі:\n"
    "- Денна 14:00–23:00\n"
    "- Нічна 23:00–08:00\n"
    "На кожній зміні передбачено:\n"
    "– 1 година основної перерви\n"
    "– Короткі міні-перерви по 5 хвилин\n"
    "Щодо вихідних: у Вас є 8 вихідних днів на місяць, брати їх можна коли зручно."
)
SHIFT_QUESTION_TEXT = "Яку зміну Вам зручніше розглянути: денну чи нічну?"
FORMAT_TEXT = (
    "Щоб Вам було легше зрозуміти формат без довгих пояснень у чаті, я підготував короткий мінікурс + відео для новачків.\n"
    "Там по суті: як виглядає зміна, що саме потрібно робити, як оплачується робота і з чого почати."
)
FORMAT_QUESTION_TEXT = "Як вам зручніше: переглянути коротке відео чи пройти мінікурс?"
VIDEO_FOLLOWUP_TEXT = (
    "Якщо після перегляду відео у вас залишаться запитання, я з радістю на них відповім 😊"
)
MINI_COURSE_LINK = "https://alpha-mini.pp.ua/"
MINI_COURSE_FOLLOWUP_TEXT = (
    "Якщо після проходження мінікурсу у вас залишаться запитання, я з радістю на них відповім 😊"
)
BOTH_FORMATS_FOLLOWUP_TEXT = (
    "Після ознайомлення з матеріалами ви зможете краще зрозуміти формат роботи. "
    "Якщо виникнуть запитання — я на звʼязку."
)
TRAINING_TEXT = (
    "Навчання проходить онлайн на нашому сайті\n"
    "та займає приблизно 3 години.\n\n"
    "Формат навчання:\n"
    "– короткі текстові блоки\n"
    "– відеоуроки\n"
    "– невеликі тести після кожного блоку\n\n"
    "Проходите у зручному для Вас темпі."
)
TRAINING_QUESTION_TEXT = "Чи готові ви перейти до навчання?"
FORM_TEXT = (
    "Фінальний етап перед стартом — заповнення анкети.\n"
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
CONFIRM_TEXT = "Дякую.\nПередаю Вас тімліду на наступний етап."
REFERRAL_TEXT = "Також хочу повідомити, що в нашій компанії діє реферальна програма 💰."

STATUS_RULES_WORKSHEET = os.environ.get("STATUS_RULES_WORKSHEET", "StatusRules")
STATUS_RULES_HEADERS = ["template", "status"]

DEFAULT_STATUS_RULES = [
    (CONTACT_TEXT, "Вводные отправлены"),
    (CLARIFY_TEXT, "Вводные отправлены"),
    (SHIFT_QUESTION_TEXT, "Ожидание выбора смены"),
    (FORMAT_QUESTION_TEXT, "Формат работы объяснен"),
    (VIDEO_FOLLOWUP_TEXT, "Формат работы объяснен"),
    (TRAINING_QUESTION_TEXT, "Доход и обучение объяснены"),
    (CONFIRM_TEXT, "Передано тимлиду"),
]

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
    MINI_COURSE_FOLLOWUP_TEXT,
    BOTH_FORMATS_FOLLOWUP_TEXT,
    TRAINING_TEXT,
    TRAINING_QUESTION_TEXT,
    FORM_TEXT,
    CONFIRM_TEXT,
    REFERRAL_TEXT,
]


def normalize_text(s: Optional[str]) -> str:
    text = (s or "").strip().lower()
    if not text:
        return ""
    return " ".join(text.split())


def format_month_sheet_title(dt: date) -> str:
    return f"{RU_MONTHS[dt.month]} {dt.month:02d} {dt.year}"


def classify_status(
    template_out: str,
    last_msg_from_me: Optional[bool],
    consecutive_out: int,
    status_rules: List[Tuple[str, str]],
    last_in_text: str
) -> str:
    t_out = normalize_text(template_out)
    if normalize_text(REFERRAL_TEXT) in t_out:
        return ""
    if normalize_text(CONFIRM_TEXT) in t_out:
        return "Передано тимлиду"

    if last_msg_from_me is False:
        return ""

    if consecutive_out >= 3:
        return "Не актуально"

    for template, status in status_rules:
        if normalize_text(template) in t_out:
            return status

    return ""


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
        ws.update(range_name="1:1", values=[new_headers])


def get_or_create_worksheet(sh, title: str, rows: int, cols: int):
    try:
        return sh.worksheet(title)
    except WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)




def normalize_username(username: Optional[str]) -> str:
    return (username or "").strip().lstrip("@").lower()


def load_status_rules(sh) -> List[Tuple[str, str]]:
    if not LEGACY_SHEETS_WRITE_ENABLED:
        return DEFAULT_STATUS_RULES[:]
    ws = get_or_create_worksheet(sh, STATUS_RULES_WORKSHEET, rows=1000, cols=len(STATUS_RULES_HEADERS))
    ensure_headers(ws, STATUS_RULES_HEADERS, strict=False)
    values = ws.get_all_values()
    if len(values) <= 1:
        rows = [[t, s] for t, s in DEFAULT_STATUS_RULES]
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
        return DEFAULT_STATUS_RULES[:]

    rules = []
    for row in values[1:]:
        if len(row) < 2:
            continue
        template = row[0].strip()
        status = row[1].strip()
        if template and status:
            rules.append((template, status))
    return rules or DEFAULT_STATUS_RULES[:]



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
    if not LEGACY_SHEETS_WRITE_ENABLED:
        return False, "disabled"
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


def add_exclusion_entries_bulk(entries: List[Tuple[Optional[int], Optional[str], str, str, Optional[str], Optional[str]]]) -> int:
    if not entries:
        return 0
    if not LEGACY_SHEETS_WRITE_ENABLED:
        return 0

    creds_path = os.environ["GOOGLE_CREDS"]
    sheet_name = os.environ["SHEET_NAME"]
    worksheet_name = os.environ.get("EXCLUDED_WORKSHEET", "Excluded")

    headers = ["peer_id", "username", "name", "chat_link_app", "added_at", "added_by", "source"]
    gc = sheets_client(creds_path)
    sh = gc.open(sheet_name)
    ws = get_or_create_worksheet(sh, worksheet_name, rows=1000, cols=len(headers))
    ensure_headers(ws, headers, strict=False)

    peer_ids, usernames = load_exclusions(sh, worksheet_name)
    tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
    added_at = datetime.now(tz).isoformat(timespec="seconds")
    rows = []

    for peer_id, username, added_by, source, name, chat_link_app in entries:
        norm_username = normalize_username(username)
        if peer_id is not None and peer_id in peer_ids:
            continue
        if norm_username and norm_username in usernames:
            continue

        rows.append([
            str(peer_id) if peer_id is not None else "",
            ("@" + norm_username) if norm_username else "",
            name or "",
            chat_link_app or "",
            added_at,
            added_by,
            source,
        ])
        if peer_id is not None:
            peer_ids.add(peer_id)
        if norm_username:
            usernames.add(norm_username)

    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED")
    return len(rows)


def _read_lock_meta(lock_path: str) -> dict:
    try:
        with open(lock_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _is_lock_stale(lock_path: str, ttl_sec: int) -> bool:
    ttl = max(1, int(ttl_sec or 0))
    now = time.time()
    meta = _read_lock_meta(lock_path)
    created_at = meta.get("created_at")
    if isinstance(created_at, (int, float)) and (now - float(created_at)) >= ttl:
        return True
    try:
        return (now - os.path.getmtime(lock_path)) >= ttl
    except Exception:
        return True


def _try_create_lock_file(lock_path: str, payload: dict) -> bool:
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    fd = os.open(lock_path, flags, 0o644)
    try:
        data = json.dumps(payload, ensure_ascii=True)
        os.write(fd, data.encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    return True


def acquire_lock(lock_path: str, ttl_sec: int = 300) -> bool:
    now = time.time()
    payload = {
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "token": str(uuid.uuid4()),
        "created_at": now,
    }
    for _ in range(2):
        try:
            return _try_create_lock_file(lock_path, payload)
        except FileExistsError:
            if not _is_lock_stale(lock_path, ttl_sec):
                return False
            try:
                os.remove(lock_path)
            except FileNotFoundError:
                continue
            except Exception:
                return False
        except Exception:
            return False
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
    replace_existing: bool = False,
    session_file: Optional[str] = None,
    session_lock: Optional[str] = None,
    api_id: Optional[int] = None,
    api_hash: Optional[str] = None,
) -> Tuple[int, str]:
    if not LEGACY_SHEETS_WRITE_ENABLED:
        return 0, "❌ Legacy update_google_sheet вимкнено (LEGACY_SHEETS_WRITE_ENABLED=0)"
    api_id = api_id or int(os.environ["API_ID"])
    api_hash = api_hash or os.environ["API_HASH"]
    session_file = session_file or os.environ["SESSION_FILE"]
    session_lock = session_lock or os.environ.get("TELETHON_SESSION_LOCK", f"{session_file}.lock")

    tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
    env_only_today = os.environ.get("ONLY_TODAY", "true").lower() == "true"
    filter_today = target_date is not None or env_only_today
    today = target_date or datetime.now(tz).date()

    creds_path = os.environ["GOOGLE_CREDS"]
    sheet_name = os.environ["SHEET_NAME"]
    worksheet_name = worksheet_override or format_month_sheet_title(today)

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
    status_rules = load_status_rules(sh)

    if not acquire_lock(session_lock, ttl_sec=300):
        return 0, "❌ Сесія зайнята (інший процес працює)"

    client = TelegramClient(session_file, api_id, api_hash)
    await client.connect()

    if not await client.is_user_authorized():
        await client.disconnect()
        release_lock(session_lock)
        return 0, "❌ Сессия не авторизована"

    rows = []
    exclusions = []

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
            if last_in and last_out and template_out and not counting_consecutive_out:
                break

        if not template_out:
            exclusions.append(
                (peer_id, norm_uname or None, "auto", "auto", name, chat_link)
            )
            continue
        if not last_in and not last_out:
            continue

        if has_referral_template:
            status = "🎁 Реферал"
        else:
            status = classify_status(
                template_out,
                last_msg_from_me,
                consecutive_out,
                status_rules,
                last_in,
            )

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

    if exclusions:
        add_exclusion_entries_bulk(exclusions)

    await client.disconnect()
    release_lock(session_lock)
    return len(rows), "OK"


async def run_cli():
    n, msg = await update_google_sheet()
    print(f"✔ rows: {n} | {msg}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_cli())
