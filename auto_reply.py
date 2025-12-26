import os
import re
import time
import json
import asyncio
import signal
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, Tuple
import urllib.request
import urllib.error

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import UsernameNotOccupiedError, PhoneNumberInvalidError
from telethon.tl.types import User

from tg_to_sheets import (
    sheets_client,
    get_or_create_worksheet,
    ensure_headers,
    build_chat_link_app,
    normalize_username,
    normalize_text,
    is_script_template,
    classify_status,
    acquire_lock,
    release_lock,
    load_exclusions,
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
)

load_dotenv("/opt/tg_leads/.env")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_FILE = os.environ.get("AUTO_REPLY_SESSION_FILE", os.environ["SESSION_FILE"])

SHEET_NAME = os.environ["SHEET_NAME"]
GOOGLE_CREDS = os.environ["GOOGLE_CREDS"]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Kyiv")

LEADS_GROUP_TITLE = os.environ.get("LEADS_GROUP_TITLE", "DATING AGENCY | Referral")
VIDEO_GROUP_LINK = os.environ.get("VIDEO_GROUP_LINK")
VIDEO_GROUP_TITLE = os.environ.get("VIDEO_GROUP_TITLE", "Промо відео")
AUTO_REPLY_LOCK = os.environ.get("AUTO_REPLY_LOCK", "/opt/tg_leads/.auto_reply.lock")
AUTO_REPLY_LOCK_TTL = int(os.environ.get("AUTO_REPLY_LOCK_TTL", "300"))
REPLY_DEBOUNCE_SEC = float(os.environ.get("REPLY_DEBOUNCE_SEC", "3"))
SESSION_LOCK = os.environ.get("TELETHON_SESSION_LOCK", f"{SESSION_FILE}.lock")
STATUS_PATH = os.environ.get("AUTO_REPLY_STATUS_PATH", "/opt/tg_leads/.auto_reply.status")

HEADERS = [
    "date",
    "name",
    "chat_link_app",
    "username",
    "status",
    "auto_reply",
    "last_in",
    "last_out",
    "peer_id",
]

USERNAME_RE = re.compile(r"(?:@|t\.me/)([A-Za-z0-9_]{5,})")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{9,}\d")

VIDEO_WORDS = ("відео", "видео")
DIALOG_AI_URL = os.environ.get("DIALOG_AI_URL", "http://127.0.0.1:3000/dialog_suggest")
DIALOG_AI_TIMEOUT_SEC = float(os.environ.get("DIALOG_AI_TIMEOUT_SEC", "20"))
STEP_STATE_PATH = os.environ.get("AUTO_REPLY_STEP_STATE_PATH", "/opt/tg_leads/.auto_reply.step_state.json")
STATUS_RULES_WORKSHEET = os.environ.get("STATUS_RULES_WORKSHEET", "StatusRules")
STATUS_RULES_CACHE_PATH = os.environ.get("STATUS_RULES_CACHE_PATH", "/opt/tg_leads/.auto_reply.status_rules.json")
STATUS_RULES_CACHE_TTL_HOURS = int(os.environ.get("STATUS_RULES_CACHE_TTL_HOURS", "48"))
EXCLUDED_WORKSHEET = os.environ.get("EXCLUDED_WORKSHEET", "Excluded")
EXCLUSIONS_CACHE_PATH = os.environ.get("EXCLUSIONS_CACHE_PATH", "/opt/tg_leads/.auto_reply.exclusions.json")
EXCLUSIONS_CACHE_TTL_HOURS = int(os.environ.get("EXCLUSIONS_CACHE_TTL_HOURS", "6"))
PAUSE_WORKSHEET = os.environ.get("PAUSE_WORKSHEET", "Paused")
PAUSE_CACHE_TTL_SEC = int(os.environ.get("PAUSE_CACHE_TTL_SEC", "120"))
GROUP_LEADS_WORKSHEET = os.environ.get("GROUP_LEADS_WORKSHEET", "GroupLeads")
CONTINUE_DELAY_SEC = float(os.environ.get("AUTO_REPLY_CONTINUE_DELAY_SEC", "20"))
CONFIRM_STATUS = "✅ Погодився"
REFERRAL_STATUS = "🎁 Реферал"
IMMUTABLE_STATUSES = {CONFIRM_STATUS, REFERRAL_STATUS}
STOP_COMMANDS = {"стоп1", "stop1"}
START_COMMANDS = {"старт1", "start1"}

STEP_CONTACT = "contact"
STEP_INTEREST = "interest"
STEP_DATING = "dating"
STEP_DUTIES = "duties"
STEP_CLARIFY = "clarify"
STEP_SHIFTS = "shifts"
STEP_SHIFT_QUESTION = "shift_question"
STEP_FORMAT = "format"
STEP_FORMAT_QUESTION = "format_question"
STEP_VIDEO_FOLLOWUP = "video_followup"
STEP_TRAINING = "training"
STEP_TRAINING_QUESTION = "training_question"
STEP_FORM = "form"

TEMPLATE_TO_STEP = {
    normalize_text(CONTACT_TEXT): STEP_CONTACT,
    normalize_text(INTEREST_TEXT): STEP_INTEREST,
    normalize_text(DATING_TEXT): STEP_DATING,
    normalize_text(DUTIES_TEXT): STEP_DUTIES,
    normalize_text(CLARIFY_TEXT): STEP_CLARIFY,
    normalize_text(SHIFTS_TEXT): STEP_SHIFTS,
    normalize_text(SHIFT_QUESTION_TEXT): STEP_SHIFT_QUESTION,
    normalize_text(FORMAT_TEXT): STEP_FORMAT,
    normalize_text(FORMAT_QUESTION_TEXT): STEP_FORMAT_QUESTION,
    normalize_text(VIDEO_FOLLOWUP_TEXT): STEP_VIDEO_FOLLOWUP,
    normalize_text(TRAINING_TEXT): STEP_TRAINING,
    normalize_text(TRAINING_QUESTION_TEXT): STEP_TRAINING_QUESTION,
    normalize_text(FORM_TEXT): STEP_FORM,
}

PAUSE_HEADERS = [
    "peer_id",
    "username",
    "name",
    "chat_link_app",
    "status",
    "updated_at",
    "updated_by",
]

GROUP_LEADS_HEADERS = [
    "received_at",
    "status",
    "full_name",
    "age",
    "phone",
    "tg",
    "pc",
    "source_id",
    "source_name",
    "raw_text",
]

GROUP_KEY_MAP = {
    "піб": "full_name",
    "фио": "full_name",
    "імя": "full_name",
    "ім'я": "full_name",
    "имя": "full_name",
    "вік": "age",
    "возраст": "age",
    "номер телефону": "phone",
    "номер телефона": "phone",
    "телефон": "phone",
    "phone": "phone",
    "тг": "tg",
    "tg": "tg",
    "telegram": "tg",
    "чи є пк": "pc",
    "є пк": "pc",
    "pc": "pc",
    "id": "source_id",
    "name": "source_name",
}

def extract_contact(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None

    username_match = USERNAME_RE.search(text)
    if username_match:
        return username_match.group(1), None

    phone_match = PHONE_RE.search(text)
    if phone_match:
        raw = phone_match.group(0)
        normalized = re.sub(r"[^\d+]", "", raw)
        return None, normalized

    return None, None


def message_has_question(text: str) -> bool:
    return "?" in (text or "")


def should_send_question(sent_text: str, question_text: str) -> bool:
    if not sent_text:
        return True
    return normalize_text(question_text) not in normalize_text(sent_text)


def mark_step_without_send(
    sheet: "SheetWriter",
    tz: ZoneInfo,
    entity: User,
    status: Optional[str],
    step_state: Optional["StepState"],
    step_name: Optional[str],
):
    if step_state and step_name:
        step_state.set(entity.id, step_name)
    name = getattr(entity, "first_name", "") or "Unknown"
    username = getattr(entity, "username", "") or ""
    chat_link = build_chat_link_app(entity, entity.id)
    sheet.upsert(
        tz=tz,
        peer_id=entity.id,
        name=name,
        username=username,
        chat_link=chat_link,
        status=status,
        last_out=None,
    )


def normalize_key(text: str) -> str:
    cleaned = normalize_text(text)
    return re.sub(r"[^\w\s]", "", cleaned, flags=re.IGNORECASE)


def normalize_phone(text: str) -> str:
    return re.sub(r"[^\d+]", "", text or "")


def parse_group_message(text: str) -> dict:
    data = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        match = re.match(r"^[🔹•\-\*]\s*(.+?)\s*:\s*(.+)$", line)
        if not match:
            match = re.match(r"^(ID|Name)\s*:\s*(.+)$", line, flags=re.IGNORECASE)
        if not match:
            continue
        key_raw, value = match.group(1), match.group(2)
        key_norm = normalize_key(key_raw)
        field = GROUP_KEY_MAP.get(key_norm)
        if field:
            data[field] = value.strip()

    username, phone = extract_contact(text or "")
    if username and not data.get("tg"):
        data["tg"] = f"@{username}"
    if phone and not data.get("phone"):
        data["phone"] = phone
    data["raw_text"] = (text or "").strip()
    return data


class SheetWriter:
    def __init__(self):
        self.gc = sheets_client(GOOGLE_CREDS)
        self.sh = self.gc.open(SHEET_NAME)
        self.ws = None
        self.current_title = None

    def _today_title(self, tz: ZoneInfo) -> str:
        return datetime.now(tz).strftime("%d.%m.%y")

    def get_ws(self, tz: ZoneInfo):
        title = self._today_title(tz)
        if self.ws is None or self.current_title != title:
            self.ws = get_or_create_worksheet(self.sh, title, rows=1000, cols=len(HEADERS))
            ensure_headers(self.ws, HEADERS, strict=False)
            self.current_title = title
        return self.ws

    def _find_row(self, ws, peer_id: int):
        values = ws.get_all_values()
        if not values:
            return None, None
        headers = [h.strip().lower() for h in values[0]]
        if "peer_id" not in headers:
            return None, None
        peer_idx = headers.index("peer_id")
        for idx, row in enumerate(values[1:], start=2):
            if peer_idx < len(row) and row[peer_idx].strip() == str(peer_id):
                return idx, row
        return None, None

    def _get_headers(self, ws):
        return [h.strip().lower() for h in ws.row_values(1)]

    def _col_letter(self, col_idx: int) -> str:
        result = []
        while col_idx > 0:
            col_idx, rem = divmod(col_idx - 1, 26)
            result.append(chr(ord("A") + rem))
        return "".join(reversed(result))

    def upsert(
        self,
        tz: ZoneInfo,
        peer_id: int,
        name: str,
        username: str,
        chat_link: str,
        status: Optional[str] = None,
        auto_reply_enabled: Optional[bool] = None,
        last_in: Optional[str] = None,
        last_out: Optional[str] = None,
    ):
        ws = self.get_ws(tz)
        headers = self._get_headers(ws)
        row_idx, existing = self._find_row(ws, peer_id)
        existing = existing or [""] * len(headers)
        if len(existing) < len(headers):
            existing = existing + [""] * (len(headers) - len(existing))

        def col_idx(name: str) -> Optional[int]:
            try:
                return headers.index(name)
            except ValueError:
                return None

        def set_value(name: str, value: Optional[str]):
            if value is None:
                return
            idx = col_idx(name)
            if idx is None:
                return
            existing[idx] = value

        status_idx = col_idx("status")
        existing_status = (
            existing[status_idx] if status_idx is not None and status_idx < len(existing) else ""
        )
        if existing_status in IMMUTABLE_STATUSES:
            status = existing_status

        set_value("date", str(datetime.now(tz).date()))
        set_value("name", name)
        set_value("chat_link_app", chat_link)
        set_value("username", ("@" + username) if username else "")
        if status is not None:
            set_value("status", status)
        if auto_reply_enabled is not None:
            set_value("auto_reply", "ON" if auto_reply_enabled else "OFF")
        if last_in is not None:
            set_value("last_in", last_in)
        if last_out is not None:
            set_value("last_out", last_out)
        set_value("peer_id", str(peer_id))

        if row_idx:
            end_col = self._col_letter(len(headers))
            ws.update(f"A{row_idx}:{end_col}{row_idx}", [existing], value_input_option="USER_ENTERED")
        else:
            ws.append_row(existing, value_input_option="USER_ENTERED")

    def load_enabled_peers(self, tz: ZoneInfo) -> set:
        ws = self.get_ws(tz)
        values = ws.get_all_values()
        if not values:
            return set()
        headers = [h.strip().lower() for h in values[0]]
        try:
            peer_idx = headers.index("peer_id")
            auto_idx = headers.index("auto_reply")
        except ValueError:
            return set()
        enabled = set()
        for row in values[1:]:
            if peer_idx >= len(row) or auto_idx >= len(row):
                continue
            peer_raw = row[peer_idx].strip()
            auto_raw = row[auto_idx].strip().lower()
            if not peer_raw.isdigit():
                continue
            if auto_raw in {"on", "1", "yes", "true", "enabled"}:
                enabled.add(int(peer_raw))
        return enabled


class PauseStore:
    def __init__(self):
        self.gc = sheets_client(GOOGLE_CREDS)
        self.sh = self.gc.open(SHEET_NAME)
        self.ws = get_or_create_worksheet(self.sh, PAUSE_WORKSHEET, rows=1000, cols=len(PAUSE_HEADERS))
        ensure_headers(self.ws, PAUSE_HEADERS, strict=False)
        self.cache = {}
        self.username_cache = {}
        self.loaded_at = 0.0

    def _load_cache(self):
        try:
            values = self.ws.get_all_values()
        except Exception:
            values = []
        self.cache = {}
        self.username_cache = {}
        self.loaded_at = time.time()
        if not values:
            return
        headers = [h.strip().lower() for h in values[0]]

        def get_col(name: str) -> Optional[int]:
            try:
                return headers.index(name)
            except ValueError:
                return None

        peer_idx = get_col("peer_id")
        user_idx = get_col("username")
        status_idx = get_col("status")
        for row in values[1:]:
            status = row[status_idx].strip() if status_idx is not None and status_idx < len(row) else ""
            if peer_idx is not None and peer_idx < len(row):
                raw = row[peer_idx].strip()
                if raw.isdigit():
                    self.cache[int(raw)] = status
            if user_idx is not None and user_idx < len(row):
                uname = normalize_username(row[user_idx])
                if uname:
                    self.username_cache[uname] = status

    def get_status(self, peer_id: int, username: Optional[str]) -> Optional[str]:
        now = time.time()
        if not self.loaded_at or now - self.loaded_at > PAUSE_CACHE_TTL_SEC:
            self._load_cache()
        status = self.cache.get(peer_id)
        if not status and username:
            status = self.username_cache.get(normalize_username(username))
        if status:
            return status
        self._load_cache()
        status = self.cache.get(peer_id)
        if not status and username:
            status = self.username_cache.get(normalize_username(username))
        return status or None

    def set_status(
        self,
        peer_id: int,
        username: Optional[str],
        name: Optional[str],
        chat_link: Optional[str],
        status: str,
        updated_by: str = "manual",
    ):
        try:
            values = self.ws.get_all_values()
        except Exception:
            values = []
        headers = [h.strip().lower() for h in values[0]] if values else []

        def get_col(name: str) -> Optional[int]:
            try:
                return headers.index(name)
            except ValueError:
                return None

        row_idx = None
        peer_idx = get_col("peer_id")
        user_idx = get_col("username")
        if values and (peer_idx is not None or user_idx is not None):
            for idx, row in enumerate(values[1:], start=2):
                if peer_idx is not None and peer_idx < len(row):
                    if row[peer_idx].strip() == str(peer_id):
                        row_idx = idx
                        break
                if row_idx is None and user_idx is not None and user_idx < len(row):
                    if normalize_username(row[user_idx]) == normalize_username(username):
                        row_idx = idx
                        break

        updated_at = datetime.now(ZoneInfo(TIMEZONE)).isoformat(timespec="seconds")
        row = [
            str(peer_id),
            ("@" + normalize_username(username)) if username else "",
            name or "",
            chat_link or "",
            status,
            updated_at,
            updated_by,
        ]
        if row_idx:
            self.ws.update(f"A{row_idx}:G{row_idx}", [row], value_input_option="USER_ENTERED")
        else:
            self.ws.append_row(row, value_input_option="USER_ENTERED")
        self.cache[peer_id] = status
        if username:
            self.username_cache[normalize_username(username)] = status
        self.loaded_at = time.time()


class GroupLeadsSheet:
    def __init__(self):
        self.gc = sheets_client(GOOGLE_CREDS)
        self.sh = self.gc.open(SHEET_NAME)
        self.ws = get_or_create_worksheet(self.sh, GROUP_LEADS_WORKSHEET, rows=1000, cols=len(GROUP_LEADS_HEADERS))
        ensure_headers(self.ws, GROUP_LEADS_HEADERS, strict=False)

    def _find_row(self, values, tg_norm: str, phone_norm: str):
        if not values:
            return None, None
        headers = [h.strip().lower() for h in values[0]]
        data = values[1:]

        def get_col(name: str) -> Optional[int]:
            try:
                return headers.index(name)
            except ValueError:
                return None

        tg_idx = get_col("tg")
        phone_idx = get_col("phone")
        for idx, row in enumerate(data, start=2):
            if tg_idx is not None and tg_idx < len(row) and tg_norm:
                if normalize_username(row[tg_idx]) == tg_norm:
                    return idx, row
            if phone_idx is not None and phone_idx < len(row) and phone_norm:
                if normalize_phone(row[phone_idx]) == phone_norm:
                    return idx, row
        return None, None

    def upsert(self, tz: ZoneInfo, data: dict, status: Optional[str]):
        received_at = datetime.now(tz).isoformat(timespec="seconds")
        tg_value = data.get("tg", "") or ""
        phone_value = data.get("phone", "") or ""
        tg_norm = normalize_username(tg_value)
        phone_norm = normalize_phone(phone_value)
        try:
            values = self.ws.get_all_values()
        except Exception:
            values = []
        row_idx, existing = self._find_row(values, tg_norm, phone_norm)
        existing = existing or [""] * len(GROUP_LEADS_HEADERS)

        def take(key: str, idx: int) -> str:
            value = data.get(key)
            if value is not None and value != "":
                return value
            return existing[idx] if idx < len(existing) else ""

        row = [
            received_at,
            status or take("status", 1),
            take("full_name", 2),
            take("age", 3),
            take("phone", 4),
            take("tg", 5),
            take("pc", 6),
            take("source_id", 7),
            take("source_name", 8),
            take("raw_text", 9),
        ]
        if row_idx:
            self.ws.update(f"A{row_idx}:J{row_idx}", [row], value_input_option="USER_ENTERED")
        else:
            self.ws.append_row(row, value_input_option="USER_ENTERED")


async def find_group_by_title(client: TelegramClient, title: str):
    title_norm = (title or "").strip().lower()
    async for dialog in client.iter_dialogs():
        if not dialog.is_group:
            continue
        name = (dialog.name or "").strip().lower()
        if name == title_norm:
            return dialog.entity
    return None


async def resolve_contact(client: TelegramClient, username: Optional[str], phone: Optional[str]):
    try:
        if username:
            return await client.get_entity(username)
        if phone:
            return await client.get_entity(phone)
    except (UsernameNotOccupiedError, PhoneNumberInvalidError, ValueError):
        return None
    except Exception:
        return None
    return None


async def get_last_outgoing_step(client: TelegramClient, entity: User) -> Optional[str]:
    async for m in client.iter_messages(entity, limit=50):
        if not m.message or not m.out:
            continue
        msg_norm = normalize_text(m.message)
        for tmpl_norm, step in TEMPLATE_TO_STEP.items():
            if tmpl_norm and tmpl_norm in msg_norm:
                return step
    return None


async def get_last_step(client: TelegramClient, entity: User, step_state: "StepState") -> Optional[str]:
    cached = step_state.get(entity.id)
    if cached:
        return cached
    step = await get_last_outgoing_step(client, entity)
    if step:
        step_state.set(entity.id, step)
    return step


async def has_outgoing_template(client: TelegramClient, entity: User, step_state: "StepState") -> bool:
    if step_state.get(entity.id):
        return True
    async for m in client.iter_messages(entity, limit=30):
        if not m.message or not m.out:
            continue
        if is_script_template(m.message):
            return True
    return False


async def send_and_update(
    client: TelegramClient,
    sheet: SheetWriter,
    tz: ZoneInfo,
    entity: User,
    text: str,
    status: Optional[str],
    delay_after: Optional[float] = None,
    use_ai: bool = True,
    draft: Optional[str] = None,
    step_state: Optional["StepState"] = None,
    step_name: Optional[str] = None,
    auto_reply_enabled: Optional[bool] = None,
):
    message_text = text
    if use_ai:
        history = await build_ai_history(client, entity, limit=10)
        ai_text = await dialog_suggest(history, draft or text)
        if ai_text:
            message_text = ai_text
    await client.send_message(entity, message_text)
    name = getattr(entity, "first_name", "") or "Unknown"
    username = getattr(entity, "username", "") or ""
    chat_link = build_chat_link_app(entity, entity.id)
    try:
        with open(STATUS_PATH, "w") as f:
            json.dump(
                {
                    "last_sent_at": datetime.now(tz).isoformat(timespec="seconds"),
                    "peer_id": entity.id,
                    "username": username or "",
                    "name": name or "",
                    "text_preview": message_text[:200],
                },
                f,
                ensure_ascii=True,
            )
    except Exception:
        pass
    if step_state and step_name:
        step_state.set(entity.id, step_name)
    sheet.upsert(
        tz=tz,
        peer_id=entity.id,
        name=name,
        username=username,
        chat_link=chat_link,
        status=status,
        auto_reply_enabled=auto_reply_enabled,
        last_out=message_text[:200],
    )
    if delay_after:
        await asyncio.sleep(delay_after)
    return message_text


def wants_video(text: str) -> bool:
    t = normalize_text(text)
    return any(word in t for word in VIDEO_WORDS)


def _post_json(url: str, payload: dict, timeout_sec: float) -> dict:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        raw = resp.read().decode("utf-8")
    return json.loads(raw) if raw else {}


async def dialog_suggest(history: list, draft: str) -> Optional[str]:
    if not DIALOG_AI_URL:
        return None
    payload = {"history": history, "draft": draft}
    try:
        data = await asyncio.to_thread(_post_json, DIALOG_AI_URL, payload, DIALOG_AI_TIMEOUT_SEC)
    except (urllib.error.URLError, urllib.error.HTTPError, ValueError, OSError) as err:
        print(f"⚠️ AI error: {err}")
        return None
    if not data or not data.get("ok"):
        return None
    text = data.get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    suggestions = data.get("suggestions") or []
    if suggestions:
        return str(suggestions[0]).strip()
    return None


async def build_ai_history(client: TelegramClient, entity: User, limit: int = 10) -> list:
    items = []
    async for m in client.iter_messages(entity, limit=limit):
        if not m.message:
            continue
        items.append(
            {
                "sender": "me" if m.out else "candidate",
                "text": m.message,
            }
        )
    return list(reversed(items))


class StepState:
    def __init__(self, path: str):
        self.path = path
        self.data = {}
        self._load()

    def _load(self):
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                self.data = raw
        except Exception:
            self.data = {}

    def _save(self):
        try:
            with open(self.path, "w") as f:
                json.dump(self.data, f, ensure_ascii=True)
        except Exception:
            pass

    def get(self, peer_id: int) -> Optional[str]:
        return self.data.get(str(peer_id))

    def set(self, peer_id: int, step: str):
        self.data[str(peer_id)] = step
        self._save()


def load_status_rules_from_sheet() -> Tuple[Tuple[str, str], ...]:
    try:
        gc = sheets_client(GOOGLE_CREDS)
        sh = gc.open(SHEET_NAME)
        ws = get_or_create_worksheet(sh, STATUS_RULES_WORKSHEET, rows=1000, cols=2)
        ensure_headers(ws, ["template", "status"], strict=False)
        values = ws.get_all_values()
        if len(values) <= 1:
            rows = [
                [CONTACT_TEXT, "👋 Привітання"],
                [CLARIFY_TEXT, "🏢 Знайомство з компанією"],
                [SHIFT_QUESTION_TEXT, "🕒 Графік"],
                [FORMAT_QUESTION_TEXT, "🎥 Більше інформації"],
                [VIDEO_FOLLOWUP_TEXT, "🎥 Відео"],
                [TRAINING_QUESTION_TEXT, "🎓 Навчання"],
                [CONFIRM_TEXT, "✅ Погодився Дякую! 🙌 Передаю вас на етап навчання"],
                [REFERRAL_TEXT, "🎁 Реферал Також хочу повідомити, що в нашій компанії діє реферальна програма 💰."],
            ]
            ws.append_rows(rows, value_input_option="USER_ENTERED")
            return tuple((r[0], r[1]) for r in rows)

        rules = []
        for row in values[1:]:
            if len(row) < 2:
                continue
            template = row[0].strip()
            status = row[1].strip()
            if template and status:
                rules.append((template, status))
        return tuple(rules) if rules else tuple()
    except Exception:
        return tuple()


def get_status_rules_cached() -> Tuple[Tuple[str, str], ...]:
    now_ts = time.time()
    ttl_sec = STATUS_RULES_CACHE_TTL_HOURS * 3600
    if os.path.exists(STATUS_RULES_CACHE_PATH):
        try:
            with open(STATUS_RULES_CACHE_PATH, "r") as f:
                data = json.load(f)
            fetched_at = float(data.get("fetched_at", 0))
            rules = data.get("rules", [])
            if now_ts - fetched_at < ttl_sec and rules:
                return tuple((r[0], r[1]) for r in rules if len(r) >= 2)
        except Exception:
            pass

    rules = load_status_rules_from_sheet()
    if rules:
        try:
            with open(STATUS_RULES_CACHE_PATH, "w") as f:
                json.dump(
                    {"fetched_at": now_ts, "rules": [list(r) for r in rules]},
                    f,
                    ensure_ascii=True,
                )
        except Exception:
            pass
    return rules


def status_for_text(text: str, rules: Tuple[Tuple[str, str], ...]) -> Optional[str]:
    t = normalize_text(text)
    for template, status in rules:
        if normalize_text(template) in t:
            return status
    return None


def get_exclusions_cached() -> Tuple[set, set]:
    now_ts = time.time()
    ttl_sec = EXCLUSIONS_CACHE_TTL_HOURS * 3600
    if os.path.exists(EXCLUSIONS_CACHE_PATH):
        try:
            with open(EXCLUSIONS_CACHE_PATH, "r") as f:
                data = json.load(f)
            fetched_at = float(data.get("fetched_at", 0))
            peer_ids = set(data.get("peer_ids", []))
            usernames = set(data.get("usernames", []))
            if now_ts - fetched_at < ttl_sec:
                return peer_ids, usernames
        except Exception:
            pass

    try:
        gc = sheets_client(GOOGLE_CREDS)
        sh = gc.open(SHEET_NAME)
        peer_ids, usernames = load_exclusions(sh, EXCLUDED_WORKSHEET)
    except Exception:
        peer_ids, usernames = set(), set()

    try:
        with open(EXCLUSIONS_CACHE_PATH, "w") as f:
            json.dump(
                {
                    "fetched_at": now_ts,
                    "peer_ids": list(peer_ids),
                    "usernames": list(usernames),
                },
                f,
                ensure_ascii=True,
            )
    except Exception:
        pass

    return peer_ids, usernames


async def main():
    tz = ZoneInfo(TIMEZONE)
    sheet = SheetWriter()
    pause_store = PauseStore()
    group_leads_sheet = GroupLeadsSheet()
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    processing_peers = set()
    paused_peers = set()
    enabled_peers = set()
    last_reply_at = {}
    last_incoming_at = {}
    step_state = StepState(STEP_STATE_PATH)
    status_rules = get_status_rules_cached()
    stop_event = asyncio.Event()
    try:
        enabled_peers = sheet.load_enabled_peers(tz)
    except Exception:
        enabled_peers = set()

    def is_paused(entity: User) -> bool:
        peer_id = entity.id
        if peer_id in paused_peers:
            return True
        username = getattr(entity, "username", "") or ""
        status = pause_store.get_status(peer_id, username)
        if status == "PAUSED":
            paused_peers.add(peer_id)
            return True
        if status == "ACTIVE":
            paused_peers.discard(peer_id)
            return False
        name = getattr(entity, "first_name", "") or "Unknown"
        chat_link = build_chat_link_app(entity, entity.id)
        pause_store.set_status(entity.id, username, name, chat_link, "ACTIVE", updated_by="auto")
        return False

    def handle_stop():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_stop)
        except NotImplementedError:
            pass

    if not acquire_lock(AUTO_REPLY_LOCK, ttl_sec=AUTO_REPLY_LOCK_TTL):
        print("⛔ Автовідповідач вже запущено (lock)")
        return

    if not acquire_lock(SESSION_LOCK, ttl_sec=AUTO_REPLY_LOCK_TTL):
        print("⛔ Телеграм-сесія зайнята (інший процес працює)")
        release_lock(AUTO_REPLY_LOCK)
        return

    await client.start()

    leads_group = await find_group_by_title(client, LEADS_GROUP_TITLE)
    if not leads_group:
        print(f"❌ Не знайшов групу: {LEADS_GROUP_TITLE}")
        await client.disconnect()
        return

    video_group = None
    if VIDEO_GROUP_LINK:
        try:
            video_group = await client.get_entity(VIDEO_GROUP_LINK)
        except Exception:
            video_group = None
    if not video_group and VIDEO_GROUP_TITLE:
        video_group = await find_group_by_title(client, VIDEO_GROUP_TITLE)
    if not video_group:
        print("⚠️ Не знайшов групу з відео")
        video_message = None
    else:
        video_message = None
        async for m in client.iter_messages(video_group, limit=50):
            if m.video or (m.media and getattr(m.media, "document", None)):
                video_message = m
                break
    if not video_message:
        print("⚠️ Не знайшов відео у групі для пересилання")

    async def send_ai_response(
        entity: User,
        status: Optional[str] = None,
    ):
        if is_paused(entity):
            return
        history = await build_ai_history(client, entity, limit=10)
        ai_text = await dialog_suggest(history, "")
        if not ai_text:
            return
        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            ai_text,
            status,
            use_ai=False,
            step_state=step_state,
        )

    async def continue_flow(entity: User, last_step: str, text: str):
        if is_paused(entity):
            return
        if last_step == STEP_CONTACT:
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                INTEREST_TEXT,
                status_for_text(INTEREST_TEXT, status_rules),
                use_ai=True,
                draft=INTEREST_TEXT,
                step_state=step_state,
                step_name=STEP_INTEREST,
            )
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                DATING_TEXT,
                status_for_text(DATING_TEXT, status_rules),
                delay_after=5,
                use_ai=True,
                draft=DATING_TEXT,
                step_state=step_state,
                step_name=STEP_DATING,
            )
            duties_text = await send_and_update(
                client,
                sheet,
                tz,
                entity,
                DUTIES_TEXT,
                status_for_text(DUTIES_TEXT, status_rules),
                delay_after=5,
                use_ai=False,
                draft=DUTIES_TEXT,
                step_state=step_state,
                step_name=STEP_DUTIES,
            )
            if should_send_question(duties_text, CLARIFY_TEXT):
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    CLARIFY_TEXT,
                    status_for_text(CLARIFY_TEXT, status_rules),
                    use_ai=True,
                    draft=CLARIFY_TEXT,
                    step_state=step_state,
                    step_name=STEP_CLARIFY,
                )
            else:
                mark_step_without_send(
                    sheet,
                    tz,
                    entity,
                    status_for_text(CLARIFY_TEXT, status_rules),
                    step_state,
                    STEP_CLARIFY,
                )
            last_reply_at[entity.id] = time.time()
            return

        if last_step == STEP_CLARIFY:
            shifts_text = await send_and_update(
                client,
                sheet,
                tz,
                entity,
                SHIFTS_TEXT,
                status_for_text(SHIFTS_TEXT, status_rules),
                delay_after=5,
                use_ai=False,
                draft=SHIFTS_TEXT,
                step_state=step_state,
                step_name=STEP_SHIFTS,
            )
            if should_send_question(shifts_text, SHIFT_QUESTION_TEXT):
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    SHIFT_QUESTION_TEXT,
                    status_for_text(SHIFT_QUESTION_TEXT, status_rules),
                    use_ai=True,
                    draft=SHIFT_QUESTION_TEXT,
                    step_state=step_state,
                    step_name=STEP_SHIFT_QUESTION,
                )
            else:
                mark_step_without_send(
                    sheet,
                    tz,
                    entity,
                    status_for_text(SHIFT_QUESTION_TEXT, status_rules),
                    step_state,
                    STEP_SHIFT_QUESTION,
                )
            last_reply_at[entity.id] = time.time()
            return

        if last_step == STEP_SHIFT_QUESTION:
            format_text = await send_and_update(
                client,
                sheet,
                tz,
                entity,
                FORMAT_TEXT,
                status_for_text(FORMAT_TEXT, status_rules),
                delay_after=5,
                use_ai=False,
                draft=FORMAT_TEXT,
                step_state=step_state,
                step_name=STEP_FORMAT,
            )
            if should_send_question(format_text, FORMAT_QUESTION_TEXT):
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    FORMAT_QUESTION_TEXT,
                    status_for_text(FORMAT_QUESTION_TEXT, status_rules),
                    use_ai=True,
                    draft=FORMAT_QUESTION_TEXT,
                    step_state=step_state,
                    step_name=STEP_FORMAT_QUESTION,
                )
            else:
                mark_step_without_send(
                    sheet,
                    tz,
                    entity,
                    status_for_text(FORMAT_QUESTION_TEXT, status_rules),
                    step_state,
                    STEP_FORMAT_QUESTION,
                )
            last_reply_at[entity.id] = time.time()
            return

        if last_step == STEP_FORMAT_QUESTION:
            if wants_video(text) and video_message:
                await asyncio.sleep(30)
                try:
                    if video_message.media:
                        await client.send_file(
                            entity,
                            video_message.media,
                            caption=video_message.message or "",
                        )
                    elif video_message.message:
                        await client.send_message(entity, video_message.message)
                except Exception:
                    print("⚠️ Не вдалося надіслати відео")
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    VIDEO_FOLLOWUP_TEXT,
                    status_for_text(VIDEO_FOLLOWUP_TEXT, status_rules),
                    use_ai=True,
                    draft=VIDEO_FOLLOWUP_TEXT,
                    step_state=step_state,
                    step_name=STEP_VIDEO_FOLLOWUP,
                )
                last_reply_at[entity.id] = time.time()
                return

            training_text = await send_and_update(
                client,
                sheet,
                tz,
                entity,
                TRAINING_TEXT,
                status_for_text(TRAINING_TEXT, status_rules),
                delay_after=5,
                use_ai=False,
                draft=TRAINING_TEXT,
                step_state=step_state,
                step_name=STEP_TRAINING,
            )
            if should_send_question(training_text, TRAINING_QUESTION_TEXT):
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    TRAINING_QUESTION_TEXT,
                    status_for_text(TRAINING_QUESTION_TEXT, status_rules),
                    use_ai=True,
                    draft=TRAINING_QUESTION_TEXT,
                    step_state=step_state,
                    step_name=STEP_TRAINING_QUESTION,
                )
            else:
                mark_step_without_send(
                    sheet,
                    tz,
                    entity,
                    status_for_text(TRAINING_QUESTION_TEXT, status_rules),
                    step_state,
                    STEP_TRAINING_QUESTION,
                )
            last_reply_at[entity.id] = time.time()
            return

        if last_step == STEP_VIDEO_FOLLOWUP:
            training_text = await send_and_update(
                client,
                sheet,
                tz,
                entity,
                TRAINING_TEXT,
                status_for_text(TRAINING_TEXT, status_rules),
                delay_after=5,
                use_ai=False,
                draft=TRAINING_TEXT,
                step_state=step_state,
                step_name=STEP_TRAINING,
            )
            if should_send_question(training_text, TRAINING_QUESTION_TEXT):
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    TRAINING_QUESTION_TEXT,
                    status_for_text(TRAINING_QUESTION_TEXT, status_rules),
                    use_ai=True,
                    draft=TRAINING_QUESTION_TEXT,
                    step_state=step_state,
                    step_name=STEP_TRAINING_QUESTION,
                )
            else:
                mark_step_without_send(
                    sheet,
                    tz,
                    entity,
                    status_for_text(TRAINING_QUESTION_TEXT, status_rules),
                    step_state,
                    STEP_TRAINING_QUESTION,
                )
            last_reply_at[entity.id] = time.time()
            return

        if last_step == STEP_TRAINING_QUESTION:
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                FORM_TEXT,
                status_for_text(FORM_TEXT, status_rules),
                use_ai=True,
                draft=FORM_TEXT,
                step_state=step_state,
                step_name=STEP_FORM,
            )
            last_reply_at[entity.id] = time.time()
            return

    @client.on(events.NewMessage(outgoing=True))
    async def on_outgoing_message(event):
        if not event.is_private:
            return
        text = (event.raw_text or "").strip().lower()
        if not text:
            return
        if text not in STOP_COMMANDS and text not in START_COMMANDS:
            return
        peer_id = event.chat_id
        if not peer_id:
            return
        if text in STOP_COMMANDS:
            paused_peers.add(peer_id)
            enabled_peers.discard(peer_id)
        else:
            paused_peers.discard(peer_id)
            enabled_peers.add(peer_id)
        try:
            entity = await event.get_chat()
        except Exception:
            entity = None
        if isinstance(entity, User):
            name = getattr(entity, "first_name", "") or "Unknown"
            username = getattr(entity, "username", "") or ""
            chat_link = build_chat_link_app(entity, entity.id)
            status = "PAUSED" if text in STOP_COMMANDS else "ACTIVE"
            pause_store.set_status(entity.id, username, name, chat_link, status, updated_by="manual")
            sheet.upsert(
                tz=tz,
                peer_id=entity.id,
                name=name,
                username=username,
                chat_link=chat_link,
                auto_reply_enabled=(text in START_COMMANDS),
            )
        else:
            status = "PAUSED" if text in STOP_COMMANDS else "ACTIVE"
            pause_store.set_status(peer_id, None, None, None, status, updated_by="manual")
        try:
            await event.delete()
        except Exception:
            pass

    @client.on(events.NewMessage(chats=leads_group))
    async def on_lead_message(event):
        text = event.raw_text or ""
        try:
            group_data = parse_group_message(text)
            group_status = status_for_text(CONTACT_TEXT, status_rules)
            group_leads_sheet.upsert(tz, group_data, group_status)
        except Exception:
            pass
        username, phone = extract_contact(text)
        if not username and not phone:
            return

        entity = await resolve_contact(client, username, phone)
        if not entity:
            print(f"⏭️ Пропускаю контакт: {username or phone} (немає в контактах)")
            return
        if getattr(entity, "bot", False):
            return
        excluded_ids, excluded_usernames = get_exclusions_cached()
        norm_uname = normalize_username(getattr(entity, "username", "") or "")
        if entity.id in excluded_ids or (norm_uname and norm_uname in excluded_usernames):
            print(f"⏭️ Пропускаю виключеного користувача: {entity.id}")
            return
        if is_paused(entity):
            print(f"⏭️ Призупинено для користувача: {entity.id}")
            return

        if await has_outgoing_template(client, entity, step_state):
            print(f"ℹ️ Вже контактували: {entity.id}")
            return

        enabled_peers.add(entity.id)
        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            CONTACT_TEXT,
            status_for_text(CONTACT_TEXT, status_rules),
            use_ai=True,
            draft=CONTACT_TEXT,
            step_state=step_state,
            step_name=STEP_CONTACT,
            auto_reply_enabled=True,
        )
        print(f"✅ Надіслано перше повідомлення: {entity.id}")

    @client.on(events.NewMessage(incoming=True))
    async def on_private_message(event):
        if not event.is_private:
            return
        sender = await event.get_sender()
        if not isinstance(sender, User) or sender.bot:
            return
        peer_id = sender.id
        excluded_ids, excluded_usernames = get_exclusions_cached()
        norm_uname = normalize_username(getattr(sender, "username", "") or "")
        if peer_id in excluded_ids or (norm_uname and norm_uname in excluded_usernames):
            return
        if is_paused(sender):
            return
        if peer_id not in enabled_peers:
            return
        if peer_id in processing_peers:
            return
        now_ts = time.time()
        last_ts = last_reply_at.get(peer_id)
        if last_ts and now_ts - last_ts < REPLY_DEBOUNCE_SEC:
            return
        processing_peers.add(peer_id)

        try:
            text = event.raw_text or ""
            last_incoming_at[peer_id] = time.time()
            name = getattr(sender, "first_name", "") or "Unknown"
            username = getattr(sender, "username", "") or ""
            chat_link = build_chat_link_app(sender, sender.id)
            sheet.upsert(
                tz=tz,
                peer_id=sender.id,
                name=name,
                username=username,
                chat_link=chat_link,
                last_in=text[:200],
            )

            last_step = await get_last_step(client, sender, step_state)
            if message_has_question(text):
                await send_ai_response(sender, status="знак питання")
                return

            if not last_step:
                return

            if CONTINUE_DELAY_SEC > 0:
                await asyncio.sleep(CONTINUE_DELAY_SEC)
                if is_paused(sender):
                    return
            await continue_flow(sender, last_step, text)
        finally:
            processing_peers.discard(peer_id)

    print("🤖 Автовідповідач запущено")
    try:
        while not stop_event.is_set():
            await asyncio.sleep(0.5)
    finally:
        await client.disconnect()
        release_lock(SESSION_LOCK)
        release_lock(AUTO_REPLY_LOCK)


if __name__ == "__main__":
    asyncio.run(main())
