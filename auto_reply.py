import os
import re
import time
import json
import asyncio
import signal
from datetime import datetime, date
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

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

HEADERS = ["date", "name", "chat_link_app", "username", "status", "last_in", "last_out", "peer_id"]

USERNAME_RE = re.compile(r"(?:@|t\.me/)([A-Za-z0-9_]{5,})")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{9,}\d")

VIDEO_WORDS = ("відео", "видео")
STOP_WORDS = (
    "жаль",
    "нажаль",
    "сожалению",
    "ні",
    "нет",
    "вибачте",
    "извините",
    "шкода",
)

STOP_WORDS_WORKSHEET = os.environ.get("STOP_WORDS_WORKSHEET", "StopWords")
STOP_WORDS_CACHE_PATH = os.environ.get("STOP_WORDS_CACHE_PATH", "/opt/tg_leads/.auto_reply.stop_words.json")
STOP_WORDS_CACHE_TTL_HOURS = int(os.environ.get("STOP_WORDS_CACHE_TTL_HOURS", "48"))
STOP_STATE_PATH = os.environ.get("AUTO_REPLY_STOP_STATE_PATH", "/opt/tg_leads/.auto_reply.stop_state.json")
STOP_STATE_TTL_HOURS = int(os.environ.get("AUTO_REPLY_STOP_TTL_HOURS", "48"))
STATUS_RULES_WORKSHEET = os.environ.get("STATUS_RULES_WORKSHEET", "StatusRules")
STATUS_RULES_CACHE_PATH = os.environ.get("STATUS_RULES_CACHE_PATH", "/opt/tg_leads/.auto_reply.status_rules.json")
STATUS_RULES_CACHE_TTL_HOURS = int(os.environ.get("STATUS_RULES_CACHE_TTL_HOURS", "48"))
EXCLUDED_WORKSHEET = os.environ.get("EXCLUDED_WORKSHEET", "Excluded")
EXCLUSIONS_CACHE_PATH = os.environ.get("EXCLUSIONS_CACHE_PATH", "/opt/tg_leads/.auto_reply.exclusions.json")
EXCLUSIONS_CACHE_TTL_HOURS = int(os.environ.get("EXCLUSIONS_CACHE_TTL_HOURS", "6"))
CONFIRM_STATUS = "✅ Погодився"
REFERRAL_STATUS = "🎁 Реферал"
IMMUTABLE_STATUSES = {CONFIRM_STATUS, REFERRAL_STATUS}

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

STEP_STATUS = {}


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

    def upsert(
        self,
        tz: ZoneInfo,
        peer_id: int,
        name: str,
        username: str,
        chat_link: str,
        status: Optional[str] = None,
        last_in: Optional[str] = None,
        last_out: Optional[str] = None,
    ):
        ws = self.get_ws(tz)
        row_idx, existing = self._find_row(ws, peer_id)
        existing = existing or [""] * len(HEADERS)
        existing_status = existing[4] if len(existing) > 4 else ""

        def take(value: Optional[str], idx: int) -> str:
            if value is not None:
                return value
            return existing[idx] if idx < len(existing) else ""

        if existing_status in IMMUTABLE_STATUSES:
            status = existing_status

        row = [
            str(datetime.now(tz).date()),
            name,
            chat_link,
            ("@" + username) if username else "",
            take(status, 4),
            take(last_in, 5),
            take(last_out, 6),
            str(peer_id),
        ]

        if row_idx:
            ws.update(f"A{row_idx}:H{row_idx}", [row], value_input_option="USER_ENTERED")
        else:
            ws.append_row(row, value_input_option="USER_ENTERED")


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


async def has_outgoing_template(client: TelegramClient, entity: User) -> bool:
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
):
    await client.send_message(entity, text)
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
                    "text_preview": text[:200],
                },
                f,
                ensure_ascii=True,
            )
    except Exception:
        pass
    sheet.upsert(
        tz=tz,
        peer_id=entity.id,
        name=name,
        username=username,
        chat_link=chat_link,
        status=status,
        last_out=text[:200],
    )
    if delay_after:
        await asyncio.sleep(delay_after)


def wants_video(text: str) -> bool:
    t = normalize_text(text)
    return any(word in t for word in VIDEO_WORDS)


def has_stop_words(text: str, stop_words: Tuple[str, ...]) -> bool:
    t = normalize_text(text)
    return any(word in t for word in stop_words)


def load_stop_words_from_sheet() -> Tuple[str, ...]:
    try:
        gc = sheets_client(GOOGLE_CREDS)
        sh = gc.open(SHEET_NAME)
        ws = get_or_create_worksheet(sh, STOP_WORDS_WORKSHEET, rows=1000, cols=1)
        values = ws.col_values(1)
        words = [normalize_text(v) for v in values if normalize_text(v)]
        if words:
            return tuple(words)
        rows = [[w] for w in STOP_WORDS]
        if rows:
            ws.append_rows(rows, value_input_option="USER_ENTERED")
        return STOP_WORDS
    except Exception:
        return STOP_WORDS


def get_stop_words_cached() -> Tuple[str, ...]:
    now_ts = time.time()
    ttl_sec = STOP_WORDS_CACHE_TTL_HOURS * 3600
    if os.path.exists(STOP_WORDS_CACHE_PATH):
        try:
            with open(STOP_WORDS_CACHE_PATH, "r") as f:
                data = json.load(f)
            fetched_at = float(data.get("fetched_at", 0))
            words = data.get("words", [])
            if now_ts - fetched_at < ttl_sec and words:
                return tuple(words)
        except Exception:
            pass

    words = load_stop_words_from_sheet()
    try:
        with open(STOP_WORDS_CACHE_PATH, "w") as f:
            json.dump({"fetched_at": now_ts, "words": list(words)}, f, ensure_ascii=True)
    except Exception:
        pass
    return words


class StopState:
    def __init__(self, path: str, ttl_hours: int):
        self.path = path
        self.ttl_sec = ttl_hours * 3600
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

    def _cleanup(self):
        now_ts = time.time()
        expired = [k for k, v in self.data.items() if now_ts - float(v) > self.ttl_sec]
        for k in expired:
            self.data.pop(k, None)
        if expired:
            self._save()

    def is_stopped(self, peer_id: int) -> bool:
        self._cleanup()
        return str(peer_id) in self.data

    def stop_at(self, peer_id: int) -> Optional[float]:
        self._cleanup()
        value = self.data.get(str(peer_id))
        return float(value) if value is not None else None

    def set_stop(self, peer_id: int):
        self.data[str(peer_id)] = time.time()
        self._save()

    def clear_stop(self, peer_id: int):
        if str(peer_id) in self.data:
            self.data.pop(str(peer_id), None)
            self._save()


async def last_outgoing_is_non_template(client: TelegramClient, entity: User) -> bool:
    async for m in client.iter_messages(entity, limit=20):
        if not m.message or not m.out:
            continue
        return not is_script_template(m.message)
    return False


async def get_last_outgoing_message(client: TelegramClient, entity: User):
    async for m in client.iter_messages(entity, limit=20):
        if not m.message or not m.out:
            continue
        return m
    return None


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
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    processing_peers = set()
    last_reply_at = {}
    stop_state = StopState(STOP_STATE_PATH, STOP_STATE_TTL_HOURS)
    status_rules = get_status_rules_cached()
    stop_event = asyncio.Event()

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

    @client.on(events.NewMessage(chats=leads_group))
    async def on_lead_message(event):
        text = event.raw_text or ""
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

        if await has_outgoing_template(client, entity):
            print(f"ℹ️ Вже контактували: {entity.id}")
            return

        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            CONTACT_TEXT,
            status_for_text(CONTACT_TEXT, status_rules),
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
        if peer_id in processing_peers:
            return
        now_ts = time.time()
        last_ts = last_reply_at.get(peer_id)
        if last_ts and now_ts - last_ts < REPLY_DEBOUNCE_SEC:
            return
        processing_peers.add(peer_id)

        try:
            text = event.raw_text or ""
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

            if message_has_question(text):
                sheet.upsert(
                    tz=tz,
                    peer_id=peer_id,
                    name=name,
                    username=username,
                    chat_link=chat_link,
                    status="знак питання",
                )
                return

            stop_words = get_stop_words_cached()
            if has_stop_words(text, stop_words):
                stop_state.set_stop(peer_id)
                sheet.upsert(
                    tz=tz,
                    peer_id=peer_id,
                    name=name,
                    username=username,
                    chat_link=chat_link,
                    status="стоп слово",
                )
                return

            if stop_state.is_stopped(peer_id):
                last_out = await get_last_outgoing_message(client, sender)
                stop_at = stop_state.stop_at(peer_id)
                if last_out and last_out.date and stop_at:
                    if last_out.date.timestamp() > stop_at and is_script_template(last_out.message):
                        stop_state.clear_stop(peer_id)
                    else:
                        return
                else:
                    return

            if await last_outgoing_is_non_template(client, sender):
                stop_state.set_stop(peer_id)
                sheet.upsert(
                    tz=tz,
                    peer_id=peer_id,
                    name=name,
                    username=username,
                    chat_link=chat_link,
                    status="користувач",
                )
                return

            last_step = await get_last_outgoing_step(client, sender)
            if not last_step:
                return

            await asyncio.sleep(10)
            if last_step == STEP_CONTACT:
                await send_and_update(client, sheet, tz, sender, INTEREST_TEXT, status_for_text(INTEREST_TEXT, status_rules))
                await send_and_update(client, sheet, tz, sender, DATING_TEXT, status_for_text(DATING_TEXT, status_rules), delay_after=5)
                await send_and_update(client, sheet, tz, sender, DUTIES_TEXT, status_for_text(DUTIES_TEXT, status_rules), delay_after=5)
                await send_and_update(client, sheet, tz, sender, CLARIFY_TEXT, status_for_text(CLARIFY_TEXT, status_rules))
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_CLARIFY:
                await send_and_update(client, sheet, tz, sender, SHIFTS_TEXT, status_for_text(SHIFTS_TEXT, status_rules), delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, SHIFT_QUESTION_TEXT, status_for_text(SHIFT_QUESTION_TEXT, status_rules)
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_SHIFT_QUESTION:
                await send_and_update(client, sheet, tz, sender, FORMAT_TEXT, status_for_text(FORMAT_TEXT, status_rules), delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, FORMAT_QUESTION_TEXT, status_for_text(FORMAT_QUESTION_TEXT, status_rules)
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_FORMAT_QUESTION:
                if wants_video(text) and video_message:
                    await asyncio.sleep(30)
                    try:
                        if video_message.media:
                            await client.send_file(
                                sender,
                                video_message.media,
                                caption=video_message.message or "",
                            )
                        elif video_message.message:
                            await client.send_message(sender, video_message.message)
                    except Exception:
                        print("⚠️ Не вдалося надіслати відео")
                    await send_and_update(
                        client, sheet, tz, sender, VIDEO_FOLLOWUP_TEXT, status_for_text(VIDEO_FOLLOWUP_TEXT, status_rules)
                    )
                    last_reply_at[peer_id] = time.time()
                    return

                await send_and_update(client, sheet, tz, sender, TRAINING_TEXT, status_for_text(TRAINING_TEXT, status_rules), delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, TRAINING_QUESTION_TEXT, status_for_text(TRAINING_QUESTION_TEXT, status_rules)
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_VIDEO_FOLLOWUP:
                await send_and_update(client, sheet, tz, sender, TRAINING_TEXT, status_for_text(TRAINING_TEXT, status_rules), delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, TRAINING_QUESTION_TEXT, status_for_text(TRAINING_QUESTION_TEXT, status_rules)
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_TRAINING_QUESTION:
                await send_and_update(client, sheet, tz, sender, FORM_TEXT, status_for_text(FORM_TEXT, status_rules))
                last_reply_at[peer_id] = time.time()
                return
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
