import os
import re
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
SESSION_FILE = os.environ["SESSION_FILE"]

SHEET_NAME = os.environ["SHEET_NAME"]
GOOGLE_CREDS = os.environ["GOOGLE_CREDS"]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Kyiv")

LEADS_GROUP_TITLE = os.environ.get("LEADS_GROUP_TITLE", "DATING AGENCY | Referral")
VIDEO_GROUP_LINK = os.environ.get("VIDEO_GROUP_LINK")
VIDEO_GROUP_TITLE = os.environ.get("VIDEO_GROUP_TITLE", "Промо відео")
AUTO_REPLY_LOCK = os.environ.get("AUTO_REPLY_LOCK", "/opt/tg_leads/.auto_reply.lock")
AUTO_REPLY_LOCK_TTL = int(os.environ.get("AUTO_REPLY_LOCK_TTL", "300"))
REPLY_DEBOUNCE_SEC = float(os.environ.get("REPLY_DEBOUNCE_SEC", "3"))

HEADERS = ["date", "name", "chat_link_app", "username", "status", "last_in", "last_out", "peer_id"]

USERNAME_RE = re.compile(r"(?:@|t\.me/)([A-Za-z0-9_]{5,})")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{9,}\d")

VIDEO_WORDS = ("відео", "видео")
CONFIRM_STATUS = "✅ Погодився Дякую! 🙌 Передаю вас на етап навчання"
REFERRAL_STATUS = "🎁 Реферал Також хочу повідомити, що в нашій компанії діє реферальна програма 💰."
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

STEP_STATUS = {
    STEP_CONTACT: "👋 Привітання",
    STEP_INTEREST: None,
    STEP_DATING: None,
    STEP_DUTIES: None,
    STEP_CLARIFY: "🏢 Знайомство з компанією",
    STEP_SHIFTS: None,
    STEP_SHIFT_QUESTION: "🕒 Графік",
    STEP_FORMAT: None,
    STEP_FORMAT_QUESTION: "🎥 Більше інформації",
    STEP_VIDEO_FOLLOWUP: "🎥 Відео",
    STEP_TRAINING: None,
    STEP_TRAINING_QUESTION: "🎓 Навчання",
    STEP_FORM: None,
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


async def main():
    tz = ZoneInfo(TIMEZONE)
    sheet = SheetWriter()
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    processing_peers = set()
    last_reply_at = {}
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

        if await has_outgoing_template(client, entity):
            print(f"ℹ️ Вже контактували: {entity.id}")
            return

        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            CONTACT_TEXT,
            STEP_STATUS[STEP_CONTACT],
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
                return

            last_step = await get_last_outgoing_step(client, sender)
            if last_step == STEP_CONTACT:
                await send_and_update(client, sheet, tz, sender, INTEREST_TEXT, STEP_STATUS[STEP_INTEREST])
                await send_and_update(client, sheet, tz, sender, DATING_TEXT, STEP_STATUS[STEP_DATING], delay_after=5)
                await send_and_update(client, sheet, tz, sender, DUTIES_TEXT, STEP_STATUS[STEP_DUTIES], delay_after=5)
                await send_and_update(client, sheet, tz, sender, CLARIFY_TEXT, STEP_STATUS[STEP_CLARIFY])
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_CLARIFY:
                await send_and_update(client, sheet, tz, sender, SHIFTS_TEXT, STEP_STATUS[STEP_SHIFTS], delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, SHIFT_QUESTION_TEXT, STEP_STATUS[STEP_SHIFT_QUESTION]
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_SHIFT_QUESTION:
                await send_and_update(client, sheet, tz, sender, FORMAT_TEXT, STEP_STATUS[STEP_FORMAT], delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, FORMAT_QUESTION_TEXT, STEP_STATUS[STEP_FORMAT_QUESTION]
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_FORMAT_QUESTION:
                if wants_video(text) and video_message:
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
                        client, sheet, tz, sender, VIDEO_FOLLOWUP_TEXT, STEP_STATUS[STEP_VIDEO_FOLLOWUP]
                    )
                    last_reply_at[peer_id] = time.time()
                    return

                await send_and_update(client, sheet, tz, sender, TRAINING_TEXT, STEP_STATUS[STEP_TRAINING], delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, TRAINING_QUESTION_TEXT, STEP_STATUS[STEP_TRAINING_QUESTION]
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_VIDEO_FOLLOWUP:
                await send_and_update(client, sheet, tz, sender, TRAINING_TEXT, STEP_STATUS[STEP_TRAINING], delay_after=5)
                await send_and_update(
                    client, sheet, tz, sender, TRAINING_QUESTION_TEXT, STEP_STATUS[STEP_TRAINING_QUESTION]
                )
                last_reply_at[peer_id] = time.time()
                return

            if last_step == STEP_TRAINING_QUESTION:
                sheet.upsert(
                    tz=tz,
                    peer_id=sender.id,
                    name=name,
                    username=username,
                    chat_link=chat_link,
                    status=CONFIRM_STATUS,
                )
                await send_and_update(client, sheet, tz, sender, FORM_TEXT, STEP_STATUS[STEP_FORM])
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
        release_lock(AUTO_REPLY_LOCK)


if __name__ == "__main__":
    asyncio.run(main())
