import os
import re
import sys
import json
import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Set, Optional, Tuple, Dict, List
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv("/opt/tg_leads/.env")

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from telethon import TelegramClient
from telethon.tl.types import User as TgUser

from tg_to_sheets import acquire_lock, release_lock

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_FILE = os.environ.get("SESSION_FILE")
EXPORT_DAYS = int(os.environ.get("EXPORT_DAYS", "90"))
STATE_DIR = os.environ.get("TG_LEADS_STATE_DIR", "/opt/tg_leads/state")
EXPORT_DIR_BASE = os.environ.get("EXPORT_DIR", "/opt/tg_leads/exports")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
AUTO_REPLY_PROCESS: Dict[str, subprocess.Popen] = {}

AUTO_REPLY_PATH = os.environ.get("AUTO_REPLY_PATH", "auto_reply.py")
AUTO_REPLY_CMD = os.environ.get("AUTO_REPLY_CMD")


@dataclass
class AccountConfig:
    key: str
    name: str
    env_prefix: str
    title: str
    session_file: str
    auto_reply_session_file: str
    session_lock: str
    export_lock_path: str
    export_dir: str
    auto_reply_lock: str
    auto_reply_status_path: str
    auto_reply_followup_state_path: str
    auto_reply_step_state_path: str
    auto_reply_paused_state_path: str


def env_key(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", name).upper()


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def make_account_key(name: str, used: Set[str]) -> str:
    base = re.sub(r"[^a-zA-Z0-9_]+", "_", name.strip().lower())[:24] or "acct"
    key = base
    i = 2
    while key in used:
        key = f"{base}_{i}"
        i += 1
    return key


def load_accounts() -> List[AccountConfig]:
    raw = os.environ.get("ACCOUNTS", "default")
    names = [n.strip() for n in raw.split(",") if n.strip()]
    if not names:
        names = ["default"]

    used_keys: Set[str] = set()
    accounts: List[AccountConfig] = []
    for name in names:
        key = make_account_key(name, used_keys)
        used_keys.add(key)
        env_prefix = f"ACCOUNT_{env_key(name)}_"

        session_file = os.environ.get(env_prefix + "SESSION_FILE")
        if not session_file and name == "default":
            session_file = SESSION_FILE
        if not session_file:
            raise RuntimeError(f"Missing session for account '{name}'. Set {env_prefix}SESSION_FILE")

        auto_reply_session_file = os.environ.get(env_prefix + "AUTO_REPLY_SESSION_FILE", session_file)
        title = os.environ.get(env_prefix + "TITLE", name)

        state_dir = os.environ.get(env_prefix + "STATE_DIR", STATE_DIR)
        export_dir = os.environ.get(env_prefix + "EXPORT_DIR", os.path.join(EXPORT_DIR_BASE, key))
        ensure_dir(state_dir)
        ensure_dir(export_dir)

        accounts.append(AccountConfig(
            key=key,
            name=name,
            env_prefix=env_prefix,
            title=title,
            session_file=session_file,
            auto_reply_session_file=auto_reply_session_file,
            session_lock=os.path.join(state_dir, f"telethon_{key}.lock"),
            export_lock_path=os.path.join(state_dir, f"export_{key}.lock"),
            export_dir=export_dir,
            auto_reply_lock=os.path.join(state_dir, f"auto_reply_{key}.lock"),
            auto_reply_status_path=os.path.join(state_dir, f"auto_reply_{key}.status"),
            auto_reply_followup_state_path=os.path.join(state_dir, f"auto_reply_{key}.followup_state.json"),
            auto_reply_step_state_path=os.path.join(state_dir, f"auto_reply_{key}.step_state.json"),
            auto_reply_paused_state_path=os.path.join(state_dir, f"auto_reply_{key}.paused.json"),
        ))
    return accounts


ACCOUNTS = load_accounts()
ACCOUNTS_BY_KEY = {a.key: a for a in ACCOUNTS}
DEFAULT_ACCOUNT = ACCOUNTS[0]
ACCOUNTS_STATE_PATH = os.path.join(STATE_DIR, "accounts_state.json")


def load_accounts_state() -> Dict[str, bool]:
    try:
        with open(ACCOUNTS_STATE_PATH, "r") as f:
            data = json.load(f)
        return {str(k): bool(v) for k, v in data.items()}
    except Exception:
        return {}


def save_accounts_state(state: Dict[str, bool]):
    ensure_dir(STATE_DIR)
    with open(ACCOUNTS_STATE_PATH, "w") as f:
        json.dump(state, f)


def is_account_enabled(account: AccountConfig) -> bool:
    state = load_accounts_state()
    return state.get(account.key, True)


def set_account_enabled(account: AccountConfig, enabled: bool):
    state = load_accounts_state()
    state[account.key] = bool(enabled)
    save_accounts_state(state)


def kb_main():
    kb = types.InlineKeyboardMarkup()
    for acct in ACCOUNTS:
        enabled = is_account_enabled(acct)
        running = auto_reply_running(acct)
        status_icon = "✅" if enabled else "⛔"
        run_icon = "▶️" if running else "⏸"
        label = f"{status_icon} {run_icon} {acct.title}"
        kb.add(types.InlineKeyboardButton(label, callback_data=f"acct:{acct.key}:menu"))
    return kb


def kb_account(acct: AccountConfig):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("▶️ Старт авто", callback_data=f"acct:{acct.key}:auto_start"))
    kb.add(types.InlineKeyboardButton("⏹ Стоп авто", callback_data=f"acct:{acct.key}:auto_stop"))
    kb.add(types.InlineKeyboardButton("📊 Статус авто", callback_data=f"acct:{acct.key}:auto_status"))
    kb.add(types.InlineKeyboardButton("🧠 Експорт чатів (3 міс.)", callback_data=f"acct:{acct.key}:export_chats"))
    toggle_label = "⏼ Вимкнути акаунт" if is_account_enabled(acct) else "⏼ Увімкнути акаунт"
    kb.add(types.InlineKeyboardButton(toggle_label, callback_data=f"acct:{acct.key}:toggle"))
    kb.add(types.InlineKeyboardButton("⬅️ Назад", callback_data="acct:back"))
    return kb


def auto_reply_running(acct: AccountConfig) -> bool:
    proc = AUTO_REPLY_PROCESS.get(acct.key)
    return proc is not None and proc.poll() is None


def start_auto_reply(acct: AccountConfig) -> Tuple[bool, str]:
    if auto_reply_running(acct):
        return False, "Автовідповідач вже запущено для цього акаунта"
    if not is_account_enabled(acct):
        return False, "Акаунт вимкнено"

    if AUTO_REPLY_CMD:
        cmd = AUTO_REPLY_CMD.split()
    else:
        cmd = [sys.executable, AUTO_REPLY_PATH]
    try:
        env = os.environ.copy()
        env["AUTO_REPLY_SESSION_FILE"] = acct.auto_reply_session_file
        env["TELETHON_SESSION_LOCK"] = acct.session_lock
        env["AUTO_REPLY_LOCK"] = acct.auto_reply_lock
        env["AUTO_REPLY_STATUS_PATH"] = acct.auto_reply_status_path
        env["AUTO_REPLY_FOLLOWUP_STATE_PATH"] = acct.auto_reply_followup_state_path
        env["AUTO_REPLY_STEP_STATE_PATH"] = acct.auto_reply_step_state_path
        env["AUTO_REPLY_PAUSED_STATE_PATH"] = acct.auto_reply_paused_state_path
        env["AUTO_REPLY_ACCOUNT_KEY"] = acct.key
        for key in (
            "BOT_REPLY_DELAY_SEC",
            "REPLY_DEBOUNCE_SEC",
            "QUESTION_GAP_SEC",
            "QUESTION_RESPONSE_DELAY_SEC",
            "AUTO_REPLY_CONTINUE_DELAY_SEC",
            "AUTO_REPLY_LOCK_TTL",
            "AUTO_REPLY_FOLLOWUP_CHECK_SEC",
            "FOLLOWUP_WINDOW_START_HOUR",
            "FOLLOWUP_WINDOW_END_HOUR",
            "VIDEO_MESSAGE_LINK",
            "VIDEO_GROUP_LINK",
            "VIDEO_GROUP_TITLE",
            "VIDEO_CACHE_PATH",
            "TODAY_WORKSHEET",
            "HISTORY_SHEET_PREFIX",
            "GROUP_LEADS_WORKSHEET",
            "HISTORY_RETENTION_MONTHS",
        ):
            val = os.environ.get(acct.env_prefix + key)
            if val is not None:
                env[key] = val
        AUTO_REPLY_PROCESS[acct.key] = subprocess.Popen(cmd, env=env)
        return True, "✅ Автовідповідач запущено"
    except Exception:
        AUTO_REPLY_PROCESS.pop(acct.key, None)
        return False, "❌ Не вдалося запустити автовідповідач"


def stop_auto_reply(acct: AccountConfig) -> Tuple[bool, str]:
    proc = AUTO_REPLY_PROCESS.get(acct.key)
    if not proc or proc.poll() is not None:
        AUTO_REPLY_PROCESS.pop(acct.key, None)
        return False, "Автовідповідач не запущено для цього акаунта"
    try:
        proc.terminate()
        proc.wait(timeout=5)
        AUTO_REPLY_PROCESS.pop(acct.key, None)
        return True, "⏹ Автовідповідач зупинено"
    except Exception:
        return False, "❌ Не вдалося зупинити автовідповідач"


def read_auto_status(acct: AccountConfig) -> str:
    running = auto_reply_running(acct)
    if not os.path.exists(acct.auto_reply_status_path):
        return "📊 Автовідповідач: " + ("працює" if running else "зупинено") + "\nДані про останню відправку відсутні"
    try:
        with open(acct.auto_reply_status_path, "r") as f:
            data = json.load(f)
    except Exception:
        return "📊 Автовідповідач: " + ("працює" if running else "зупинено") + "\nНе вдалося прочитати статус"

    last_at = data.get("last_sent_at", "—")
    peer_id = data.get("peer_id", "—")
    username = data.get("username", "")
    name = data.get("name", "")
    who = (f"@{username}" if username else "") or name or str(peer_id)
    preview = data.get("text_preview", "")
    return (
        "📊 Автовідповідач: "
        + ("працює" if running else "зупинено")
        + f"\nОстання відправка: {last_at}\nКому: {who}\nPeer ID: {peer_id}\nТекст: {preview}"
    )


def normalize_message_text(text: str) -> str:
    return " ".join((text or "").split())


async def export_recent_chats(acct: AccountConfig) -> Tuple[Optional[str], Optional[str]]:
    if not is_account_enabled(acct):
        return None, "Акаунт вимкнено"
    if not acquire_lock(acct.export_lock_path, ttl_sec=1800):
        return None, "⏳ Експорт уже виконується. Спробуйте пізніше."
    if not acquire_lock(acct.session_lock, ttl_sec=300):
        release_lock(acct.export_lock_path)
        return None, "⏳ Телеграм-сесія зайнята. Зупиніть авто і спробуйте ще раз."
    os.makedirs(acct.export_dir, exist_ok=True)
    tz = ZoneInfo(os.environ.get("TIMEZONE", "Europe/Kyiv"))
    cutoff = datetime.now(tz) - timedelta(days=EXPORT_DAYS)
    stamp = datetime.now(tz).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(acct.export_dir, f"chats_export_{stamp}.txt")

    client = TelegramClient(acct.session_file, API_ID, API_HASH)
    try:
        await client.start()
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"Export generated: {datetime.now(tz).isoformat(timespec='seconds')}\n")
            f.write(f"Period: last {EXPORT_DAYS} days\n\n")
            async for dialog in client.iter_dialogs():
                if not dialog.is_user:
                    continue
                entity = dialog.entity
                if isinstance(entity, TgUser) and getattr(entity, "bot", False):
                    continue
                messages = []
                async for m in client.iter_messages(entity):
                    if not m.message:
                        continue
                    msg_dt = m.date.astimezone(tz) if m.date else None
                    if msg_dt and msg_dt < cutoff:
                        break
                    messages.append(m)
                if not messages:
                    continue
                name_parts = [
                    getattr(entity, "first_name", "") or "",
                    getattr(entity, "last_name", "") or "",
                ]
                name = " ".join(p for p in name_parts if p).strip() or (dialog.name or "")
                username = getattr(entity, "username", "") or ""
                header = f"=== CHAT: {name} {('@' + username) if username else ''} (id {entity.id}) ===\n"
                f.write(header)
                for m in reversed(messages):
                    msg_dt = m.date.astimezone(tz) if m.date else None
                    ts = msg_dt.strftime("%Y-%m-%d %H:%M") if msg_dt else "unknown time"
                    sender = "me" if m.out else "candidate"
                    text = normalize_message_text(m.message)
                    f.write(f"{ts} [{sender}]: {text}\n")
                f.write("\n")
        return out_path, None
    except Exception:
        return None, "❌ Не вдалося сформувати експорт."
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass
        release_lock(acct.session_lock)
        release_lock(acct.export_lock_path)


@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    await message.answer("Готово 👇", reply_markup=kb_main())


@dp.callback_query_handler(lambda c: c.data.startswith("acct:"))
async def cb_account_actions(call: types.CallbackQuery):
    acct, action = parse_account_callback(call.data)
    if action == "back":
        await call.answer()
        await call.message.reply("Головне меню 👇", reply_markup=kb_main())
        return
    if not acct:
        await call.answer("Невідомий акаунт", show_alert=True)
        return

    if action == "menu":
        enabled = is_account_enabled(acct)
        running = auto_reply_running(acct)
        await call.answer()
        await call.message.reply(
            f"Акаунт: {acct.title}\nСтатус: {'увімкнено' if enabled else 'вимкнено'}\nАвто: {'працює' if running else 'зупинено'}",
            reply_markup=kb_account(acct),
        )
        return

    if action == "toggle":
        enabled = is_account_enabled(acct)
        if enabled:
            stop_auto_reply(acct)
        set_account_enabled(acct, not enabled)
        await call.answer()
        await call.message.reply(
            f"Акаунт {acct.title} {'увімкнено' if not enabled else 'вимкнено'}",
            reply_markup=kb_account(acct),
        )
        return

    if action == "auto_start":
        ok, msg = start_auto_reply(acct)
        await call.answer()
        await call.message.reply(msg)
        return

    if action == "auto_stop":
        ok, msg = stop_auto_reply(acct)
        await call.answer()
        await call.message.reply(msg)
        return

    if action == "auto_status":
        msg = read_auto_status(acct)
        await call.answer()
        await call.message.reply(msg)
        return

    if action == "export_chats":
        await call.answer()
        await call.message.reply("⏳ Готую експорт чатів за 3 місяці…")
        path, err = await export_recent_chats(acct)
        if err:
            await call.message.reply(err)
            return
        try:
            await call.message.reply_document(types.InputFile(path), caption="✅ Експорт готовий")
        except Exception:
            await call.message.reply("❌ Не вдалося надіслати файл експорту")
        return

    await call.answer("Невідома дія", show_alert=True)

def parse_account_callback(data: str) -> Tuple[Optional[AccountConfig], Optional[str]]:
    if not data.startswith("acct:"):
        return None, None
    if data == "acct:back":
        return None, "back"
    parts = data.split(":", 2)
    if len(parts) < 3:
        return None, None
    key = parts[1]
    action = parts[2]
    acct = ACCOUNTS_BY_KEY.get(key)
    return acct, action


@dp.callback_query_handler(lambda c: c.data == "auto_start")
async def cb_auto_start(call: types.CallbackQuery):
    ok, msg = start_auto_reply(DEFAULT_ACCOUNT)
    await call.answer()
    await call.message.reply(msg)


@dp.callback_query_handler(lambda c: c.data == "auto_stop")
async def cb_auto_stop(call: types.CallbackQuery):
    ok, msg = stop_auto_reply(DEFAULT_ACCOUNT)
    await call.answer()
    await call.message.reply(msg)


@dp.callback_query_handler(lambda c: c.data == "auto_status")
async def cb_auto_status(call: types.CallbackQuery):
    msg = read_auto_status(DEFAULT_ACCOUNT)
    await call.answer()
    await call.message.reply(msg)


@dp.callback_query_handler(lambda c: c.data == "export_chats")
async def cb_export_chats(call: types.CallbackQuery):
    await call.answer()
    await call.message.reply("⏳ Готую експорт чатів за 3 місяці…")
    path, err = await export_recent_chats(DEFAULT_ACCOUNT)
    if err:
        await call.message.reply(err)
        return
    try:
        await call.message.reply_document(types.InputFile(path), caption="✅ Експорт готовий")
    except Exception:
        await call.message.reply("❌ Не вдалося надіслати файл експорту")


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
