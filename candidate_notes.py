from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def format_note_entry(tz: ZoneInfo, tag: str, text_block: str) -> str:
    ts = datetime.now(tz).isoformat(timespec="seconds")
    safe = (text_block or "").strip()
    return f"[{ts}] ({tag}) {safe}"


def append_candidate_answers(enqueue_func, peer_id: int, name: str, username: str, chat_link: str, note_entry: str) -> bool:
    payload = {
        "peer_id": peer_id,
        "name": name,
        "username": username,
        "chat_link": chat_link,
        "candidate_note_append": note_entry,
    }
    return bool(enqueue_func("today_upsert", payload))
