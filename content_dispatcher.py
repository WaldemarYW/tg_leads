from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

MESSAGE_LINK_RE = re.compile(r"https?://t\.me/(c/)?([A-Za-z0-9_]+)/([0-9]+)")


@dataclass
class SendResult:
    ok: bool
    message: str = ""
    error: str = ""
    message_ids: List[int] = field(default_factory=list)
    preview: str = ""


def parse_message_link(link: str) -> Optional[Tuple[object, int]]:
    if not link:
        return None
    match = MESSAGE_LINK_RE.search(link.strip())
    if not match:
        return None
    is_private = bool(match.group(1))
    chat_id = match.group(2)
    message_id = int(match.group(3))
    if is_private:
        return int(f"-100{chat_id}"), message_id
    return chat_id, message_id


def validate_content_env(env_map: Dict[str, str]) -> Dict[str, str]:
    missing = []
    for key in ("VOICE_MESSAGE_LINK", "PHOTO_1_MESSAGE_LINK", "PHOTO_2_MESSAGE_LINK", "TEST_TASK_MESSAGE_LINK", "FORM_MESSAGE_LINK"):
        if not (env_map.get(key) or "").strip():
            missing.append(key)
    return {"missing": ",".join(missing)}


async def dispatch_content(client, entity, content_link: str) -> SendResult:
    parsed = parse_message_link(content_link)
    if not parsed:
        return SendResult(ok=False, error="invalid_message_link")
    peer, message_id = parsed
    try:
        source = await client.get_entity(peer)
        msg = await client.get_messages(source, ids=message_id)
        if not msg:
            return SendResult(ok=False, error="source_message_not_found")
        sent = await client.forward_messages(entity, msg, drop_author=True)
        if not sent:
            return SendResult(ok=False, error="forward_failed")
        if isinstance(sent, list):
            ids = [int(getattr(item, "id", 0)) for item in sent if getattr(item, "id", None)]
        else:
            ids = [int(getattr(sent, "id", 0))] if getattr(sent, "id", None) else []
        preview = (getattr(msg, "message", None) or "").strip()
        if not preview:
            if getattr(msg, "photo", None):
                preview = "[forwarded photo]"
            elif getattr(msg, "video", None):
                preview = "[forwarded video]"
            elif getattr(msg, "media", None):
                preview = "[forwarded media]"
            else:
                preview = "[forwarded message]"
        return SendResult(ok=True, message="forwarded", message_ids=ids, preview=preview)
    except Exception as err:
        return SendResult(ok=False, error=f"{type(err).__name__}: {err}")
