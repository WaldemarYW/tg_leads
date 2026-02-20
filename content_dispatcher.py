from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

MESSAGE_LINK_RE = re.compile(r"https?://t\.me/(c/)?([A-Za-z0-9_]+)/([0-9]+)")


@dataclass
class SendResult:
    ok: bool
    message: str = ""
    error: str = ""


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
        return SendResult(ok=True, message="forwarded")
    except Exception as err:
        return SendResult(ok=False, error=f"{type(err).__name__}: {err}")
