import re
from typing import Optional, Tuple


INVITE_LINK_RE = re.compile(r"^https?://t\.me/(?:\+|joinchat/)([A-Za-z0-9_-]+)$", re.IGNORECASE)


def extract_invite_hash(target: Optional[str]) -> str:
    raw = (target or "").strip().rstrip("/")
    if not raw:
        return ""
    match = INVITE_LINK_RE.match(raw)
    if not match:
        return ""
    return match.group(1)


async def resolve_group_target_entity(client, tl_functions, target: str):
    cleaned = (target or "").strip().rstrip("/")
    if not cleaned:
        return None, "", "empty_target"
    invite_hash = extract_invite_hash(cleaned)
    if invite_hash:
        try:
            result = await client(tl_functions.messages.CheckChatInviteRequest(hash=invite_hash))
        except Exception as err:
            return None, cleaned, f"{type(err).__name__}: {err}"
        chat = getattr(result, "chat", None)
        if chat is None:
            return None, cleaned, "invite_not_joined"
        return chat, cleaned, ""
    try:
        entity = await client.get_entity(cleaned)
    except Exception as err:
        return None, cleaned, f"{type(err).__name__}: {err}"
    username = getattr(entity, "username", "") or ""
    if username:
        return entity, f"@{username}", ""
    return entity, cleaned, ""
