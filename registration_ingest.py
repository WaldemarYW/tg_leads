import re
from typing import Dict, Optional

ZERO_WIDTH_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")
USERNAME_RE = re.compile(r"(?:^|\s)@\s*[\u200b\u200c\u200d\ufeff]*([A-Za-z0-9_]{4,})(?=$|\s)")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{8,}\d")
DATE_YEAR_RE = re.compile(r"^\d{1,2}\.\d{2}\.(?:\d{2}|\d{4})$")
DATE_FULL_RE = re.compile(r"\b\d{2}\.\d{2}\.\d{4}\b")
DATE_SHORT_RE = re.compile(r"\b\d{2}\.\d{2}\b")
PEER_LINE_RE = re.compile(r"^[^\d]*?(\d{6,})\s*$")
TIME_RANGE_RE = re.compile(r"\b\d{1,2}\s*(?::\d{2})?\s*(?:до|-|–)\s*\d{1,2}(?::\d{2})?\b", re.IGNORECASE)


def _looks_like_valid_numeric_date(line: str) -> bool:
    raw = str(line or "").strip()
    if not DATE_YEAR_RE.fullmatch(raw):
        return False
    parts = raw.split(".")
    if len(parts) != 3:
        return False
    try:
        day = int(parts[0])
        month = int(parts[1])
    except (TypeError, ValueError):
        return False
    return 1 <= day <= 31 and 1 <= month <= 12


def _clean_line(line: str) -> str:
    line = ZERO_WIDTH_RE.sub("", (line or "")).strip()
    if not line:
        return ""
    if _looks_like_valid_numeric_date(line):
        return line
    # Drop numbering prefixes used in анкета lines.
    line = re.sub(r"^\s*[0-9]\ufe0f?\u20e3\s*", "", line)
    match = re.match(r"^\s*(\d{1,2})[\.)](.+)$", line)
    if match:
        num = int(match.group(1))
        tail = match.group(2).strip()
        dot_count = line.count(".")
        # Keep plain dates like "15.11.1997", but strip prefixes like "2.09.03.2006" or "6. Денна".
        if num <= 10 and (
            dot_count >= 3
            or bool(re.match(r"^[^\d]", tail))
            or bool(DATE_FULL_RE.search(tail))
            or bool(DATE_SHORT_RE.search(tail))
        ):
            line = tail
    return line.strip()


def _looks_like_name(line: str) -> bool:
    if not line:
        return False
    if "@" in line or EMAIL_RE.search(line) or PHONE_RE.search(line):
        return False
    if DATE_FULL_RE.search(line) or TIME_RANGE_RE.search(line):
        return False
    words = [w for w in re.split(r"\s+", line) if w]
    if len(words) < 2 or len(words) > 4:
        return False
    alpha_words = [w for w in words if re.search(r"[A-Za-zА-Яа-яІіЇїЄєҐґ]", w)]
    return len(alpha_words) >= 2


def _looks_like_schedule(line: str) -> bool:
    t = line.lower()
    return bool(
        TIME_RANGE_RE.search(t)
        or "денн" in t
        or "ніч" in t
        or "смен" in t
        or "зміна" in t
        or "графік" in t
    )


def _looks_like_start_date(line: str) -> bool:
    t = line.lower()
    if any(k in t for k in ("сьогодні", "сегодня", "завтра", "з ", "c ")) and DATE_SHORT_RE.search(t):
        return True
    if DATE_YEAR_RE.fullmatch(line.strip()):
        return True
    if DATE_SHORT_RE.search(t) and not DATE_FULL_RE.search(t):
        return True
    return False


def _looks_like_city(line: str) -> bool:
    if not line:
        return False
    t = line.lower()
    if any(ch in t for ch in ("@",)):
        return False
    if EMAIL_RE.search(t) or PHONE_RE.search(t) or DATE_FULL_RE.search(t):
        return False
    if _looks_like_schedule(t) or _looks_like_start_date(t):
        return False
    if t in {"нема", "немає", "нет", "ні", "не має", "так", "є"}:
        return False
    if any(k in t for k in ("нема", "немає", "нет", "не має", "ні,", "ні ", "так", "є ")):
        return False
    words = [w for w in re.split(r"\s+", line) if w]
    if not (1 <= len(words) <= 3):
        return False
    if any(re.search(r"\d", w) for w in words):
        return False
    return all(re.search(r"[A-Za-zА-Яа-яІіЇїЄєҐґ'-]", w) for w in words)


def parse_registration_message(text: str) -> Dict[str, str]:
    raw_text = (text or "").strip()
    normalized_raw_text = ZERO_WIDTH_RE.sub("", raw_text)
    lines = [_clean_line(l) for l in raw_text.splitlines()]
    lines = [l for l in lines if l]

    usernames = ["@" + m.group(1) for m in USERNAME_RE.finditer(normalized_raw_text)]
    admin_tg = usernames[-1] if usernames else ""
    candidate_tg = ""
    for mention in usernames:
        if mention != admin_tg:
            candidate_tg = mention
            break

    email = ""
    phone = ""
    birth_date = ""
    start_date = ""
    schedule = ""
    city = ""
    full_name = ""
    peer_id = ""
    used = set()

    for idx in range(len(lines) - 1, -1, -1):
        line = lines[idx]
        m = PEER_LINE_RE.match(line)
        if m:
            peer_id = m.group(1)
            used.add(idx)
            break

    for idx, line in enumerate(lines):
        if not email:
            m = EMAIL_RE.search(line)
            if m:
                email = m.group(0)
                used.add(idx)
                continue
        if not phone:
            m = PHONE_RE.search(line)
            if m:
                phone = re.sub(r"\s+", "", m.group(0))
                used.add(idx)
                continue

    for idx, line in enumerate(lines):
        if idx in used:
            continue
        if not candidate_tg and USERNAME_RE.search(line):
            mention = USERNAME_RE.search(line)
            val = "@" + mention.group(1)
            if val != admin_tg:
                candidate_tg = val
                used.add(idx)
                continue

    for idx, line in enumerate(lines):
        if idx in used:
            continue
        if not schedule and _looks_like_schedule(line):
            schedule = line
            used.add(idx)
            continue

    schedule_idx = None
    if schedule:
        for idx, line in enumerate(lines):
            if line == schedule:
                schedule_idx = idx
                break

    year_date_candidates = [
        (idx, line)
        for idx, line in enumerate(lines)
        if idx not in used and DATE_YEAR_RE.fullmatch(line)
    ]
    if year_date_candidates:
        if schedule_idx is not None:
            for idx, line in year_date_candidates:
                if idx > schedule_idx and not start_date:
                    start_date = line
                    used.add(idx)
                    break
        for idx, line in year_date_candidates:
            if idx in used:
                continue
            if not birth_date:
                birth_date = line
                used.add(idx)
                break
        for idx, line in year_date_candidates:
            if idx in used:
                continue
            if not start_date:
                start_date = line
                used.add(idx)
                break

    for idx, line in enumerate(lines):
        if idx in used:
            continue
        if not start_date and _looks_like_start_date(line):
            start_date = line
            used.add(idx)
            continue

    for idx, line in enumerate(lines):
        if idx in used:
            continue
        if not full_name and _looks_like_name(line):
            full_name = line
            used.add(idx)
            continue

    for idx, line in enumerate(lines):
        if idx in used:
            continue
        if not city and _looks_like_city(line):
            city = line
            used.add(idx)
            continue

    return {
        "full_name": full_name,
        "birth_date": birth_date,
        "phone": phone,
        "email": email,
        "candidate_tg": candidate_tg,
        "schedule": schedule,
        "start_date": start_date,
        "city": city,
        "admin_tg": admin_tg,
        "peer_id": peer_id,
        "raw_text": raw_text,
    }


def is_media_registration_message(message) -> bool:
    if message is None:
        return False
    if getattr(message, "photo", None):
        return True
    if getattr(message, "document", None):
        return True
    media = getattr(message, "media", None)
    if media and getattr(media, "photo", None):
        return True
    if media and getattr(media, "document", None):
        return True
    return False


def build_message_link(chat_id: Optional[int], message_id: Optional[int]) -> str:
    if not chat_id or not message_id:
        return ""
    if str(chat_id).startswith("-100"):
        return f"https://t.me/c/{str(chat_id)[4:]}/{int(message_id)}"
    return ""
