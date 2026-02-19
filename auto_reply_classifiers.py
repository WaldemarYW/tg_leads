import re
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Optional


class Decision(str, Enum):
    STOP = "stop"
    CONTINUE = "continue"
    UNKNOWN = "unknown"


STOP_PHRASES = [
    "не підход",
    "не подходит",
    "не цікаво",
    "не интересно",
    "не актуаль",
    "не хочу",
    "не буду",
    "не готов",
    "не готова",
    "не хочу працювати",
    "не хочу работать",
    "не буду працювати",
    "не буду работать",
    "вже знайш",
    "уже наш",
    "вже маю роботу",
    "уже нашла работу",
    "уже нашел работу",
    "не пишіть",
    "не пишите",
    "не турбуйте",
    "не беспокойте",
    "не потрібно",
    "не нужно",
    "не интересует",
    "не цікавить",
    "не зможу",
    "не смогу",
    "шукаю додатковий заробіток",
    "ищу дополнительный заработок",
    "підробіток",
    "подработ",
    "отпис",
    "stop",
    "unsubscribe",
    "not interested",
    "no thanks",
    "no thank you",
]

CONTINUE_PHRASES = [
    "так",
    "да",
    "ок",
    "добре",
    "хорошо",
    "готов",
    "готова",
    "готовий",
    "готова перейти",
    "продовжуйте",
    "продолжайте",
    "далі",
    "дальше",
    "поїхали",
    "погнали",
    "актуально",
    "цікаво",
    "интересно",
    "питань нема",
    "питань немає",
    "нема питань",
    "немає питань",
    "все зрозуміло",
    "усе зрозуміло",
    "все ясно",
    "усе ясно",
]

VIDEO_WORDS = ("відео", "видео")
FORMAT_VIDEO_WORDS = ("відео", "видео", "video")
FORMAT_MINI_COURSE_WORDS = ("мінікурс", "миникурс", "mini-course", "mini course", "курс", "тренажер", "сайт")


def normalize_text(text: Optional[str]) -> str:
    raw = (text or "").strip().lower()
    if not raw:
        return ""
    return " ".join(raw.split())


def message_has_question(text: str) -> bool:
    if "?" in (text or ""):
        return True
    t = normalize_text(text)
    if not t:
        return False
    return bool(re.search(
        r"^(коли|де|як|який|яка|які|що|чи|скільки|когда|где|как|какой|какая|какие|что|сколько|почему|зачем|можно)\b",
        t,
    ))


def strip_question_trail(text: str) -> str:
    if not text:
        return text
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    if not sentences:
        return text
    cleaned = []
    for sentence in sentences:
        lower = sentence.lower()
        if "?" in sentence:
            break
        if any(word in lower for word in ("зміна", "графік", "формат", "навчання", "анкета")):
            break
        cleaned.append(sentence)
    return " ".join(cleaned).strip() or text.strip()


def is_stop_phrase(text: str) -> bool:
    t = normalize_text(text)
    if not t:
        return False
    if message_has_question(text):
        return False
    if any(
        phrase in t
        for phrase in (
            "питань нема",
            "питань немає",
            "все зрозуміло",
            "усе зрозуміло",
            "все ясно",
            "усе ясно",
            "зрозуміло",
            "зрозуміло, дякую",
            "ок, зрозуміло",
            "ок зрозуміло",
            "ок",
        )
    ):
        return False
    return any(phrase in t for phrase in STOP_PHRASES)


def is_continue_phrase(text: str) -> bool:
    t = normalize_text(text)
    if not t:
        return False
    if message_has_question(text):
        return True
    if re.search(r"пит\w*\s+н\w*ма", t):
        return True
    return any(phrase in t for phrase in CONTINUE_PHRASES)


def is_neutral_ack(text: str) -> bool:
    t = normalize_text(text)
    if not t:
        return False
    if re.search(r"пит\w*\s+н\w*ма", t):
        return True
    return any(
        phrase in t
        for phrase in (
            "питань нема",
            "питань немає",
            "нема питань",
            "немає питань",
            "все зрозуміло",
            "усе зрозуміло",
            "все ясно",
            "усе ясно",
            "зрозуміло",
            "зрозуміло, дякую",
            "ок, зрозуміло",
            "ок зрозуміло",
            "ок",
        )
    )


def should_send_question(sent_text: str, question_text: str, clarify_text: str, shift_question_text: str, format_question_text: str) -> bool:
    if not sent_text:
        return True
    sent_norm = normalize_text(sent_text)
    question_norm = normalize_text(question_text)
    if question_norm in sent_norm:
        return False
    if question_text == clarify_text:
        if "чи все зрозуміло" in sent_norm or "можливо" in sent_norm:
            return False
    if question_text == shift_question_text:
        if "зміна" in sent_norm and "зруч" in sent_norm:
            return False
    if question_text == format_question_text:
        if "формат" in sent_norm and "зруч" in sent_norm:
            return False
    return True


def fallback_format_choice(text: str) -> str:
    t = normalize_text(text)
    has_video = any(word in t for word in FORMAT_VIDEO_WORDS)
    has_mini = any(word in t for word in FORMAT_MINI_COURSE_WORDS)
    if has_video and has_mini:
        return "both"
    if has_video:
        return "video"
    if has_mini:
        return "mini_course"
    return "unknown"


def wants_video(text: str) -> bool:
    t = normalize_text(text)
    return any(word in t for word in VIDEO_WORDS)


async def classify_stop_continue(
    text: str,
    history: list,
    ai_client: Optional[Callable[[list, str], Awaitable[Optional[bool]]]] = None,
) -> Decision:
    if is_continue_phrase(text):
        return Decision.CONTINUE
    if is_stop_phrase(text):
        return Decision.STOP
    if ai_client is None:
        return Decision.UNKNOWN
    ai_decision = await ai_client(history, text)
    if ai_decision is True:
        return Decision.STOP
    if ai_decision is False:
        return Decision.CONTINUE
    return Decision.UNKNOWN


async def classify_format_choice(
    text: str,
    history: list,
    ai_client: Optional[Callable[[list, str], Awaitable[str]]] = None,
) -> str:
    fallback = fallback_format_choice(text)
    if fallback in {"video", "mini_course", "both"}:
        return fallback
    if fallback == "unknown" and (is_neutral_ack(text) or is_continue_phrase(text)):
        return "unknown"
    if ai_client is None:
        return fallback
    ai_choice = (await ai_client(history, text) or "").strip().lower()
    if ai_choice in {"video", "mini_course", "both"}:
        return ai_choice
    return fallback
