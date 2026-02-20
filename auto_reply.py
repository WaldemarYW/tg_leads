import os
import re
import time
import json
import asyncio
import signal
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple
from collections import deque
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
    MINI_COURSE_LINK,
    MINI_COURSE_FOLLOWUP_TEXT,
    BOTH_FORMATS_FOLLOWUP_TEXT,
    TRAINING_TEXT,
    TRAINING_QUESTION_TEXT,
    FORM_TEXT,
    CONFIRM_TEXT,
    REFERRAL_TEXT,
)
from auto_reply_classifiers import (
    Intent,
    classify_intent,
    classify_format_choice,
    fallback_format_choice as fallback_format_choice_impl,
    is_continue_phrase as is_continue_phrase_impl,
    is_neutral_ack as is_neutral_ack_impl,
    is_short_neutral_ack,
    is_stop_phrase as is_stop_phrase_impl,
    message_has_question as message_has_question_impl,
    should_send_question as should_send_question_impl,
    strip_question_trail as strip_question_trail_impl,
    wants_video as wants_video_impl,
)
from auto_reply_flow import (
    FlowContext,
    STEP_CLARIFY,
    STEP_CONTACT,
    STEP_DATING,
    STEP_DUTIES,
    STEP_FORMAT,
    STEP_FORMAT_QUESTION,
    STEP_INTEREST,
    STEP_ORDER,
    STEP_SHIFT_QUESTION,
    STEP_SHIFTS,
    STEP_TRAINING,
    STEP_TRAINING_QUESTION,
    STEP_VIDEO_FOLLOWUP,
    STEP_FORM,
    advance_flow,
    send_message_with_fallback,
)
from auto_reply_state import (
    FollowupState as FollowupStateStore,
    LocalPauseStore as LocalPauseStoreStore,
    StepState as StepStateStore,
    adjust_to_followup_window as adjust_to_followup_window_impl,
    within_followup_window as within_followup_window_impl,
)
from gspread.exceptions import APIError
from registration_ingest import (
    build_message_link as build_registration_message_link,
    is_media_registration_message,
    parse_registration_message,
)
from sheets_queue import SheetsQueueStore, calculate_backoff_sec
from flow_engine import (
    PeerRuntimeState,
    STEP_AGE_REJECTED,
    STEP_COMPANY_INTRO,
    STEP_FORM_FORWARD,
    STEP_HANDOFF,
    STEP_PROOF_FORWARD,
    STEP_SCHEDULE_BLOCK,
    STEP_SCHEDULE_SHIFT_WAIT,
    STEP_SCHEDULE_CONFIRM,
    STEP_SCREENING_WAIT,
    STEP_TEST_REVIEW,
    STEP_VOICE_WAIT,
    VOICE_AUTO_ADVANCED,
    VOICE_FALLBACK_SENT,
    VOICE_SENT,
    advance_flow as advance_flow_v2,
)
from intent_router import detect_intent as detect_intent_v2
from faq_service import answer_from_faq, build_cluster_key, normalize_question
from content_dispatcher import dispatch_content, validate_content_env
from candidate_notes import append_candidate_answers
from faq_learning import build_question_log
from v2_state import V2EnrollmentStore, V2RuntimeStore

load_dotenv("/opt/tg_leads/.env")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_FILE = os.environ.get("AUTO_REPLY_SESSION_FILE", os.environ["SESSION_FILE"])

SHEET_NAME = os.environ["SHEET_NAME"]
GOOGLE_CREDS = os.environ["GOOGLE_CREDS"]
TIMEZONE = os.environ.get("TIMEZONE", "Europe/Kyiv")

LEADS_GROUP_TITLE = os.environ.get("LEADS_GROUP_TITLE", "DATING AGENCY | Referral")
TRAFFIC_GROUP_TITLE = os.environ.get("TRAFFIC_GROUP_TITLE", "ТРАФИК FURIOZA")
VIDEO_GROUP_LINK = os.environ.get("VIDEO_GROUP_LINK")
VIDEO_GROUP_TITLE = os.environ.get("VIDEO_GROUP_TITLE", "Промо відео")
VIDEO_MESSAGE_LINK = os.environ.get("VIDEO_MESSAGE_LINK")
VIDEO_CACHE_PATH = os.environ.get("VIDEO_CACHE_PATH", "/opt/tg_leads/.video_cache.json")
AUTO_REPLY_LOCK = os.environ.get("AUTO_REPLY_LOCK", "/opt/tg_leads/.auto_reply.lock")
AUTO_REPLY_LOCK_TTL = int(os.environ.get("AUTO_REPLY_LOCK_TTL", "300"))
REPLY_DEBOUNCE_SEC = float(os.environ.get("REPLY_DEBOUNCE_SEC", "3"))
BOT_REPLY_DELAY_SEC = float(os.environ.get("BOT_REPLY_DELAY_SEC", "5"))
QUESTION_GAP_SEC = float(os.environ.get("QUESTION_GAP_SEC", "5"))
QUESTION_RESPONSE_DELAY_SEC = float(os.environ.get("QUESTION_RESPONSE_DELAY_SEC", "10"))
QUESTION_RESUME_DELAY_SEC = float(os.environ.get("QUESTION_RESUME_DELAY_SEC", "300"))
QA_GATE_REMINDER_DELAY_SEC = float(os.environ.get("QA_GATE_REMINDER_DELAY_SEC", "300"))
TRAINING_TO_FORM_DELAY_SEC = float(os.environ.get("TRAINING_TO_FORM_DELAY_SEC", "30"))
SENT_MESSAGE_CACHE_LIMIT = int(os.environ.get("SENT_MESSAGE_CACHE_LIMIT", "200"))
JOURNAL_MAX_LINES_PER_CHAT = int(os.environ.get("JOURNAL_MAX_LINES_PER_CHAT", "500"))
SESSION_LOCK = os.environ.get("TELETHON_SESSION_LOCK", f"{SESSION_FILE}.lock")
STATUS_PATH = os.environ.get("AUTO_REPLY_STATUS_PATH", "/opt/tg_leads/.auto_reply.status")
FOLLOWUP_STATE_PATH = os.environ.get("AUTO_REPLY_FOLLOWUP_STATE_PATH", "/opt/tg_leads/.auto_reply.followup_state.json")
FOLLOWUP_CHECK_SEC = int(os.environ.get("AUTO_REPLY_FOLLOWUP_CHECK_SEC", "60"))
FOLLOWUP_WINDOW_START_HOUR = int(os.environ.get("FOLLOWUP_WINDOW_START_HOUR", "9"))
FOLLOWUP_WINDOW_END_HOUR = int(os.environ.get("FOLLOWUP_WINDOW_END_HOUR", "18"))

ACCOUNT_KEY = os.environ.get("AUTO_REPLY_ACCOUNT_KEY", "default")
TODAY_WORKSHEET = os.environ.get("TODAY_WORKSHEET", "Сегодня")
HISTORY_SHEET_PREFIX = os.environ.get("HISTORY_SHEET_PREFIX", "История")
HISTORY_RETENTION_MONTHS = int(os.environ.get("HISTORY_RETENTION_MONTHS", "6"))
PAUSED_STATE_PATH = os.environ.get("AUTO_REPLY_PAUSED_STATE_PATH", "/opt/tg_leads/.auto_reply.paused.json")

TODAY_HEADERS = [
    "Имя",
    "Username",
    "Возраст",
    "Наличие ПК/ноутбука",
    "Відповіді кандидата",
    "Ссылка на чат",
    "Ссылка на заявку",
    "Ссылка на журнал",
    "Статус",
    "Автоответчик",
    "Последнее входящее",
    "Последнее исходящее",
    "Peer ID",
    "Тех. шаг",
    "Обновлено",
    "Аккаунт",
    "Дата",
]

HISTORY_HEADERS = [
    "Имя",
    "Username",
    "Аккаунт",
    "Автоответчик",
    "Статус",
    "Peer ID",
    "Ссылка на чат",
    "Входящее",
    "Исходящее",
    "Тип события",
    "Создано",
    "Обновлено",
    "Время события",
    "Дата",
    "Журнал событий",
]

USERNAME_RE = re.compile(r"(?:@|t\.me/)([A-Za-z0-9_]{5,})")
PHONE_RE = re.compile(r"\+?\d[\d\s\-\(\)]{9,}\d")
MESSAGE_LINK_RE = re.compile(r"https?://t\.me/(c/)?([A-Za-z0-9_]+)/(\d+)")

DIALOG_AI_URL = os.environ.get("DIALOG_AI_URL", "http://127.0.0.1:3000/dialog_suggest")
DIALOG_AI_TIMEOUT_SEC = float(os.environ.get("DIALOG_AI_TIMEOUT_SEC", "20"))
DIALOG_STOP_URL = os.environ.get("DIALOG_STOP_URL", "http://127.0.0.1:3000/should_pause")
DIALOG_STOP_TIMEOUT_SEC = float(os.environ.get("DIALOG_STOP_TIMEOUT_SEC", "15"))
DIALOG_INTENT_URL = os.environ.get("DIALOG_INTENT_URL", "http://127.0.0.1:3000/intent_classify")
DIALOG_INTENT_TIMEOUT_SEC = float(os.environ.get("DIALOG_INTENT_TIMEOUT_SEC", "15"))
DIALOG_FORMAT_URL = os.environ.get("DIALOG_FORMAT_URL", "http://127.0.0.1:3000/format_choice")
DIALOG_FORMAT_TIMEOUT_SEC = float(os.environ.get("DIALOG_FORMAT_TIMEOUT_SEC", "15"))
STEP_STATE_PATH = os.environ.get("AUTO_REPLY_STEP_STATE_PATH", "/opt/tg_leads/.auto_reply.step_state.json")
GROUP_LEADS_WORKSHEET = os.environ.get("GROUP_LEADS_WORKSHEET", "GroupLeads")
REGISTRATION_WORKSHEET = os.environ.get("REGISTRATION_WORKSHEET", "Регистрация")
REGISTRATION_DRIVE_FOLDER_ID = os.environ.get("REGISTRATION_DRIVE_FOLDER_ID", "").strip()
REGISTRATION_DOWNLOAD_DIR = os.environ.get("REGISTRATION_DOWNLOAD_DIR", "/opt/tg_leads/registration_docs")
REGISTRATION_PARSE_DELAY_SEC = float(os.environ.get("REGISTRATION_PARSE_DELAY_SEC", "60"))
SHEETS_QUEUE_PATH = os.environ.get("AUTO_REPLY_SHEETS_QUEUE_PATH", "/opt/tg_leads/.sheet_events.sqlite")
SHEETS_QUEUE_FLUSH_SEC = float(os.environ.get("AUTO_REPLY_SHEETS_QUEUE_FLUSH_SEC", "1"))
SHEETS_QUEUE_BATCH_SIZE = int(os.environ.get("AUTO_REPLY_SHEETS_QUEUE_BATCH_SIZE", "20"))
SHEETS_QUEUE_LOG_SEC = int(os.environ.get("AUTO_REPLY_SHEETS_QUEUE_LOG_SEC", "30"))
CONTINUE_DELAY_SEC = float(os.environ.get("AUTO_REPLY_CONTINUE_DELAY_SEC", "0"))
FLOW_V2_ENABLED = True
V2_ENROLLMENT_PATH = os.environ.get("AUTO_REPLY_V2_ENROLLMENT_PATH", "/opt/tg_leads/.auto_reply.v2_enrolled.json")
V2_RUNTIME_PATH = os.environ.get("AUTO_REPLY_V2_RUNTIME_PATH", "/opt/tg_leads/.auto_reply.v2_runtime.json")
VOICE_MESSAGE_LINK = os.environ.get("VOICE_MESSAGE_LINK", "").strip()
PHOTO_1_MESSAGE_LINK = os.environ.get("PHOTO_1_MESSAGE_LINK", "").strip()
PHOTO_2_MESSAGE_LINK = os.environ.get("PHOTO_2_MESSAGE_LINK", "").strip()
TEST_TASK_MESSAGE_LINK = os.environ.get("TEST_TASK_MESSAGE_LINK", "").strip()
FORM_MESSAGE_LINK = os.environ.get("FORM_MESSAGE_LINK", "").strip()
VOICE_FALLBACK_DELAY_SEC = float(os.environ.get("VOICE_FALLBACK_DELAY_SEC", "300"))
VOICE_AUTO_CONTINUE_DELAY_SEC = float(os.environ.get("VOICE_AUTO_CONTINUE_DELAY_SEC", "600"))
SCREENING_WAIT_SEC = float(os.environ.get("SCREENING_WAIT_SEC", "300"))
SCHEDULE_SHIFT_WAIT_SEC = float(os.environ.get("SCHEDULE_SHIFT_WAIT_SEC", "300"))
FAQ_QUESTIONS_WORKSHEET = os.environ.get("FAQ_QUESTIONS_WORKSHEET", "FAQ_Questions")
FAQ_SUGGESTIONS_WORKSHEET = os.environ.get("FAQ_SUGGESTIONS_WORKSHEET", "FAQ_Suggestions")
CONFIRM_STATUS = "✅ Погодився"
REFERRAL_STATUS = "🎁 Реферал"
IMMUTABLE_STATUSES = {CONFIRM_STATUS, REFERRAL_STATUS}
STOP_COMMANDS = {"стоп1", "stop1"}
START_COMMANDS = {"старт1", "start1"}
AUTO_STOP_STATUS = "❌ Відмова"
MANUAL_OFF_STATUS = "🧑‍💼 Manual OFF"
STOP_REPLY_TEXT = "Розумію, дякую за відповідь. Якщо обставини зміняться, дайте знати."
CLARIFY_VARIANTS = [
    CLARIFY_TEXT,
    "Чи вдалося відповісти на ваше запитання?\nЯкщо хочете, можу одразу пояснити наступний етап.",
    "Чи все зрозуміло після пояснення?\nЯкщо є ще питання, із задоволенням уточню.",
    "Чи залишилися ще запитання по цьому етапу?\nГотовий коротко пояснити детальніше.",
]
MISSING_STEP_RECOVERY_TEXT = (
    "Щоб коректно продовжити, уточню поточний етап.\n"
    "Підкажіть, будь ласка, яка зміна вам зручніша: денна чи нічна?"
)
FORM_LOCK_REPLY_TEXT = (
    "Ми вже на фінальному етапі — заповненні анкети.\n"
    "Після отримання анкети передаю вас на старт навчання."
)
CLARIFY_NEGATIVE_FOLLOWUP_TEXT = (
    "Розумію 🙌\n"
    "Підкажіть, будь ласка, що саме залишилось незрозумілим?\n"
    "Я коротко поясню."
)
QA_GATE_CONFIRM_TEXT = "Чи залишилися ще питання? Якщо все зрозуміло — можемо йти далі 🙂"
QA_GATE_REMINDER_TEXT = "Якщо зʼявилися ще питання — із радістю відповім. Коли будете готові, підемо далі 🙂"
VOICE_FALLBACK_TEXT = "Якщо зʼявилися питання по голосовому — із радістю поясню. Коли будете готові, підемо далі 🙂"
V2_GATE_CONFIRM_TEXT = "Чи залишилися ще питання? Якщо все зрозуміло — можемо йти далі 🙂"
V2_GATE_REMINDER_TEXT = "Якщо зʼявилися ще питання — із радістю відповім. Коли будете готові, підемо далі 🙂"
SCREENING_INTRO_TEXT = (
    "Привіт) Ви залишали відгук на вакансію менеджера чату. "
    "Зараз я розповім детальніше про вакансію, але спершу дайте, будь ласка, відповіді на кілька запитань:\n\n"
    "Чи є у Вас зараз якась зайнятість? (навчання / робота / декретна відпустка)\n"
    "Чи мали Ви раніше справу зі сферою дейтингу? (чули про неї або працювали)\n"
    "Скільки Вам років?\n"
    "Ваші відповіді потрібні, щоб я розумів, наскільки детально варто розповідати про вакансію 🙂."
)
AGE_UNDER18_TEXT = (
    "На жаль, на цьому етапі ми не можемо продовжити оформлення, оскільки компанія розглядає кандидатів лише з 18 років. "
    "Це пов’язано з внутрішніми правилами, юридичними вимогами та особливостями роботи на міжнародних платформах."
)
AGE_OVER40_TEXT = (
    "Дякую Вам за відповіді 🙂 На жаль, на цьому етапі ми не зможемо продовжити співпрацю, "
    "оскільки за внутрішніми критеріями проєкту ми розглядаємо кандидатів у межах іншого вікового діапазону.\n"
    "Дякую за інтерес до вакансії та бажаю Вам успіхів у пошуку роботи!"
)
REFERRAL_AFTER_REJECT_TEXT = (
    "Також хочу повідомити, що в нашій компанії діє реферальна програма 💰\n"
    "Ви можете отримати 100 $ бонусу за кожного запрошеного друга,\n"
    "який:\n"
    "- раніше не працював у нашій компанії;\n"
    "- після старту відпрацює щонайменше 14 днів;\n"
    "- за перші 30 днів заробить мінімум 200 $ балансу.\n\n"
    "Якщо серед ваших знайомих є люди,\n"
    "яким може бути цікава така робота — сміливо рекомендуйте 🙂"
)
COMPANY_INTRO_TEXT = (
    "Дякую за відповіді)\n"
    "Наша компанія Furioza Company працює в сфері дейтингу з 2014 року. "
    "Робота повністю віддалена в повну зайнятість (8-годинний робочий графік), тільки через ПК або ноутбук.\n\n"
    "В обов'язки чат-менеджера входить комунікація з клієнтами в режимі онлайн чату, "
    "відповіді на листи, створення інвайтів та просування анкети, без дзвінків та відеодзвінків.\n\n"
    "Вам буде зручно прослухати голосове повідомлення, щоб я детальніше розповіла всі умови роботи?"
)
COMPANY_INTRO_TIMEOUT_TEXT = (
    "Наша компанія Furioza Company працює в сфері дейтингу з 2014 року. "
    "Робота повністю віддалена в повну зайнятість (8-годинний робочий графік), тільки через ПК або ноутбук.\n\n"
    "В обов'язки чат-менеджера входить комунікація з клієнтами в режимі онлайн чату, "
    "відповіді на листи, створення інвайтів та просування анкети, без дзвінків та відеодзвінків.\n\n"
    "Вам буде зручно прослухати голосове повідомлення, щоб я детальніше розповіла всі умови роботи?"
)
SCHEDULE_SHIFT_TEXT = (
    "Ми пропонує 2 зміни на вибір — ви обираєте одну і працюєте за цим графіком на постійній основі:\n"
    "- Денна 14:00–23:00\n"
    "- Нічна 23:00–08:00\n"
    "На кожній зміні передбачено:\n"
    "- 1 година основної перерви\n"
    "- Короткі міні-перерви по 5 хвилин\n"
    "Щодо вихідних: у вас є 8 вихідних днів на місяць, брати їх можна коли зручно, будні це дні чи вихідні - не важливо)\n"
    "Який графік роботи тобі підходить?"
)
SCHEDULE_DETAILS_TEXT = (
    "Робота на сайті відбувається одночасно на декількох анкет.\n"
    "Працювати потрібно у парі з напарниками — 8 годин у своїй зміні, та 2 напарника — у своїй. "
    "Ваша комунікація буде в Telegram, де створиться спільний чат. Там ви зможете обговорювати робочі моменти по анкетах, ділитись думками та допомагати одне одному.\n"
    "Щоб мати уявлення, як виглядає інтерфейс сайту і що на тебе чекає — переглянь навчальний [сайт](https://alpha-mini.pp.ua/) приклад робочого процесу!\n\n"
    "Підсумуємо головне:\n"
    "Графік — 8 годин на день біля ПК. У вас буде особистий кабінет, де фіксується робочий час. "
    "Робота інтенсивна: в середньому одна дія має бути кожні 5 хвилин. Тобто ти реально працюєш увесь час, а не просто \"в онлайні\".\n"
    "Навчання триває 8 днів.\n"
    "Зарплата в кінці місяця — 48% від суми на балансі профілю."
)
SCHEDULE_CONFIRM_TEXT = "Чи зрозумілий тобі графік і як рахується робочий час? Можемо йти далі? 😊"

FOLLOWUP_TEMPLATES = [
    (
        30 * 60,
        "Хотів уточнити, чи встигли ви ознайомитися з інформацією?\n"
        "Якщо з’явилися запитання — я на зв’язку і з радістю відповім.",
    ),
    (
        24 * 60 * 60,
        "Доброго дня 🙂\n"
        "Нагадую про себе, щоб не загубити контакт.\n"
        "Підкажіть, будь ласка, чи актуально для вас продовжити спілкування щодо вакансії?",
    ),
    (
        3 * 24 * 60 * 60,
        "Добрий день!\n"
        "Нагадую щодо вакансії оператора чату. Якщо тема вже не актуальна — напишіть, будь ласка, щоб я не турбував.",
    ),
]
TEST_USER_ID = "156414561"
TEST_START_COMMANDS = {"старт8", "start8"}

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

LEGACY_SHEET_NAMES = {"StatusRules", "Excluded", "Paused", "Leads"}
LEGACY_DAY_SHEET_RE = re.compile(r"^\d{2}\.\d{2}\.\d{2}(\d{2})?$")
RU_MONTHS = {
    1: "Январь",
    2: "Февраль",
    3: "Март",
    4: "Апрель",
    5: "Май",
    6: "Июнь",
    7: "Июль",
    8: "Август",
    9: "Сентябрь",
    10: "Октябрь",
    11: "Ноябрь",
    12: "Декабрь",
}

STATUS_BY_TEMPLATE = {
    normalize_text(CONTACT_TEXT): "👋 Привітання",
    normalize_text(INTEREST_TEXT): "👋 Привітання",
    normalize_text(DATING_TEXT): "🏢 Знайомство з компанією",
    normalize_text(DUTIES_TEXT): "🏢 Знайомство з компанією",
    normalize_text(CLARIFY_TEXT): "🏢 Знайомство з компанією",
    normalize_text(SHIFTS_TEXT): "🕒 Графік",
    normalize_text(SHIFT_QUESTION_TEXT): "🕒 Графік",
    normalize_text(FORMAT_TEXT): "🎥 Більше інформації",
    normalize_text(FORMAT_QUESTION_TEXT): "🎥 Більше інформації",
    normalize_text(VIDEO_FOLLOWUP_TEXT): "🎥 Відео",
    normalize_text(MINI_COURSE_LINK): "🎥 Більше інформації",
    normalize_text(MINI_COURSE_FOLLOWUP_TEXT): "🎥 Більше інформації",
    normalize_text(BOTH_FORMATS_FOLLOWUP_TEXT): "🎥 Більше інформації",
    normalize_text(TRAINING_TEXT): "🎓 Навчання",
    normalize_text(TRAINING_QUESTION_TEXT): "🎓 Навчання",
    normalize_text(FORM_TEXT): "📝 Анкета",
    normalize_text(CONFIRM_TEXT): CONFIRM_STATUS,
    normalize_text(REFERRAL_TEXT): REFERRAL_STATUS,
}

GROUP_LEADS_HEADERS = [
    "Получено",
    "Статус",
    "ФИО",
    "Возраст",
    "Телефон",
    "Telegram",
    "ПК/ноутбук",
    "ID источника",
    "Источник",
    "Сырой текст",
]

REGISTRATION_HEADERS = [
    "ФИО",
    "Дата рождения",
    "Телефон",
    "Email",
    "Telegram кандидата",
    "График",
    "Дата старта",
    "Город",
    "Telegram админа",
    "Ссылка на документ (Drive)",
    "Ссылка на сообщение",
    "Сырой текст",
    "Группа-источник",
    "ID сообщения",
    "Получено",
]

FAQ_QUESTIONS_HEADERS = [
    "created_at",
    "peer_id",
    "step",
    "question_raw",
    "question_norm",
    "cluster_key",
    "count",
    "last_seen_at",
    "answer_preview",
    "resolved_status",
]

FAQ_SUGGESTIONS_HEADERS = [
    "question_cluster",
    "suggested_answer",
    "source_examples",
    "review_status",
    "reviewed_at",
    "reviewed_by",
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
    return message_has_question_impl(text)


def strip_question_trail(text: str) -> str:
    return strip_question_trail_impl(text)


SENT_MESSAGES = {}
PAUSE_CHECKER = None
SHEETS_EVENT_ENQUEUER = None

def track_sent_message(peer_id: int, message_id: int) -> None:
    if not peer_id or not message_id:
        return
    bucket = SENT_MESSAGES.get(peer_id)
    if bucket is None:
        bucket = deque(maxlen=SENT_MESSAGE_CACHE_LIMIT)
        SENT_MESSAGES[peer_id] = bucket
    bucket.append(int(message_id))


def is_tracked_message(peer_id: int, message_id: int) -> bool:
    bucket = SENT_MESSAGES.get(peer_id)
    if not bucket:
        return False
    return int(message_id) in bucket


def is_stop_phrase(text: str) -> bool:
    return is_stop_phrase_impl(text)


def is_continue_phrase(text: str) -> bool:
    return is_continue_phrase_impl(text)


def is_neutral_ack(text: str) -> bool:
    return is_neutral_ack_impl(text)


def enqueue_sheet_event(event_type: str, payload: dict):
    global SHEETS_EVENT_ENQUEUER
    if not SHEETS_EVENT_ENQUEUER:
        return False
    try:
        event_id = SHEETS_EVENT_ENQUEUER(event_type, payload)
        print(f"SHEETS_QUEUE_ENQUEUE type={event_type} id={event_id}")
        return True
    except Exception as err:
        print(f"⚠️ SHEETS_QUEUE_ENQUEUE_FAIL type={event_type}: {type(err).__name__}: {err}")
        return False


def is_clarify_uncertain_reply(text: str) -> bool:
    t = normalize_text(text)
    if not t:
        return False
    if t in {"ні", "нет", "неа", "не зовсім", "не совсем", "не дуже", "не очень"}:
        return True
    if re.fullmatch(r"(ні|нет)[.!]?", t):
        return True
    return False


async def classify_candidate_intent(history: list, text: str, last_step: Optional[str]) -> Intent:
    async def _ai_client(hist: list, last_text: str) -> str:
        if DIALOG_INTENT_URL:
            payload = {"history": hist, "last_message": last_text}
            try:
                data = await asyncio.to_thread(_post_json, DIALOG_INTENT_URL, payload, DIALOG_INTENT_TIMEOUT_SEC)
            except (urllib.error.URLError, urllib.error.HTTPError, ValueError, OSError) as err:
                print(f"⚠️ AI intent error: {err}")
                data = None
            if data and data.get("ok"):
                return str(data.get("intent") or "").strip().lower()
        if not DIALOG_STOP_URL:
            return "other"
        payload = {"history": hist, "last_message": last_text}
        try:
            data = await asyncio.to_thread(_post_json, DIALOG_STOP_URL, payload, DIALOG_STOP_TIMEOUT_SEC)
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, OSError) as err:
            print(f"⚠️ AI stop check error: {err}")
            return "other"
        if not data or not data.get("ok"):
            return "other"
        return "stop" if bool(data.get("stop")) else "ack_continue"

    return await classify_intent(text, history, last_step=last_step, ai_client=_ai_client)


def should_send_question(sent_text: str, question_text: str) -> bool:
    return should_send_question_impl(
        sent_text,
        question_text,
        CLARIFY_TEXT,
        SHIFT_QUESTION_TEXT,
        FORMAT_QUESTION_TEXT,
    )


def is_test_restart(sender: User, text: str) -> bool:
    if not sender or not text:
        return False
    if str(getattr(sender, "id", "")) != TEST_USER_ID:
        return False
    return normalize_text(text) in {normalize_text(cmd) for cmd in TEST_START_COMMANDS}


def is_test_user(sender: User) -> bool:
    if not sender:
        return False
    return str(getattr(sender, "id", "")) == TEST_USER_ID


def parse_shift_choice(text: str) -> Optional[str]:
    t = normalize_text(text)
    if not t:
        return None
    day_markers = ("день", "ден", "денна", "14:00", "14 00", "14-23", "14 до 23")
    night_markers = ("ніч", "ноч", "нічна", "23:00", "23 00", "23-08", "23 до 08")
    day = any(m in t for m in day_markers)
    night = any(m in t for m in night_markers)
    if day and not night:
        return "денна"
    if night and not day:
        return "нічна"
    return None


def split_answer_lines(text: str) -> List[str]:
    raw = (text or "").strip()
    if not raw:
        return []
    # Try to parse compact one-line formats like:
    # "1 ... 2 ... 3 ..." or "1) ... 2) ... 3) ..."
    if "\n" not in raw and "\r" not in raw:
        marker_re = re.compile(r"(1️⃣|2️⃣|3️⃣|(?:^|\s)([1-3])[\)\.\-:]\s*)", flags=re.IGNORECASE)
        matches = list(marker_re.finditer(raw))
        if len(matches) >= 3:
            chunks: List[str] = []
            for idx, m in enumerate(matches):
                start = m.end()
                end = matches[idx + 1].start() if idx + 1 < len(matches) else len(raw)
                part = raw[start:end].strip(" \t,;|")
                if part:
                    chunks.append(part)
            if len(chunks) >= 3:
                return chunks[:3]
    lines = []
    for part in re.split(r"[\n\r]+", raw):
        item = re.sub(r"^\s*(?:\d+[.)]|[•\-])\s*", "", part).strip()
        if item:
            lines.append(item)
    return lines


def format_numbered_answers(items: List[str]) -> str:
    clean = [x.strip() for x in items if x and x.strip()]
    return "\n".join(f"{idx}. {val}" for idx, val in enumerate(clean, start=1))


def _contains_any(text: str, needles: Tuple[str, ...]) -> bool:
    return any(n in text for n in needles)


def evaluate_test_answers(answers: List[str]) -> Tuple[bool, str]:
    a1 = normalize_text((answers[0] if len(answers) > 0 else "") or "")
    a2 = normalize_text((answers[1] if len(answers) > 1 else "") or "")
    a3 = normalize_text((answers[2] if len(answers) > 2 else "") or "")

    q1_ok = ("8" in a1 and _contains_any(a1, ("год", "годин", "година", "hours"))) or _contains_any(a1, ("вісім", "восем"))
    q2_ok = "48" in a2 and "%" in a2
    q3_ok = _contains_any(
        a3,
        (
            "перевед",
            "зміна команди",
            "змiна команди",
            "зміна адміністратора",
            "змiна адміністратора",
            "зміна тімліда",
            "припинення співпраці",
            "припинити співпрацю",
            "звіль",
            "звiль",
            "завершення співпраці",
        ),
    )

    if q1_ok and q2_ok and q3_ok:
        return True, "Дякую, відповіді вірні ✅ Можемо переходити до наступного етапу."

    return (
        False,
        "Є неточності. Правильні відповіді:\n"
        "1) Мінімум — 8 годин на день.\n"
        "2) Гарантований відсоток у перший місяць — 48%.\n"
        "3) HR може ухвалити рішення про переведення в іншу команду/до іншого адміністратора або про припинення співпраці.",
    )


def merge_test_answers(existing: List[str], text: str) -> List[str]:
    answers = list(existing or ["", "", ""])
    if len(answers) < 3:
        answers = answers + [""] * (3 - len(answers))
    candidate_lines = split_answer_lines(text)
    for line in candidate_lines:
        line_l = normalize_text(line)
        idx = None
        m = re.match(r"^\s*([123])[.):\-]?\s*(.+)$", line, flags=re.IGNORECASE)
        if m:
            idx = int(m.group(1)) - 1
            line = m.group(2).strip()
            line_l = normalize_text(line)
        elif "%" in line_l:
            idx = 1
        elif "год" in line_l or "8" in line_l:
            idx = 0
        elif any(k in line_l for k in ("hr", "ейчар", "рішен", "решен", "перев", "звіль", "увільн", "звiль")):
            idx = 2
        if idx is None:
            for i in range(3):
                if not answers[i]:
                    idx = i
                    break
        if idx is not None:
            answers[idx] = line
    return answers[:3]


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
    payload = {
        "peer_id": entity.id,
        "name": name,
        "username": username,
        "chat_link": chat_link,
        "status": status,
        "last_out": None,
        "tech_step": step_name,
    }
    if not enqueue_sheet_event("today_upsert", payload):
        try:
            sheet.upsert(tz=tz, **payload)
            print(f"AUTO_REPLY_CONTINUE despite_sheet_error peer={entity.id}")
        except Exception as err:
            print(f"⚠️ SHEETS_DIRECT_WRITE_FAIL peer={entity.id}: {type(err).__name__}: {err}")


def normalize_key(text: str) -> str:
    cleaned = normalize_text(text)
    return re.sub(r"[^\w\s]", "", cleaned, flags=re.IGNORECASE)


def normalize_phone(text: str) -> str:
    return re.sub(r"[^\d+]", "", text or "")


def normalize_name(text: str) -> str:
    cleaned = normalize_text(text)
    return re.sub(r"\s+", " ", cleaned).strip()


def names_match(left: str, right: str) -> bool:
    l = normalize_name(left)
    r = normalize_name(right)
    if not l or not r:
        return False
    if l == r:
        return True
    if l.startswith(r + " ") or r.startswith(l + " "):
        return True
    l_tokens = [t for t in l.split(" ") if t]
    r_tokens = [t for t in r.split(" ") if t]
    if len(l_tokens) == 1 and l_tokens[0] in r_tokens:
        return True
    if len(r_tokens) == 1 and r_tokens[0] in l_tokens:
        return True
    return False


def col_letter(col_idx: int) -> str:
    result = []
    while col_idx > 0:
        col_idx, rem = divmod(col_idx - 1, 26)
        result.append(chr(ord("A") + rem))
    return "".join(reversed(result))


def within_followup_window(dt: datetime) -> bool:
    return within_followup_window_impl(dt, FOLLOWUP_WINDOW_START_HOUR, FOLLOWUP_WINDOW_END_HOUR)


def adjust_to_followup_window(dt: datetime) -> datetime:
    return adjust_to_followup_window_impl(dt, FOLLOWUP_WINDOW_START_HOUR, FOLLOWUP_WINDOW_END_HOUR)


class FollowupState(FollowupStateStore):
    def __init__(self, path: str):
        super().__init__(
            path=path,
            templates=FOLLOWUP_TEMPLATES,
            start_hour=FOLLOWUP_WINDOW_START_HOUR,
            end_hour=FOLLOWUP_WINDOW_END_HOUR,
            test_user_id=TEST_USER_ID,
        )

    def schedule_from_now(self, peer_id: int, tz: ZoneInfo):
        super().schedule_from_now(peer_id, datetime.now(tz))

    def mark_sent_and_advance(self, peer_id: int, tz: ZoneInfo):
        return super().mark_sent_and_advance(peer_id, datetime.now(tz))


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
        self.today_ws = None
        self.today_key = None
        self._headers_cache = {}
        self._headers_cache_ts = {}
        self._headers_cache_ttl_sec = 30
        self._row_index_cache = {}
        self._row_index_cache_ts = {}
        self._row_index_cache_ttl_sec = 30
        self._next_row_cache = {}
        self.migrate_sheets()

    def _col_letter(self, col_idx: int) -> str:
        result = []
        while col_idx > 0:
            col_idx, rem = divmod(col_idx - 1, 26)
            result.append(chr(ord("A") + rem))
        return "".join(reversed(result))

    def _month_title(self, dt: date) -> str:
        return f"{RU_MONTHS[dt.month]} {dt.year}"

    def _parse_month_title(self, title: str) -> Optional[Tuple[int, int]]:
        title = (title or "").strip()
        for month_num, month_name in RU_MONTHS.items():
            prefix = f"{month_name} "
            if title.startswith(prefix):
                year_part = title[len(prefix):].strip()
                if year_part.isdigit():
                    return int(year_part), month_num
        return None

    def _month_shift(self, dt: date, months: int) -> date:
        month_index = (dt.year * 12 + dt.month - 1) + months
        year = month_index // 12
        month = month_index % 12 + 1
        return date(year, month, 1)

    def _today_key(self, tz: ZoneInfo) -> str:
        return datetime.now(tz).strftime("%Y-%m-%d")

    def _ensure_today_ws(self, tz: ZoneInfo):
        key = self._today_key(tz)
        if self.today_ws is None:
            self.today_ws = get_or_create_worksheet(self.sh, TODAY_WORKSHEET, rows=1000, cols=len(TODAY_HEADERS))
            current = self.today_ws.row_values(1)
            if current != TODAY_HEADERS:
                self.today_ws.clear()
                self.today_ws.append_row(TODAY_HEADERS, value_input_option="USER_ENTERED")
            self.today_key = key
            self._invalidate_ws_cache(self.today_ws)
            return self.today_ws
        if self.today_key != key:
            self.today_ws.clear()
            self.today_ws.append_row(TODAY_HEADERS, value_input_option="USER_ENTERED")
            self.today_key = key
            self._invalidate_ws_cache(self.today_ws)
        return self.today_ws

    def _invalidate_ws_cache(self, ws):
        ws_id = ws.id
        self._headers_cache.pop(ws_id, None)
        self._headers_cache_ts.pop(ws_id, None)
        self._row_index_cache.pop(ws_id, None)
        self._row_index_cache_ts.pop(ws_id, None)
        self._next_row_cache.pop(ws_id, None)

    def _get_headers(self, ws):
        ws_id = ws.id
        now = time.time()
        cached = self._headers_cache.get(ws_id)
        ts = self._headers_cache_ts.get(ws_id, 0)
        if cached and (now - ts) < self._headers_cache_ttl_sec:
            return cached
        headers = [h.strip() for h in ws.row_values(1)]
        self._headers_cache[ws_id] = headers
        self._headers_cache_ts[ws_id] = now
        return headers

    def _ensure_row_index(self, ws, headers):
        ws_id = ws.id
        now = time.time()
        ts = self._row_index_cache_ts.get(ws_id, 0)
        if ws_id in self._row_index_cache and (now - ts) < self._row_index_cache_ttl_sec:
            return
        try:
            peer_idx = headers.index("Peer ID")
            account_idx = headers.index("Аккаунт")
        except ValueError:
            self._row_index_cache[ws_id] = {}
            self._row_index_cache_ts[ws_id] = now
            self._next_row_cache[ws_id] = 2
            return
        values = ws.get_all_values()
        index = {}
        for idx, row in enumerate(values[1:], start=2):
            if peer_idx >= len(row) or account_idx >= len(row):
                continue
            peer_raw = row[peer_idx].strip()
            account_raw = row[account_idx].strip()
            if peer_raw and account_raw:
                index[(peer_raw, account_raw)] = idx
        self._row_index_cache[ws_id] = index
        self._row_index_cache_ts[ws_id] = now
        self._next_row_cache[ws_id] = len(values) + 1

    def _history_ws(self, tz: ZoneInfo):
        title = self._month_title(datetime.now(tz).date())
        ws = get_or_create_worksheet(self.sh, title, rows=1000, cols=len(HISTORY_HEADERS))
        self._ensure_history_headers(ws)
        return ws

    def _ensure_history_headers(self, ws):
        values = ws.get_all_values()
        if not values:
            ws.append_row(HISTORY_HEADERS, value_input_option="USER_ENTERED")
            self._invalidate_ws_cache(ws)
            return
        current_headers = [h.strip() for h in values[0]]
        if current_headers == HISTORY_HEADERS:
            return
        data_rows = values[1:]
        remapped_rows = []
        for row in data_rows:
            row_map = {}
            for idx, header in enumerate(current_headers):
                row_map[header] = row[idx] if idx < len(row) else ""
            remapped_rows.append([row_map.get(h, "") for h in HISTORY_HEADERS])
        ws.clear()
        ws.append_row(HISTORY_HEADERS, value_input_option="USER_ENTERED")
        if remapped_rows:
            ws.append_rows(remapped_rows, value_input_option="USER_ENTERED")
        self._invalidate_ws_cache(ws)

    def migrate_sheets(self):
        try:
            worksheets = self.sh.worksheets()
        except Exception:
            return
        for ws in worksheets:
            title = (ws.title or "").strip()
            if title in {GROUP_LEADS_WORKSHEET, TODAY_WORKSHEET}:
                continue
            if title in LEGACY_SHEET_NAMES or LEGACY_DAY_SHEET_RE.match(title):
                try:
                    self.sh.del_worksheet(ws)
                except Exception as err:
                    print(f"⚠️ Не вдалося видалити legacy лист '{title}': {err}")
        self.cleanup_old_month_sheets()

    def cleanup_old_month_sheets(self):
        if HISTORY_RETENTION_MONTHS <= 0:
            return
        today = datetime.now(ZoneInfo(TIMEZONE)).date()
        keep_from = self._month_shift(date(today.year, today.month, 1), -(HISTORY_RETENTION_MONTHS - 1))
        try:
            worksheets = self.sh.worksheets()
        except Exception:
            return
        for ws in worksheets:
            parsed = self._parse_month_title(ws.title or "")
            if not parsed:
                continue
            year, month = parsed
            sheet_month = date(year, month, 1)
            if sheet_month < keep_from:
                try:
                    self.sh.del_worksheet(ws)
                except Exception as err:
                    print(f"⚠️ Не вдалося видалити старий лист '{ws.title}': {err}")

    def _find_row(self, ws, peer_id: int, account_key: str):
        headers = self._get_headers(ws)
        self._ensure_row_index(ws, headers)
        ws_id = ws.id
        idx = self._row_index_cache.get(ws_id, {}).get((str(peer_id), account_key))
        end_col = self._col_letter(len(headers))
        if idx:
            values = ws.get(f"A{idx}:{end_col}{idx}")
            row = values[0] if values else []
            if row:
                return idx, row

        # Fallback: full scan when cache misses/stales.
        try:
            values = ws.get_all_values()
        except Exception:
            self._invalidate_ws_cache(ws)
            return None, None
        if not values:
            self._row_index_cache[ws_id] = {}
            self._row_index_cache_ts[ws_id] = time.time()
            self._next_row_cache[ws_id] = 2
            return None, None
        try:
            peer_idx = headers.index("Peer ID")
            account_idx = headers.index("Аккаунт")
        except ValueError:
            return None, None
        found_idx = None
        found_row = None
        for row_idx, row in enumerate(values[1:], start=2):
            peer_match = peer_idx < len(row) and row[peer_idx].strip() == str(peer_id)
            account_match = account_idx < len(row) and row[account_idx].strip() == account_key
            if peer_match and account_match:
                found_idx = row_idx
                found_row = row
                break
        index = self._row_index_cache.get(ws_id, {})
        if not isinstance(index, dict):
            index = {}
        if found_idx:
            index[(str(peer_id), account_key)] = found_idx
        self._row_index_cache[ws_id] = index
        self._row_index_cache_ts[ws_id] = time.time()
        self._next_row_cache[ws_id] = len(values) + 1
        if found_idx:
            return found_idx, found_row
        return None, None

    def _event_type(
        self,
        status: Optional[str],
        auto_reply_enabled: Optional[bool],
        last_in: Optional[str],
        last_out: Optional[str],
        event_type_override: Optional[str] = None,
    ) -> str:
        if event_type_override:
            return event_type_override
        if last_out is not None:
            return "Исходящее сообщение"
        if last_in is not None:
            return "Входящее сообщение"
        if auto_reply_enabled is not None:
            return "Переключение автоответчика"
        if status is not None:
            return "Изменение статуса"
        return "Служебное обновление"

    def _sheet_row_link(self, ws, row_idx: int, label: str) -> str:
        return f'=HYPERLINK("#gid={ws.id}&range=A{int(row_idx)}";"{label}")'

    def _find_group_lead_info(self, username: str, name: str) -> Optional[dict]:
        uname = normalize_username(username)
        name_norm = normalize_name(name)
        if not uname:
            uname = ""
        if not name_norm:
            name_norm = ""
        try:
            ws = self.sh.worksheet(GROUP_LEADS_WORKSHEET)
        except Exception:
            return None
        try:
            values = ws.get_all_values()
        except Exception:
            return None
        if not values:
            return None
        headers = [h.strip().lower() for h in values[0]]

        def idx_of(*names: str) -> Optional[int]:
            for name in names:
                try:
                    return headers.index(name)
                except ValueError:
                    continue
            return None

        tg_idx = idx_of("tg", "telegram", "тг")
        age_idx = idx_of("age", "возраст")
        pc_idx = idx_of("pc", "ноутбук", "пк", "пк/ноутбук")
        full_name_idx = idx_of("full_name", "фио", "піб", "имя")
        try:
            if tg_idx is None and full_name_idx is None:
                raise ValueError("no lookup columns found")
        except ValueError:
            return None
        fallback_match = None
        for idx, row in enumerate(values[1:], start=2):
            row_username = row[tg_idx] if tg_idx is not None and tg_idx < len(row) else ""
            row_name = row[full_name_idx] if full_name_idx is not None and full_name_idx < len(row) else ""
            if uname and normalize_username(row_username) == uname:
                age = row[age_idx].strip() if age_idx is not None and age_idx < len(row) else ""
                pc = row[pc_idx].strip() if pc_idx is not None and pc_idx < len(row) else ""
                return {"row_idx": idx, "age": age, "pc": pc}
            if not fallback_match and name_norm and names_match(row_name, name_norm):
                age = row[age_idx].strip() if age_idx is not None and age_idx < len(row) else ""
                pc = row[pc_idx].strip() if pc_idx is not None and pc_idx < len(row) else ""
                fallback_match = {"row_idx": idx, "age": age, "pc": pc}
        return fallback_match

    def refresh_today_from_group_lead(self, tz: ZoneInfo, group_data: dict) -> int:
        ws = self._ensure_today_ws(tz)
        headers = self._get_headers(ws)
        try:
            values = ws.get_all_values()
        except Exception:
            self._invalidate_ws_cache(ws)
            return 0
        if not values or len(values) <= 1:
            return 0

        def idx_of(name: str) -> Optional[int]:
            try:
                return headers.index(name)
            except ValueError:
                return None

        username_idx = idx_of("Username")
        name_idx = idx_of("Имя")
        app_link_idx = idx_of("Ссылка на заявку")
        age_idx = idx_of("Возраст")
        pc_idx = idx_of("Наличие ПК/ноутбука")
        if app_link_idx is None or age_idx is None or pc_idx is None:
            return 0

        lead_info = self._find_group_lead_info(group_data.get("tg", ""), group_data.get("full_name", ""))
        if not lead_info:
            return 0
        app_ws = self.sh.worksheet(GROUP_LEADS_WORKSHEET)
        app_link = self._sheet_row_link(app_ws, int(lead_info["row_idx"]), "Открыть заявку")
        lead_age = lead_info.get("age", "")
        lead_pc = lead_info.get("pc", "")
        target_uname = normalize_username(group_data.get("tg", ""))
        target_name = normalize_name(group_data.get("full_name", ""))
        updated = 0
        end_col = self._col_letter(len(headers))

        for idx, row in enumerate(values[1:], start=2):
            row_uname = normalize_username(row[username_idx]) if username_idx is not None and username_idx < len(row) else ""
            row_name = normalize_name(row[name_idx]) if name_idx is not None and name_idx < len(row) else ""
            matches = False
            if target_uname and row_uname == target_uname:
                matches = True
            elif target_name and names_match(row_name, target_name):
                matches = True
            if not matches:
                continue

            row_full = row[:] + [""] * max(0, len(headers) - len(row))
            changed = False
            if row_full[app_link_idx] != app_link:
                row_full[app_link_idx] = app_link
                changed = True
            if row_full[age_idx] != lead_age:
                row_full[age_idx] = lead_age
                changed = True
            if row_full[pc_idx] != lead_pc:
                row_full[pc_idx] = lead_pc
                changed = True
            if not changed:
                continue
            ws.update(range_name=f"A{idx}:{end_col}{idx}", values=[row_full], value_input_option="USER_ENTERED")
            updated += 1
        if updated:
            self._invalidate_ws_cache(ws)
        return updated

    def _sort_today_by_updated(self, ws, headers):
        try:
            updated_idx = headers.index("Обновлено") + 1
        except ValueError:
            return
        end_col = self._col_letter(len(headers))
        try:
            ws.sort((updated_idx, "des"), range=f"A2:{end_col}{ws.row_count}")
            self._invalidate_ws_cache(ws)
        except Exception as err:
            print(f"⚠️ Не вдалося відсортувати лист '{TODAY_WORKSHEET}': {err}")

    def append_history_event(
        self,
        tz: ZoneInfo,
        event_type: str,
        peer_id: int,
        name: str,
        username: str,
        chat_link: str,
        status: Optional[str],
        auto_reply_enabled: Optional[bool],
        last_in: Optional[str],
        last_out: Optional[str],
        sender_role: Optional[str] = None,
        dialog_mode: Optional[str] = None,
        step_snapshot: Optional[str] = None,
        full_text: Optional[str] = None,
    ) -> Optional[str]:
        ws = self._history_ws(tz)
        now_iso = datetime.now(tz).isoformat(timespec="seconds")
        headers = self._get_headers(ws)
        row_idx, existing = self._find_row(ws, peer_id, ACCOUNT_KEY)
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

        def get_value(name: str) -> str:
            idx = col_idx(name)
            if idx is None or idx >= len(existing):
                return ""
            return existing[idx]

        role_value = (sender_role or "").strip() or "unknown"
        mode_value = (dialog_mode or "").strip() or "ON"
        step_value = (step_snapshot or "").strip() or "-"
        text_value = (full_text if full_text is not None else (last_in if last_in is not None else last_out)) or ""
        text_value = str(text_value).replace("\r\n", "\n").replace("\r", "\n").replace("\n", "\\n").strip()
        event_line = f"{now_iso} | role={role_value} | mode={mode_value} | step={step_value} | {event_type} | text={text_value}"

        journal_prev = get_value("Журнал событий").strip()
        journal_lines = [line for line in journal_prev.splitlines() if line.strip()]
        journal_lines.append(event_line)
        journal_lines = journal_lines[-max(50, JOURNAL_MAX_LINES_PER_CHAT):]
        journal_text = "\n".join(journal_lines)

        set_value("Время события", now_iso)
        set_value("Дата", str(datetime.now(tz).date()))
        set_value("Аккаунт", ACCOUNT_KEY)
        set_value("Тип события", event_type)
        set_value("Имя", name)
        set_value("Username", ("@" + username) if username else "")
        set_value("Ссылка на чат", chat_link)
        set_value("Статус", status or "")
        if auto_reply_enabled is not None:
            set_value("Автоответчик", "ON" if auto_reply_enabled else "OFF")
        set_value("Входящее", last_in or "")
        set_value("Исходящее", last_out or "")
        set_value("Peer ID", str(peer_id))
        if not get_value("Создано"):
            set_value("Создано", now_iso)
        set_value("Обновлено", now_iso)
        set_value("Журнал событий", journal_text)

        try:
            if row_idx:
                end_col = self._col_letter(len(headers))
                ws.update(range_name=f"A{row_idx}:{end_col}{row_idx}", values=[existing], value_input_option="USER_ENTERED")
            else:
                row_idx_recheck, existing_recheck = self._find_row(ws, peer_id, ACCOUNT_KEY)
                if row_idx_recheck:
                    row_idx = row_idx_recheck
                    existing = existing_recheck or [""] * len(headers)
                    if len(existing) < len(headers):
                        existing = existing + [""] * (len(headers) - len(existing))
                    end_col = self._col_letter(len(headers))
                    ws.update(range_name=f"A{row_idx}:{end_col}{row_idx}", values=[existing], value_input_option="USER_ENTERED")
                else:
                    next_row = int(self._next_row_cache.get(ws.id, 2))
                    end_col = self._col_letter(len(headers))
                    ws.update(range_name=f"A{next_row}:{end_col}{next_row}", values=[existing], value_input_option="USER_ENTERED")
                    self._row_index_cache.setdefault(ws.id, {})[(str(peer_id), ACCOUNT_KEY)] = next_row
                    self._next_row_cache[ws.id] = next_row + 1
        except Exception as err:
            print(f"⚠️ Не вдалося записати історію: {err}")
            self._invalidate_ws_cache(ws)
            return None
        if not row_idx:
            row_idx, _ = self._find_row(ws, peer_id, ACCOUNT_KEY)
        if not row_idx:
            return None
        return self._sheet_row_link(ws, row_idx, "Открыть журнал")

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
        tech_step: Optional[str] = None,
        sender_role: Optional[str] = None,
        dialog_mode: Optional[str] = None,
        step_snapshot: Optional[str] = None,
        full_text: Optional[str] = None,
        event_type_override: Optional[str] = None,
        followup_stage: Optional[str] = None,
        followup_next_at: Optional[str] = None,
        followup_last_sent_at: Optional[str] = None,
        candidate_note_append: Optional[str] = None,
    ):
        del followup_stage, followup_next_at, followup_last_sent_at
        ws = self._ensure_today_ws(tz)
        headers = self._get_headers(ws)
        row_idx, existing = self._find_row(ws, peer_id, ACCOUNT_KEY)
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

        status_idx = col_idx("Статус")
        existing_status = existing[status_idx] if status_idx is not None and status_idx < len(existing) else ""
        if existing_status in IMMUTABLE_STATUSES:
            status = existing_status

        now_iso = datetime.now(tz).isoformat(timespec="seconds")
        set_value("Дата", str(datetime.now(tz).date()))
        set_value("Имя", name)
        set_value("Username", ("@" + username) if username else "")
        set_value("Возраст", "")
        set_value("Наличие ПК/ноутбука", "")
        set_value("Ссылка на чат", chat_link)
        try:
            lead_info = self._find_group_lead_info(username, name)
            if lead_info:
                app_ws = self.sh.worksheet(GROUP_LEADS_WORKSHEET)
                set_value("Ссылка на заявку", self._sheet_row_link(app_ws, int(lead_info["row_idx"]), "Открыть заявку"))
                set_value("Возраст", lead_info.get("age", ""))
                set_value("Наличие ПК/ноутбука", lead_info.get("pc", ""))
            else:
                set_value("Ссылка на заявку", "")
        except Exception:
            set_value("Ссылка на заявку", "")
        set_value("Статус", status)
        if auto_reply_enabled is not None:
            set_value("Автоответчик", "ON" if auto_reply_enabled else "OFF")
        set_value("Последнее входящее", last_in)
        set_value("Последнее исходящее", last_out)
        set_value("Peer ID", str(peer_id))
        set_value("Тех. шаг", tech_step or step_snapshot)
        set_value("Обновлено", now_iso)
        set_value("Аккаунт", ACCOUNT_KEY)
        if candidate_note_append:
            notes_idx = col_idx("Відповіді кандидата")
            if notes_idx is not None:
                current_notes = existing[notes_idx] if notes_idx < len(existing) else ""
                merged = candidate_note_append if not current_notes else f"{current_notes}\n---\n{candidate_note_append}"
                existing[notes_idx] = merged

        event_type = self._event_type(status, auto_reply_enabled, last_in, last_out, event_type_override)
        history_link = self.append_history_event(
            tz=tz,
            event_type=event_type,
            peer_id=peer_id,
            name=name,
            username=username,
            chat_link=chat_link,
            status=status,
            auto_reply_enabled=auto_reply_enabled,
            last_in=last_in,
            last_out=last_out,
            sender_role=sender_role,
            dialog_mode=dialog_mode,
            step_snapshot=step_snapshot or tech_step,
            full_text=full_text,
        )
        set_value("Ссылка на журнал", history_link or "")

        try:
            if row_idx:
                end_col = self._col_letter(len(headers))
                ws.update(range_name=f"A{row_idx}:{end_col}{row_idx}", values=[existing], value_input_option="USER_ENTERED")
            else:
                row_idx_recheck, existing_recheck = self._find_row(ws, peer_id, ACCOUNT_KEY)
                if row_idx_recheck:
                    row_idx = row_idx_recheck
                    existing = existing_recheck or [""] * len(headers)
                    if len(existing) < len(headers):
                        existing = existing + [""] * (len(headers) - len(existing))
                    end_col = self._col_letter(len(headers))
                    ws.update(range_name=f"A{row_idx}:{end_col}{row_idx}", values=[existing], value_input_option="USER_ENTERED")
                else:
                    next_row = int(self._next_row_cache.get(ws.id, 2))
                    end_col = self._col_letter(len(headers))
                    ws.update(range_name=f"A{next_row}:{end_col}{next_row}", values=[existing], value_input_option="USER_ENTERED")
                    self._row_index_cache.setdefault(ws.id, {})[(str(peer_id), ACCOUNT_KEY)] = next_row
                    self._next_row_cache[ws.id] = next_row + 1
        except Exception as err:
            print(f"⚠️ Не вдалося записати лист '{TODAY_WORKSHEET}': {err}")
            self._invalidate_ws_cache(ws)
            return
        self._sort_today_by_updated(ws, headers)

    def load_enabled_peers(self, tz: ZoneInfo) -> set:
        ws = self._ensure_today_ws(tz)
        values = ws.get_all_values()
        if not values:
            return set()
        headers = [h.strip() for h in values[0]]
        try:
            peer_idx = headers.index("Peer ID")
            auto_idx = headers.index("Автоответчик")
            account_idx = headers.index("Аккаунт")
        except ValueError:
            return set()
        enabled = set()
        for row in values[1:]:
            if peer_idx >= len(row) or auto_idx >= len(row) or account_idx >= len(row):
                continue
            if row[account_idx].strip() != ACCOUNT_KEY:
                continue
            peer_raw = row[peer_idx].strip()
            auto_raw = row[auto_idx].strip().lower()
            if peer_raw.isdigit() and auto_raw in {"on", "1", "yes", "true", "enabled"}:
                enabled.add(int(peer_raw))
        return enabled


class LocalPauseStore(LocalPauseStoreStore):
    def __init__(self, path: str):
        super().__init__(
            path,
            now_factory=lambda: datetime.now(ZoneInfo(TIMEZONE)),
        )


class GroupLeadsSheet:
    def __init__(self):
        self.gc = sheets_client(GOOGLE_CREDS)
        self.sh = self.gc.open(SHEET_NAME)
        self.ws = get_or_create_worksheet(self.sh, GROUP_LEADS_WORKSHEET, rows=1000, cols=len(GROUP_LEADS_HEADERS))
        self._ensure_headers_exact()

    def _ensure_headers_exact(self):
        try:
            current = self.ws.row_values(1)
        except Exception:
            current = []
        if not current:
            self.ws.update(
                range_name=f"A1:{col_letter(len(GROUP_LEADS_HEADERS))}1",
                values=[GROUP_LEADS_HEADERS],
                value_input_option="USER_ENTERED",
            )
            return
        if current[: len(GROUP_LEADS_HEADERS)] != GROUP_LEADS_HEADERS:
            self.ws.update(
                range_name=f"A1:{col_letter(len(GROUP_LEADS_HEADERS))}1",
                values=[GROUP_LEADS_HEADERS],
                value_input_option="USER_ENTERED",
            )
        if len(current) > len(GROUP_LEADS_HEADERS):
            extra = len(current) - len(GROUP_LEADS_HEADERS)
            self.ws.update(
                range_name=f"{col_letter(len(GROUP_LEADS_HEADERS) + 1)}1:{col_letter(len(current))}1",
                values=[[""] * extra],
                value_input_option="USER_ENTERED",
            )

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

        tg_idx = (
            get_col("tg")
            if get_col("tg") is not None
            else (get_col("telegram") if get_col("telegram") is not None else get_col("тг"))
        )
        phone_idx = (
            get_col("phone")
            if get_col("phone") is not None
            else get_col("телефон")
        )
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
            next_row = len(values) + 1
            self.ws.update(f"A{next_row}:J{next_row}", [row], value_input_option="USER_ENTERED")


class RegistrationSheet:
    def __init__(self):
        self.gc = sheets_client(GOOGLE_CREDS)
        self.sh = self.gc.open(SHEET_NAME)
        self.ws = get_or_create_worksheet(self.sh, REGISTRATION_WORKSHEET, rows=1000, cols=len(REGISTRATION_HEADERS))
        self._ensure_headers_exact()

    def _ensure_headers_exact(self):
        try:
            current = self.ws.row_values(1)
            if not current:
                self.ws.update(
                    f"A1:{col_letter(len(REGISTRATION_HEADERS))}1",
                    [REGISTRATION_HEADERS],
                    value_input_option="USER_ENTERED",
                )
                return
            if current[: len(REGISTRATION_HEADERS)] != REGISTRATION_HEADERS:
                self.ws.update(
                    f"A1:{col_letter(len(REGISTRATION_HEADERS))}1",
                    [REGISTRATION_HEADERS],
                    value_input_option="USER_ENTERED",
                )
            if len(current) > len(REGISTRATION_HEADERS):
                extra = len(current) - len(REGISTRATION_HEADERS)
                self.ws.update(
                    f"{col_letter(len(REGISTRATION_HEADERS) + 1)}1:{col_letter(len(current))}1",
                    [[""] * extra],
                    value_input_option="USER_ENTERED",
                )
        except Exception as err:
            print(f"⚠️ Не вдалося оновити заголовки '{REGISTRATION_WORKSHEET}': {err}")

    def _find_row(self, values, source_group: str, source_message_id: str):
        if not values:
            return None
        headers = [h.strip() for h in values[0]]
        try:
            group_idx = headers.index("Группа-источник")
            msg_idx = headers.index("ID сообщения")
        except ValueError:
            return None
        for idx, row in enumerate(values[1:], start=2):
            group_val = row[group_idx].strip() if group_idx < len(row) else ""
            msg_val = row[msg_idx].strip() if msg_idx < len(row) else ""
            if group_val == source_group and msg_val == source_message_id:
                return idx
        return None

    def upsert(self, tz: ZoneInfo, data: dict):
        row = [
            data.get("full_name", ""),
            data.get("birth_date", ""),
            data.get("phone", ""),
            data.get("email", ""),
            data.get("candidate_tg", ""),
            data.get("schedule", ""),
            data.get("start_date", ""),
            data.get("city", ""),
            data.get("admin_tg", ""),
            data.get("document_drive_link", ""),
            data.get("message_link", ""),
            data.get("raw_text", ""),
            data.get("source_group", ""),
            str(data.get("source_message_id", "") or ""),
            datetime.now(tz).isoformat(timespec="seconds"),
        ]
        source_group = str(data.get("source_group", "") or "")
        source_message_id = str(data.get("source_message_id", "") or "")
        values = self.ws.get_all_values()
        row_idx = self._find_row(values, source_group, source_message_id)
        end_col = col_letter(len(REGISTRATION_HEADERS))
        if row_idx:
            self.ws.update(
                f"A{row_idx}:{end_col}{row_idx}",
                [row],
                value_input_option="USER_ENTERED",
            )
            return
        next_row = len(values) + 1
        self.ws.update(
            f"A{next_row}:{end_col}{next_row}",
            [row],
            value_input_option="USER_ENTERED",
        )


class FAQQuestionsSheet:
    def __init__(self):
        self.gc = sheets_client(GOOGLE_CREDS)
        self.sh = self.gc.open(SHEET_NAME)
        self.ws = get_or_create_worksheet(self.sh, FAQ_QUESTIONS_WORKSHEET, rows=1000, cols=len(FAQ_QUESTIONS_HEADERS))
        self._ensure_headers()

    def _ensure_headers(self):
        try:
            current = self.ws.row_values(1)
        except Exception:
            current = []
        if current[: len(FAQ_QUESTIONS_HEADERS)] != FAQ_QUESTIONS_HEADERS:
            self.ws.update(
                range_name=f"A1:{col_letter(len(FAQ_QUESTIONS_HEADERS))}1",
                values=[FAQ_QUESTIONS_HEADERS],
                value_input_option="USER_ENTERED",
            )

    def upsert_question(self, row: dict):
        values = self.ws.get_all_values()
        if not values:
            self._ensure_headers()
            values = self.ws.get_all_values()
        headers = [h.strip() for h in values[0]]
        try:
            cluster_idx = headers.index("cluster_key")
            count_idx = headers.index("count")
            last_seen_idx = headers.index("last_seen_at")
            answer_idx = headers.index("answer_preview")
        except ValueError:
            return
        target_row = None
        for idx, existing in enumerate(values[1:], start=2):
            key = existing[cluster_idx].strip() if cluster_idx < len(existing) else ""
            if key == row.get("cluster_key", ""):
                target_row = (idx, existing)
                break
        end_col = col_letter(len(FAQ_QUESTIONS_HEADERS))
        if target_row:
            row_idx, existing = target_row
            current_count = 0
            if count_idx < len(existing):
                try:
                    current_count = int(existing[count_idx] or 0)
                except ValueError:
                    current_count = 0
            merged = [row.get(h, "") for h in FAQ_QUESTIONS_HEADERS]
            merged[count_idx] = str(max(1, current_count + 1))
            merged[last_seen_idx] = row.get("last_seen_at", row.get("created_at", ""))
            if answer_idx < len(existing) and existing[answer_idx].strip():
                merged[answer_idx] = existing[answer_idx]
            self.ws.update(
                range_name=f"A{row_idx}:{end_col}{row_idx}",
                values=[merged],
                value_input_option="USER_ENTERED",
            )
            return
        next_row = len(values) + 1
        out = [row.get(h, "") for h in FAQ_QUESTIONS_HEADERS]
        self.ws.update(
            range_name=f"A{next_row}:{end_col}{next_row}",
            values=[out],
            value_input_option="USER_ENTERED",
        )


class FAQSuggestionsSheet:
    def __init__(self):
        self.gc = sheets_client(GOOGLE_CREDS)
        self.sh = self.gc.open(SHEET_NAME)
        self.ws = get_or_create_worksheet(self.sh, FAQ_SUGGESTIONS_WORKSHEET, rows=1000, cols=len(FAQ_SUGGESTIONS_HEADERS))
        self._ensure_headers()

    def _ensure_headers(self):
        try:
            current = self.ws.row_values(1)
        except Exception:
            current = []
        if current[: len(FAQ_SUGGESTIONS_HEADERS)] != FAQ_SUGGESTIONS_HEADERS:
            self.ws.update(
                range_name=f"A1:{col_letter(len(FAQ_SUGGESTIONS_HEADERS))}1",
                values=[FAQ_SUGGESTIONS_HEADERS],
                value_input_option="USER_ENTERED",
            )

    def append_if_missing(self, row: dict):
        values = self.ws.get_all_values()
        key = str(row.get("question_cluster", "")).strip()
        if not key:
            return
        for existing in values[1:]:
            if existing and existing[0].strip() == key:
                return
        next_row = len(values) + 1
        end_col = col_letter(len(FAQ_SUGGESTIONS_HEADERS))
        out = [row.get(h, "") for h in FAQ_SUGGESTIONS_HEADERS]
        self.ws.update(
            range_name=f"A{next_row}:{end_col}{next_row}",
            values=[out],
            value_input_option="USER_ENTERED",
        )


class GoogleDriveUploader:
    def __init__(self, creds_path: str, folder_id: str):
        self.creds_path = creds_path
        self.folder_id = (folder_id or "").strip()
        self._service = None

    def _get_service(self):
        if self._service is not None:
            return self._service
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        creds = Credentials.from_service_account_file(
            self.creds_path,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        self._service = build("drive", "v3", credentials=creds, cache_discovery=False)
        return self._service

    def upload_file(self, file_path: str, file_name: str, mime_type: Optional[str] = None) -> str:
        if not self.folder_id:
            raise ValueError("REGISTRATION_DRIVE_FOLDER_ID is empty")
        from googleapiclient.http import MediaFileUpload

        service = self._get_service()
        metadata = {"name": file_name, "parents": [self.folder_id]}
        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=False)
        created = service.files().create(
            body=metadata,
            media_body=media,
            fields="id,webViewLink,webContentLink",
            supportsAllDrives=True,
        ).execute()
        file_id = created.get("id")
        if not file_id:
            raise RuntimeError("Drive upload returned no file id")
        try:
            service.permissions().create(
                fileId=file_id,
                body={"role": "reader", "type": "anyone"},
                supportsAllDrives=True,
            ).execute()
        except Exception:
            pass
        return (
            created.get("webViewLink")
            or created.get("webContentLink")
            or f"https://drive.google.com/file/d/{file_id}/view"
        )

    def check_folder_access(self) -> Optional[str]:
        if not self.folder_id:
            return None
        service = self._get_service()
        info = service.files().get(
            fileId=self.folder_id,
            fields="id,name,driveId",
            supportsAllDrives=True,
        ).execute()
        return str(info.get("name") or self.folder_id)


def build_message_link(event) -> str:
    return build_registration_message_link(getattr(event, "chat_id", None), getattr(event, "id", None))


async def upload_media_to_drive(message, chat_id: int, message_id: int, uploader: GoogleDriveUploader) -> str:
    if not uploader:
        return ""
    os.makedirs(REGISTRATION_DOWNLOAD_DIR, exist_ok=True)
    file_obj = getattr(message, "file", None)
    ext = getattr(file_obj, "ext", None) or ""
    mime_type = getattr(file_obj, "mime_type", None)
    file_name = f"registration_{int(time.time())}_{message_id}{ext}"
    local_path = os.path.join(REGISTRATION_DOWNLOAD_DIR, file_name)
    downloaded_path = ""
    try:
        downloaded_path = await message.download_media(file=local_path)
        if not downloaded_path:
            raise RuntimeError("download_media returned empty path")
        drive_name = os.path.basename(downloaded_path)
        return await asyncio.to_thread(uploader.upload_file, downloaded_path, drive_name, mime_type)
    finally:
        try:
            if downloaded_path and os.path.exists(downloaded_path):
                os.remove(downloaded_path)
            elif os.path.exists(local_path):
                os.remove(local_path)
        except Exception as err:
            print(f"⚠️ Не вдалося видалити тимчасовий файл '{local_path}' peer={chat_id} msg={message_id}: {err}")


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


def detect_step_from_text(message_text: str) -> Optional[str]:
    msg_norm = normalize_text(message_text)
    if not msg_norm:
        return None
    best_step = None
    best_order = -1
    for tmpl_norm, step in TEMPLATE_TO_STEP.items():
        if tmpl_norm and tmpl_norm in msg_norm:
            order = STEP_ORDER.get(step, -1)
            if order > best_order:
                best_order = order
                best_step = step
    return best_step


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
    delay_before: Optional[float] = None,
    use_ai: bool = True,
    no_questions: bool = False,
    schedule_followup: bool = True,
    draft: Optional[str] = None,
    step_state: Optional["StepState"] = None,
    step_name: Optional[str] = None,
    auto_reply_enabled: Optional[bool] = None,
    followup_state: Optional["FollowupState"] = None,
    parse_mode: Optional[str] = None,
):
    history = []
    if use_ai:
        history = await build_ai_history(client, entity, limit=10)
    if PAUSE_CHECKER and PAUSE_CHECKER(entity):
        return text
    if delay_before is None:
        effective_delay = QUESTION_RESPONSE_DELAY_SEC if use_ai else BOT_REPLY_DELAY_SEC
    else:
        effective_delay = delay_before
    if effective_delay and effective_delay > 0:
        await asyncio.sleep(effective_delay)
    if PAUSE_CHECKER and PAUSE_CHECKER(entity):
        return text

    async def _ai_suggest(base_text: str) -> Optional[str]:
        if not use_ai:
            return None
        return await dialog_suggest(history, draft or base_text, no_questions=no_questions)

    sent_payload = {}

    async def _sender(message_text: str):
        kwargs = {"parse_mode": parse_mode} if parse_mode else {}
        sent_message = await client.send_message(entity, message_text, **kwargs)
        sent_payload["message"] = sent_message

    result = await send_message_with_fallback(
        text,
        ai_enabled=use_ai,
        no_questions=no_questions,
        ai_suggest=_ai_suggest,
        strip_question_trail=strip_question_trail,
        send=_sender,
    )
    message_text = result.text_used
    if not result.success:
        print(f"⚠️ Send error peer={entity.id} step={step_name or '-'} err={result.error}")
        return text
    sent_message = sent_payload.get("message")
    if not sent_message:
        return text
    try:
        track_sent_message(entity.id, sent_message.id)
    except Exception:
        pass
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
    payload = {
        "peer_id": entity.id,
        "name": name,
        "username": username,
        "chat_link": chat_link,
        "status": status,
        "auto_reply_enabled": auto_reply_enabled,
        "last_out": message_text[:200],
        "tech_step": step_name,
        "sender_role": "bot",
        "dialog_mode": "ON",
        "step_snapshot": step_name,
        "full_text": message_text,
    }
    if not enqueue_sheet_event("today_upsert", payload):
        try:
            sheet.upsert(tz=tz, **payload)
            print(f"AUTO_REPLY_CONTINUE despite_sheet_error peer={entity.id}")
        except Exception as err:
            print(f"⚠️ SHEETS_DIRECT_WRITE_FAIL peer={entity.id}: {type(err).__name__}: {err}")
    if schedule_followup and followup_state and status != AUTO_STOP_STATUS:
        followup_state.schedule_from_now(entity.id, tz)
        state = followup_state.get(entity.id)
        next_at = state.get("next_at")
        stage = state.get("stage")
        if next_at is not None and stage is not None:
            next_dt = datetime.fromtimestamp(float(next_at), tz)
            follow_payload = {
                "peer_id": entity.id,
                "name": name,
                "username": username,
                "chat_link": chat_link,
                "followup_stage": str(stage + 1),
                "followup_next_at": next_dt.isoformat(timespec="seconds"),
                "followup_last_sent_at": None,
            }
            if not enqueue_sheet_event("today_upsert", follow_payload):
                try:
                    sheet.upsert(tz=tz, **follow_payload)
                    print(f"AUTO_REPLY_CONTINUE despite_sheet_error peer={entity.id}")
                except Exception as err:
                    print(f"⚠️ SHEETS_DIRECT_WRITE_FAIL peer={entity.id}: {type(err).__name__}: {err}")
    if delay_after:
        await asyncio.sleep(delay_after)
    return message_text


def wants_video(text: str) -> bool:
    return wants_video_impl(text)


def fallback_format_choice(text: str) -> str:
    return fallback_format_choice_impl(text)


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
        peer_id = int(f"-100{chat_id}")
        return peer_id, message_id
    return chat_id, message_id


async def load_message_from_link(client: TelegramClient, link: str) -> Optional["Message"]:
    parsed = parse_message_link(link)
    if not parsed:
        return None
    peer, message_id = parsed
    try:
        entity = await client.get_entity(peer)
        msg = await client.get_messages(entity, ids=message_id)
    except Exception:
        return None
    if msg:
        save_video_cache(VIDEO_CACHE_PATH, entity.id, msg.id)
        return msg
    return None


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


def load_video_cache(path: str) -> Optional[Tuple[int, int]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception:
        return None
    peer_id = data.get("peer_id")
    message_id = data.get("message_id")
    if isinstance(peer_id, int) and isinstance(message_id, int):
        return peer_id, message_id
    return None


def save_video_cache(path: str, peer_id: int, message_id: int):
    if not path:
        return
    try:
        with open(path, "w") as f:
            json.dump(
                {"peer_id": int(peer_id), "message_id": int(message_id)},
                f,
                ensure_ascii=True,
            )
    except Exception:
        pass


async def load_cached_video_message(client: TelegramClient) -> Optional["Message"]:
    cached = load_video_cache(VIDEO_CACHE_PATH)
    if not cached:
        return None
    peer_id, message_id = cached
    try:
        entity = await client.get_entity(peer_id)
        msg = await client.get_messages(entity, ids=message_id)
    except Exception:
        return None
    if msg and (msg.video or (msg.media and getattr(msg.media, "document", None))):
        return msg
    return None


async def dialog_suggest(
    history: list,
    draft: str,
    no_questions: bool = False,
    combined_answer_clarify: bool = False,
) -> Optional[str]:
    if not DIALOG_AI_URL:
        return None
    payload = {
        "history": history,
        "draft": draft,
        "no_questions": bool(no_questions),
        "combined_answer_clarify": bool(combined_answer_clarify),
    }
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


async def detect_format_choice(history: list, text: str) -> str:
    async def _ai_client(hist: list, last_text: str) -> str:
        if not DIALOG_FORMAT_URL:
            return "unknown"
        payload = {"history": hist, "last_message": last_text}
        try:
            data = await asyncio.to_thread(_post_json, DIALOG_FORMAT_URL, payload, DIALOG_FORMAT_TIMEOUT_SEC)
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, OSError) as err:
            print(f"⚠️ AI format error: {err}")
            return "unknown"
        if not data or not data.get("ok"):
            return "unknown"
        return (data.get("choice") or "").strip().lower()

    return await classify_format_choice(text, history, ai_client=_ai_client)


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


class StepState(StepStateStore):
    def __init__(self, path: str):
        super().__init__(path, STEP_ORDER)


def status_for_text(text: str) -> Optional[str]:
    t = normalize_text(text)
    for template, status in STATUS_BY_TEMPLATE.items():
        if template and template in t:
            return status
    return None


async def main():
    tz = ZoneInfo(TIMEZONE)
    sheet = SheetWriter()
    pause_store = LocalPauseStore(PAUSED_STATE_PATH)
    group_leads_sheet = GroupLeadsSheet()
    faq_questions_sheet = None
    faq_suggestions_sheet = None
    try:
        faq_questions_sheet = FAQQuestionsSheet()
        faq_suggestions_sheet = FAQSuggestionsSheet()
    except Exception as err:
        print(f"⚠️ Не вдалося підготувати FAQ-листи: {err}")
    v2_enrollment = V2EnrollmentStore(V2_ENROLLMENT_PATH)
    v2_runtime = V2RuntimeStore(V2_RUNTIME_PATH)
    content_env_map = {
        "VOICE_MESSAGE_LINK": VOICE_MESSAGE_LINK,
        "PHOTO_1_MESSAGE_LINK": PHOTO_1_MESSAGE_LINK,
        "PHOTO_2_MESSAGE_LINK": PHOTO_2_MESSAGE_LINK,
        "TEST_TASK_MESSAGE_LINK": TEST_TASK_MESSAGE_LINK,
        "FORM_MESSAGE_LINK": FORM_MESSAGE_LINK,
    }
    v2_content_validation = validate_content_env(content_env_map)
    if v2_content_validation.get("missing"):
        print(f"⚠️ V2 content env missing: {v2_content_validation.get('missing')}")
    registration_sheet = None
    try:
        registration_sheet = RegistrationSheet()
    except Exception as err:
        print(f"⚠️ Не вдалося підготувати лист '{REGISTRATION_WORKSHEET}': {err}")
    registration_drive = None
    if REGISTRATION_DRIVE_FOLDER_ID:
        try:
            registration_drive = GoogleDriveUploader(GOOGLE_CREDS, REGISTRATION_DRIVE_FOLDER_ID)
            try:
                folder_name = registration_drive.check_folder_access()
                print(f"✅ Drive папка доступна: {folder_name} ({REGISTRATION_DRIVE_FOLDER_ID})")
            except Exception as err:
                print(
                    "⚠️ Немає доступу до Drive папки "
                    f"{REGISTRATION_DRIVE_FOLDER_ID}: {type(err).__name__}: {err}"
                )
        except Exception as err:
            print(f"⚠️ Не вдалося ініціалізувати Google Drive uploader: {err}")
    else:
        print("⚠️ REGISTRATION_DRIVE_FOLDER_ID не задано: документи не будуть завантажуватись у Drive")
    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    processing_peers = set()
    buffered_incoming: Dict[int, deque] = {}
    paused_peers = set()
    enabled_peers = set()
    last_reply_at = {}
    last_incoming_at = {}
    pending_question_resume = {}
    skip_stop_check_once = set()
    format_delivery_state = {}
    step_state = StepState(STEP_STATE_PATH)
    followup_state = FollowupState(FOLLOWUP_STATE_PATH)
    qa_gate_state = {}
    sheets_queue = None
    try:
        sheets_queue = SheetsQueueStore(SHEETS_QUEUE_PATH)
    except Exception as err:
        print(f"⚠️ SHEETS_QUEUE_INIT_FAIL path={SHEETS_QUEUE_PATH}: {type(err).__name__}: {err}")
    last_queue_log_at = 0.0
    pending_registration_tasks = {}
    stop_event = asyncio.Event()

    def queue_today_upsert(**kwargs):
        if enqueue_sheet_event("today_upsert", kwargs):
            return True
        try:
            sheet.upsert(tz=tz, **kwargs)
            print(f"AUTO_REPLY_CONTINUE despite_sheet_error peer={kwargs.get('peer_id', '')}")
            return True
        except Exception as err:
            print(f"⚠️ SHEETS_DIRECT_WRITE_FAIL peer={kwargs.get('peer_id', '')}: {type(err).__name__}: {err}")
            return False
    try:
        enabled_peers = sheet.load_enabled_peers(tz)
    except Exception:
        enabled_peers = set()
    enabled_peers.update(pause_store.active_peer_ids())

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
        return False

    global PAUSE_CHECKER
    PAUSE_CHECKER = is_paused
    global SHEETS_EVENT_ENQUEUER
    SHEETS_EVENT_ENQUEUER = sheets_queue.enqueue if sheets_queue else None

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
    traffic_group = await find_group_by_title(client, TRAFFIC_GROUP_TITLE)
    if not traffic_group:
        print(f"⚠️ Не знайшов групу трафіку: {TRAFFIC_GROUP_TITLE}")

    video_group = None
    video_message = None
    video_from_link = False
    video_from_cache = False
    if VIDEO_MESSAGE_LINK:
        video_message = await load_message_from_link(client, VIDEO_MESSAGE_LINK)
        if video_message:
            video_from_link = True
            print("✅ Використовую відео з посилання")
    if not video_message:
        video_message = await load_cached_video_message(client)
        if video_message:
            video_from_cache = True
    if video_from_cache:
        print("✅ Використовую кеш відео")
    if VIDEO_GROUP_LINK:
        try:
            video_group = await client.get_entity(VIDEO_GROUP_LINK)
        except Exception:
            video_group = None
    if not video_group and VIDEO_GROUP_TITLE:
        video_group = await find_group_by_title(client, VIDEO_GROUP_TITLE)
    if not video_message and not video_group:
        print("⚠️ Не знайшов групу з відео")
    if not video_message and video_group:
        async for m in client.iter_messages(video_group, limit=50):
            if m.video or (m.media and getattr(m.media, "document", None)):
                video_message = m
                save_video_cache(VIDEO_CACHE_PATH, video_group.id, m.id)
                print("✅ Знайшов відео та зберіг у кеш")
                break
    if not video_message:
        print("⚠️ Не знайшов відео у групі для пересилання")

    async def reconcile_dialog_step(entity: User, use_cache: bool = True) -> Tuple[Optional[str], str]:
        if use_cache:
            cached = step_state.get(entity.id)
            if cached:
                return cached, "state"
        history_step = await get_last_outgoing_step(client, entity)
        if history_step:
            step_state.set(entity.id, history_step)
            return history_step, "history"
        return None, "none"

    async def send_ai_response(
        entity: User,
        status: Optional[str] = None,
        history_override: Optional[list] = None,
        append_clarify: bool = False,
    ) -> bool:
        if is_paused(entity):
            return False
        history = history_override or await build_ai_history(client, entity, limit=10)
        if append_clarify:
            draft = (
                "Відповідь має бути одним повідомленням.\n"
                "Спочатку коротко відповідай по суті на питання кандидата.\n"
                "В кінці додай один короткий уточнюючий запит у формі питання.\n"
                "Не розділяй відповідь на окремі повідомлення."
            )
            ai_text = await dialog_suggest(
                history,
                draft,
                no_questions=False,
                combined_answer_clarify=True,
            )
        else:
            ai_text = await dialog_suggest(history, "", no_questions=True)
        if not ai_text:
            return False
        final_text = ai_text if append_clarify else strip_question_trail(ai_text)
        if append_clarify and not message_has_question(final_text):
            return False
        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            final_text,
            status,
            use_ai=False,
            delay_before=QUESTION_RESPONSE_DELAY_SEC,
            step_state=step_state,
            followup_state=followup_state,
        )
        return True

    def set_qa_gate(peer_id: int, step: Optional[str]):
        qa_gate_state[peer_id] = {
            "qa_gate_active": True,
            "qa_gate_step": step or "",
            "qa_gate_reminder_sent": False,
            "qa_gate_opened_at": time.time(),
        }

    def clear_qa_gate(peer_id: int):
        qa_gate_state.pop(peer_id, None)

    async def send_ai_detailed_answer(entity: User, history_override: Optional[list] = None, step_name: Optional[str] = None) -> bool:
        if is_paused(entity):
            return False
        history = history_override or await build_ai_history(client, entity, limit=10)
        draft = (
            "Дай розгорнуту, але структуровану відповідь у межах FAQ і політик.\n"
            "Поясни по суті простими словами, без зайвої води.\n"
            "Не став запитань у цьому повідомленні."
        )
        ai_text = await dialog_suggest(history, draft, no_questions=True)
        if not ai_text:
            return False
        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            ai_text.strip(),
            "знак питання",
            use_ai=False,
            delay_before=QUESTION_RESPONSE_DELAY_SEC,
            step_state=step_state,
            step_name=step_name,
            followup_state=followup_state,
        )
        return True

    async def send_qa_gate_confirm(entity: User, step_name: Optional[str]):
        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            QA_GATE_CONFIRM_TEXT,
            "знак питання",
            use_ai=False,
            delay_before=QUESTION_GAP_SEC,
            step_state=step_state,
            step_name=step_name,
            followup_state=followup_state,
        )

    def peer_format_state(peer_id: int) -> dict:
        state = format_delivery_state.get(peer_id)
        if state is None:
            state = {"video_sent": False, "mini_course_sent": False}
            format_delivery_state[peer_id] = state
        return state

    def mark_format_stage_ready(entity: User):
        step_state.set(entity.id, STEP_VIDEO_FOLLOWUP)

    async def send_video_option(entity: User) -> bool:
        state = peer_format_state(entity.id)
        if state.get("video_sent"):
            return False
        if not video_message:
            return False
        await asyncio.sleep(15)
        sent = None
        try:
            if VIDEO_MESSAGE_LINK:
                sent = await client.forward_messages(entity, video_message)
            elif video_message.media:
                sent = await client.send_file(
                    entity,
                    video_message.media,
                    caption=video_message.message or "",
                )
            elif video_message.message:
                sent = await client.send_message(entity, video_message.message)
        except Exception:
            print("⚠️ Не вдалося надіслати відео")
            return False
        try:
            if isinstance(sent, list):
                for msg in sent:
                    track_sent_message(entity.id, msg.id)
            elif sent:
                track_sent_message(entity.id, sent.id)
        except Exception:
            pass
        mark_format_stage_ready(entity)
        state["video_sent"] = True
        return True

    async def send_mini_course_option(entity: User) -> bool:
        state = peer_format_state(entity.id)
        if state.get("mini_course_sent"):
            return False
        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            MINI_COURSE_LINK,
            status_for_text(MINI_COURSE_LINK) or status_for_text(FORMAT_QUESTION_TEXT),
            use_ai=False,
            no_questions=True,
            draft=MINI_COURSE_LINK,
            step_state=step_state,
            followup_state=followup_state,
        )
        state["mini_course_sent"] = True
        mark_format_stage_ready(entity)
        return True

    async def handle_format_choice(entity: User, choice: str) -> str:
        state = peer_format_state(entity.id)
        sent_video = False
        sent_mini = False

        if choice == "video":
            sent_video = await send_video_option(entity)
            if sent_video:
                last_reply_at[entity.id] = time.time()
                return "video"
            return "none"

        if choice == "mini_course":
            sent_mini = await send_mini_course_option(entity)
            if sent_mini:
                last_reply_at[entity.id] = time.time()
                return "mini_course"
            return "none"

        if choice == "both":
            if not state.get("mini_course_sent"):
                sent_mini = await send_mini_course_option(entity)
            if not state.get("video_sent"):
                sent_video = await send_video_option(entity)
            if sent_mini or sent_video:
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    BOTH_FORMATS_FOLLOWUP_TEXT,
                    status_for_text(BOTH_FORMATS_FOLLOWUP_TEXT) or status_for_text(FORMAT_QUESTION_TEXT),
                    use_ai=False,
                    no_questions=True,
                    draft=BOTH_FORMATS_FOLLOWUP_TEXT,
                    step_state=step_state,
                    followup_state=followup_state,
                )
                mark_format_stage_ready(entity)
                last_reply_at[entity.id] = time.time()
                return "both"
            return "none"

        return "unknown"

    def parse_age_bucket(text: str) -> str:
        t = normalize_text(text)
        if not t:
            return "unknown"
        m = re.search(r"(\\d{2})", t)
        if not m:
            return "unknown"
        age = int(m.group(1))
        if age < 18:
            return "under18"
        if age > 40:
            return "over40"
        return "ok"

    async def send_v2_message(
        entity: User,
        text: str,
        step_name: str,
        status: Optional[str] = None,
        delay_before: Optional[float] = None,
    ):
        parse_mode = "md" if "[сайт](" in (text or "") else None
        await send_and_update(
            client,
            sheet,
            tz,
            entity,
            text,
            status or status_for_text(text) or "V2",
            use_ai=False,
            no_questions=False,
            draft=text,
            delay_before=delay_before,
            step_state=step_state,
            step_name=step_name,
            followup_state=followup_state,
            parse_mode=parse_mode,
        )

    def enqueue_candidate_note(entity: User, text: str):
        note = (text or "").strip()
        if not note:
            return
        append_candidate_answers(
            lambda event_type, payload: enqueue_sheet_event(event_type, payload),
            peer_id=entity.id,
            name=getattr(entity, "first_name", "") or "Unknown",
            username=getattr(entity, "username", "") or "",
            chat_link=build_chat_link_app(entity, entity.id),
            note_entry=note,
        )

    async def resolve_v2_intent(sender: User, text: str, step_name: str) -> str:
        local = detect_intent_v2(text, step_name).intent
        if local in {"question", "stop"}:
            return local
        critical_steps = {
            STEP_COMPANY_INTRO,
            STEP_SCHEDULE_SHIFT_WAIT,
            STEP_SCHEDULE_CONFIRM,
            STEP_TEST_REVIEW,
            STEP_VOICE_WAIT,
        }
        if local == "ack_continue" and step_name not in critical_steps:
            return local
        history = await build_ai_history(client, sender, limit=8)
        ai_intent = (await classify_candidate_intent(history, text, step_name)).value
        if ai_intent in {"question", "ack_continue", "stop"}:
            return ai_intent
        return local

    async def dispatch_v2_content(sender: User, content_link: str, step_name: str, status: str) -> bool:
        res = await dispatch_content(client, sender, content_link)
        if not res.ok:
            return False
        queue_today_upsert(
            peer_id=sender.id,
            name=getattr(sender, "first_name", "") or "Unknown",
            username=getattr(sender, "username", "") or "",
            chat_link=build_chat_link_app(sender, sender.id),
            status=status,
            sender_role="bot",
            dialog_mode="ON",
            step_snapshot=step_name,
            tech_step=step_name,
            full_text=(res.preview or "forwarded").strip()[:2000],
            last_out=(res.preview or "forwarded").strip()[:200],
        )
        return True

    async def finalize_test_review(sender: User, state: PeerRuntimeState, answers: List[str]) -> bool:
        enqueue_candidate_note(sender, format_numbered_answers(answers))
        ok_answers, review_text = evaluate_test_answers(answers)
        await send_v2_message(
            sender,
            review_text,
            STEP_TEST_REVIEW,
            status="🎓 Навчання",
            delay_before=QUESTION_RESPONSE_DELAY_SEC,
        )
        if FORM_MESSAGE_LINK:
            ok = await dispatch_v2_content(sender, FORM_MESSAGE_LINK, STEP_FORM_FORWARD, "📝 Анкета")
            if not ok:
                await send_v2_message(sender, FORM_TEXT, STEP_FORM_FORWARD, status="📝 Анкета")
        else:
            await send_v2_message(sender, FORM_TEXT, STEP_FORM_FORWARD, status="📝 Анкета")
        state.flow_step = STEP_FORM_FORWARD
        state.test_answers = answers[:3]
        state.test_help_sent = False
        state.test_prompted_at = 0.0
        state.test_message_count = 0
        state.test_last_message = ""
        v2_runtime.set(state)
        return ok_answers

    def enqueue_faq_question(peer_id: int, step: str, question_raw: str, answer_preview: str):
        q_norm = normalize_question(question_raw)
        cluster_key = build_cluster_key(q_norm)
        qlog = build_question_log(
            tz=tz,
            peer_id=peer_id,
            step=step,
            question_raw=question_raw,
            question_norm=q_norm,
            cluster_key=cluster_key,
            answer_preview=answer_preview,
        )
        enqueue_sheet_event("faq_question_log", qlog.__dict__)

    async def handle_v2_message(sender: User, text: str, intent_name: str) -> bool:
        if not FLOW_V2_ENABLED or not v2_enrollment.has(sender.id):
            return False
        state = v2_runtime.get(sender.id)
        step_name = state.flow_step or STEP_SCREENING_WAIT

        if state.rejected_by_age in {"under18", "over40"}:
            if not state.referral_after_reject_sent:
                await send_v2_message(sender, REFERRAL_AFTER_REJECT_TEXT, STEP_AGE_REJECTED, status=REFERRAL_STATUS)
                state.referral_after_reject_sent = True
                state.auto_mode = "OFF"
                state.paused = True
                paused_peers.add(sender.id)
                enabled_peers.discard(sender.id)
                v2_runtime.set(state)
            return True

        if state.qa_gate_active:
            if (state.qa_gate_step or step_name) == STEP_SCHEDULE_SHIFT_WAIT and intent_name == "question":
                history = await build_ai_history(client, sender, limit=12)
                ans = await answer_from_faq(text, STEP_SCHEDULE_SHIFT_WAIT, history, dialog_suggest, mode="detailed")
                answer_text = ans.text if ans else "Уточню коротко: у нас доступні денна або нічна зміна."
                await send_v2_message(
                    sender,
                    answer_text,
                    STEP_SCHEDULE_SHIFT_WAIT,
                    status="знак питання",
                    delay_before=QUESTION_RESPONSE_DELAY_SEC,
                )
                await send_v2_message(sender, "Підкажи, будь ласка, яку зміну обираєш: денну чи нічну?", STEP_SCHEDULE_SHIFT_WAIT)
                state.qa_gate_active = False
                state.qa_gate_step = ""
                state.qa_gate_reminder_sent = False
                state.qa_gate_opened_at = 0.0
                state.shift_prompted_at = time.time()
                v2_runtime.set(state)
                enqueue_faq_question(sender.id, STEP_SCHEDULE_SHIFT_WAIT, text, answer_text)
                return True
            if intent_name == "question":
                history = await build_ai_history(client, sender, limit=12)
                ans = await answer_from_faq(text, state.qa_gate_step or step_name, history, dialog_suggest, mode="detailed")
                answer_text = ans.text if ans else "Уточню деталі по вашому питанню і повернуся з точною відповіддю."
                await send_v2_message(
                    sender,
                    answer_text,
                    state.qa_gate_step or step_name,
                    status="знак питання",
                    delay_before=QUESTION_RESPONSE_DELAY_SEC,
                )
                await send_v2_message(sender, V2_GATE_CONFIRM_TEXT, state.qa_gate_step or step_name, status="знак питання", delay_before=QUESTION_GAP_SEC)
                state.qa_gate_opened_at = time.time()
                state.qa_gate_reminder_sent = False
                v2_runtime.set(state)
                enqueue_faq_question(sender.id, state.qa_gate_step or step_name, text, answer_text)
                return True
            if intent_name == "ack_continue":
                state.qa_gate_active = False
                state.qa_gate_step = ""
                state.qa_gate_reminder_sent = False
                state.qa_gate_opened_at = 0.0
                v2_runtime.set(state)
                intent_name = "ack_continue"
            elif intent_name == "stop":
                await send_v2_message(sender, STOP_REPLY_TEXT, step_name, status=AUTO_STOP_STATUS)
                state.auto_mode = "OFF"
                state.paused = True
                paused_peers.add(sender.id)
                enabled_peers.discard(sender.id)
                v2_runtime.set(state)
                return True
            else:
                await send_v2_message(sender, V2_GATE_CONFIRM_TEXT, state.qa_gate_step or step_name, status="знак питання")
                return True

        if step_name == STEP_SCHEDULE_SHIFT_WAIT and intent_name == "question":
            history = await build_ai_history(client, sender, limit=12)
            ans = await answer_from_faq(text, STEP_SCHEDULE_SHIFT_WAIT, history, dialog_suggest, mode="detailed")
            answer_text = ans.text if ans else "Уточню коротко: у нас доступні денна або нічна зміна."
            await send_v2_message(
                sender,
                answer_text,
                STEP_SCHEDULE_SHIFT_WAIT,
                status="знак питання",
                delay_before=QUESTION_RESPONSE_DELAY_SEC,
            )
            await send_v2_message(sender, "Підкажи, будь ласка, яку зміну обираєш: денну чи нічну?", STEP_SCHEDULE_SHIFT_WAIT)
            state.shift_prompted_at = time.time()
            v2_runtime.set(state)
            enqueue_faq_question(sender.id, STEP_SCHEDULE_SHIFT_WAIT, text, answer_text)
            return True

        if intent_name == "question":
            history = await build_ai_history(client, sender, limit=12)
            ans = await answer_from_faq(text, step_name, history, dialog_suggest, mode="detailed")
            answer_text = ans.text if ans else "Уточню деталі по вашому питанню і повернуся з точною відповіддю."
            await send_v2_message(sender, answer_text, step_name, status="знак питання", delay_before=QUESTION_RESPONSE_DELAY_SEC)
            await send_v2_message(sender, V2_GATE_CONFIRM_TEXT, step_name, status="знак питання", delay_before=QUESTION_GAP_SEC)
            state.qa_gate_active = True
            state.qa_gate_step = step_name
            state.qa_gate_opened_at = time.time()
            state.qa_gate_reminder_sent = False
            v2_runtime.set(state)
            enqueue_faq_question(sender.id, step_name, text, answer_text)
            return True

        if intent_name == "stop":
            await send_v2_message(sender, STOP_REPLY_TEXT, step_name, status=AUTO_STOP_STATUS)
            state.auto_mode = "OFF"
            state.paused = True
            paused_peers.add(sender.id)
            enabled_peers.discard(sender.id)
            v2_runtime.set(state)
            return True

        if step_name == STEP_SCREENING_WAIT:
            chunks = split_answer_lines(text)
            if chunks:
                state.screening_answers = (state.screening_answers or []) + chunks
                state.screening_answers = state.screening_answers[:3]
                now_ts = time.time()
                if not state.screening_started_at:
                    state.screening_started_at = now_ts
                state.screening_last_at = now_ts
            if len(state.screening_answers or []) < 3:
                v2_runtime.set(state)
                return True

            screened = (state.screening_answers or [])[:3]
            enqueue_candidate_note(sender, format_numbered_answers(screened))
            age_bucket = parse_age_bucket("\n".join(screened))
            actions = advance_flow_v2(state, intent_name, {"age_bucket": age_bucket})
            if actions.route == "age_reject":
                reject_text = AGE_UNDER18_TEXT if age_bucket == "under18" else AGE_OVER40_TEXT
                await send_v2_message(sender, reject_text, STEP_AGE_REJECTED, status=AUTO_STOP_STATUS)
                state.flow_step = STEP_AGE_REJECTED
                state.rejected_by_age = age_bucket
                state.auto_mode = "OFF"
                state.paused = True
                paused_peers.add(sender.id)
                enabled_peers.discard(sender.id)
                v2_runtime.set(state)
                return True
            # For screening transition use intro without "Дякую за відповіді" to keep wording consistent.
            await send_v2_message(sender, COMPANY_INTRO_TIMEOUT_TEXT, STEP_COMPANY_INTRO, status="🏢 Знайомство з компанією")
            state.flow_step = STEP_COMPANY_INTRO
            state.screening_answers = screened
            state.screening_started_at = 0.0
            state.screening_last_at = 0.0
            v2_runtime.set(state)
            return True

        if step_name == STEP_COMPANY_INTRO:
            if intent_name == "ack_continue" and VOICE_MESSAGE_LINK:
                ok = await dispatch_v2_content(sender, VOICE_MESSAGE_LINK, STEP_COMPANY_INTRO, "🎧 Голосове")
                if not ok:
                    await send_v2_message(sender, "Зараз не вдалося надіслати голосове. Коротко поясню далі в чаті.", STEP_COMPANY_INTRO)
                else:
                    state.flow_step = STEP_VOICE_WAIT
                    state.voice_stage = VOICE_SENT
                    state.voice_sent_at = time.time()
                    v2_runtime.set(state)
                    return True
            await send_v2_message(sender, SCHEDULE_SHIFT_TEXT, STEP_SCHEDULE_SHIFT_WAIT, status="🕒 Графік")
            state.flow_step = STEP_SCHEDULE_SHIFT_WAIT
            state.shift_prompted_at = time.time()
            v2_runtime.set(state)
            return True

        if step_name == STEP_VOICE_WAIT:
            await send_v2_message(sender, SCHEDULE_SHIFT_TEXT, STEP_SCHEDULE_SHIFT_WAIT, status="🕒 Графік")
            state.flow_step = STEP_SCHEDULE_SHIFT_WAIT
            state.shift_prompted_at = time.time()
            v2_runtime.set(state)
            return True

        if step_name == STEP_SCHEDULE_SHIFT_WAIT:
            choice = parse_shift_choice(text)
            if not choice:
                await send_v2_message(sender, "Підкажи, будь ласка, яку зміну обираєш: денну чи нічну?", STEP_SCHEDULE_SHIFT_WAIT)
                state.shift_prompted_at = time.time()
                v2_runtime.set(state)
                return True
            enqueue_candidate_note(sender, f"Графік: {choice}")
            await send_v2_message(sender, SCHEDULE_DETAILS_TEXT, STEP_SCHEDULE_BLOCK, status="🕒 Графік")
            await send_v2_message(sender, SCHEDULE_CONFIRM_TEXT, STEP_SCHEDULE_CONFIRM, status="🕒 Графік")
            state.flow_step = STEP_SCHEDULE_CONFIRM
            state.shift_choice = choice
            v2_runtime.set(state)
            return True

        if step_name == STEP_SCHEDULE_CONFIRM:
            if intent_name != "ack_continue":
                await send_v2_message(sender, SCHEDULE_CONFIRM_TEXT, STEP_SCHEDULE_CONFIRM)
                return True
            links = [PHOTO_1_MESSAGE_LINK, PHOTO_2_MESSAGE_LINK, TEST_TASK_MESSAGE_LINK]
            missing = [l for l in links if not l]
            if missing:
                await send_v2_message(sender, "Контент для наступного етапу тимчасово недоступний. Зараз уточню і повернусь до вас.", STEP_PROOF_FORWARD)
                return True
            for link in links:
                ok = await dispatch_v2_content(sender, link, STEP_PROOF_FORWARD, "🎥 Більше інформації")
                if not ok:
                    await send_v2_message(sender, "Не вдалося надіслати один із матеріалів. Повторю трохи пізніше.", STEP_PROOF_FORWARD)
                    break
                await asyncio.sleep(BOT_REPLY_DELAY_SEC)
            state.flow_step = STEP_TEST_REVIEW
            state.test_answers = []
            state.test_prompted_at = time.time()
            state.test_help_sent = False
            state.test_message_count = 0
            state.test_last_message = ""
            v2_runtime.set(state)
            return True

        if step_name == STEP_TEST_REVIEW:
            answers = state.test_answers or ["", "", ""]
            candidate_lines = split_answer_lines(text)
            for line in candidate_lines:
                line_l = normalize_text(line)
                idx = None
                m = re.match(r"^\s*([123])[.):\-]?\s*(.+)$", line, flags=re.IGNORECASE)
                if m:
                    idx = int(m.group(1)) - 1
                    line = m.group(2).strip()
                    line_l = normalize_text(line)
                elif "%" in line_l:
                    idx = 1
                elif "год" in line_l or "8" in line_l:
                    idx = 0
                elif any(k in line_l for k in ("hr", "ейчар", "рішен", "решен", "перев", "звіль", "увільн", "звiль")):
                    idx = 2
                if idx is None:
                    for i in range(3):
                        if not answers[i]:
                            idx = i
                            break
                if idx is not None:
                    answers[idx] = line
            state.test_answers = answers
            if not all((x or "").strip() for x in answers):
                v2_runtime.set(state)
                return True
            enqueue_candidate_note(sender, format_numbered_answers(answers))
            ok, review_text = evaluate_test_answers(answers)
            await send_v2_message(
                sender,
                review_text,
                STEP_TEST_REVIEW,
                status="🎓 Навчання",
                delay_before=QUESTION_RESPONSE_DELAY_SEC,
            )
            if FORM_MESSAGE_LINK:
                ok = await dispatch_v2_content(sender, FORM_MESSAGE_LINK, STEP_FORM_FORWARD, "📝 Анкета")
                if not ok:
                    await send_v2_message(sender, FORM_TEXT, STEP_FORM_FORWARD, status="📝 Анкета")
            else:
                await send_v2_message(sender, FORM_TEXT, STEP_FORM_FORWARD, status="📝 Анкета")
            state.flow_step = STEP_FORM_FORWARD
            v2_runtime.set(state)
            return True

        if step_name == STEP_FORM_FORWARD:
            enqueue_candidate_note(sender, text)
            await send_v2_message(sender, "Дякую! Передаю вашу анкету тімліду, далі звʼяжемось по старту 🙌", STEP_HANDOFF, status=CONFIRM_STATUS)
            state.flow_step = STEP_HANDOFF
            v2_runtime.set(state)
            return True

        return True

    async def process_v2_turn(sender: User, text: str) -> bool:
        peer_id = sender.id
        if not v2_enrollment.has(peer_id):
            v2_enrollment.add(peer_id)
            seeded_state = PeerRuntimeState(
                peer_id=peer_id,
                flow_step=STEP_SCREENING_WAIT,
                auto_mode="ON",
                paused=False,
                screening_started_at=time.time(),
            )
            v2_runtime.set(seeded_state)
            await send_v2_message(sender, SCREENING_INTRO_TEXT, STEP_SCREENING_WAIT, status="👋 Привітання")
            print(f"✅ V2 auto-enrolled peer={peer_id}")
            return True
        v2_state = v2_runtime.get(peer_id)
        v2_intent = await resolve_v2_intent(sender, text, v2_state.flow_step)
        handled_v2 = await handle_v2_message(sender, text, v2_intent)
        if not handled_v2:
            print(f"⚠️ V2 handler returned no-op peer={peer_id} step={v2_state.flow_step}")
        return handled_v2

    async def continue_flow(entity: User, last_step: str, text: str):
        if is_paused(entity):
            return
        flow_actions = advance_flow(last_step, text, FlowContext(is_question=message_has_question))
        route = flow_actions.route

        if route == "contact_chain":
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                INTEREST_TEXT,
                status_for_text(INTEREST_TEXT),
                use_ai=True,
                no_questions=True,
                draft=INTEREST_TEXT,
                step_state=step_state,
                step_name=STEP_INTEREST,
                followup_state=followup_state,
            )
            dating_text = await send_and_update(
                client,
                sheet,
                tz,
                entity,
                DATING_TEXT,
                status_for_text(DATING_TEXT),
                use_ai=True,
                no_questions=True,
                draft=DATING_TEXT,
                step_state=step_state,
                step_name=STEP_DATING,
                followup_state=followup_state,
            )
            if should_send_question(dating_text, CLARIFY_TEXT):
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    CLARIFY_TEXT,
                    status_for_text(CLARIFY_TEXT),
                    use_ai=True,
                    draft=CLARIFY_TEXT,
                    delay_before=QUESTION_GAP_SEC,
                    step_state=step_state,
                    step_name=STEP_CLARIFY,
                    followup_state=followup_state,
                )
            else:
                mark_step_without_send(
                    sheet,
                    tz,
                    entity,
                    status_for_text(CLARIFY_TEXT),
                    step_state,
                    STEP_CLARIFY,
                )
            last_reply_at[entity.id] = time.time()
            return

        if route == "clarify_chain":
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                SHIFTS_TEXT,
                status_for_text(SHIFTS_TEXT),
                use_ai=False,
                draft=SHIFTS_TEXT,
                step_state=step_state,
                step_name=STEP_SHIFTS,
                followup_state=followup_state,
            )
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                SHIFT_QUESTION_TEXT,
                status_for_text(SHIFT_QUESTION_TEXT),
                use_ai=False,
                draft=SHIFT_QUESTION_TEXT,
                delay_before=QUESTION_GAP_SEC,
                step_state=step_state,
                step_name=STEP_SHIFT_QUESTION,
                followup_state=followup_state,
            )
            last_reply_at[entity.id] = time.time()
            return

        if route == "shift_question_chain":
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                FORMAT_TEXT,
                status_for_text(FORMAT_TEXT),
                use_ai=True,
                no_questions=True,
                draft=FORMAT_TEXT,
                step_state=step_state,
                step_name=STEP_FORMAT,
                followup_state=followup_state,
            )
            await handle_format_choice(entity, "both")
            mark_format_stage_ready(entity)
            last_reply_at[entity.id] = time.time()
            return

        if route == "format_choice":
            history = await build_ai_history(client, entity, limit=10)
            choice = await detect_format_choice(history, text)
            result = await handle_format_choice(entity, choice)
            if result in {"video", "mini_course", "both"}:
                return
            if choice == "unknown" or result == "none":
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    entity,
                    FORMAT_QUESTION_TEXT,
                    status_for_text(FORMAT_QUESTION_TEXT),
                    use_ai=False,
                    draft=FORMAT_QUESTION_TEXT,
                    step_state=step_state,
                    step_name=STEP_FORMAT_QUESTION,
                    followup_state=followup_state,
                )
                last_reply_at[entity.id] = time.time()
                return
            return

        if route == "video_followup_chain":
            training_combined_text = f"{TRAINING_TEXT}\n\n{TRAINING_QUESTION_TEXT}"
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                training_combined_text,
                status_for_text(TRAINING_QUESTION_TEXT) or status_for_text(TRAINING_TEXT),
                use_ai=True,
                no_questions=False,
                draft=training_combined_text,
                step_state=step_state,
                step_name=STEP_TRAINING_QUESTION,
                followup_state=followup_state,
            )
            last_reply_at[entity.id] = time.time()
            return

        if route == "training_question_chain":
            await send_and_update(
                client,
                sheet,
                tz,
                entity,
                FORM_TEXT,
                status_for_text(FORM_TEXT),
                use_ai=False,
                draft=FORM_TEXT,
                delay_before=TRAINING_TO_FORM_DELAY_SEC,
                step_state=step_state,
                step_name=STEP_FORM,
                followup_state=followup_state,
            )
            last_reply_at[entity.id] = time.time()
            return

    @client.on(events.NewMessage(outgoing=True))
    async def on_outgoing_message(event):
        if not event.is_private:
            return
        peer_id = event.chat_id
        if not peer_id:
            return
        if is_tracked_message(peer_id, event.id):
            return
        text = (event.raw_text or "").strip()
        text_lower = text.lower()
        def _log_delete_result(task: asyncio.Task):
            try:
                task.result()
            except Exception as err:
                print(f"⚠️ Не вдалося видалити команду: {err}")

        if text_lower in STOP_COMMANDS or text_lower in START_COMMANDS:
            try:
                task = asyncio.create_task(event.delete())
                task.add_done_callback(_log_delete_result)
            except Exception as err:
                print(f"⚠️ Не вдалося запустити видалення: {err}")
            if text_lower in STOP_COMMANDS:
                paused_peers.add(peer_id)
                enabled_peers.discard(peer_id)
            else:
                paused_peers.discard(peer_id)
                enabled_peers.add(peer_id)
                skip_stop_check_once.add(peer_id)
        else:
            manual_step = detect_step_from_text(text)
            if manual_step:
                step_state.set(peer_id, manual_step)
            paused_peers.add(peer_id)
            enabled_peers.discard(peer_id)
        try:
            entity = await event.get_chat()
        except Exception:
            entity = None
        if isinstance(entity, User):
            name = getattr(entity, "first_name", "") or "Unknown"
            username = getattr(entity, "username", "") or ""
            chat_link = build_chat_link_app(entity, entity.id)
            status = "PAUSED" if text_lower in STOP_COMMANDS or text_lower not in START_COMMANDS else "ACTIVE"
            auto_toggle_value = None
            if text_lower in START_COMMANDS:
                auto_toggle_value = True
            elif text_lower in STOP_COMMANDS:
                auto_toggle_value = False
            pause_store.set_status(entity.id, username, name, chat_link, status, updated_by="manual")
            if text_lower in START_COMMANDS:
                recover_source = "v2"
                v2_enrollment.add(entity.id)
                current_v2 = v2_runtime.get(entity.id)
                if not current_v2.flow_step:
                    current_v2 = PeerRuntimeState(peer_id=entity.id, flow_step=STEP_SCREENING_WAIT, auto_mode="ON", paused=False)
                current_v2.auto_mode = "ON"
                current_v2.paused = False
                v2_runtime.set(current_v2)
                print(f"START1_RECOVER peer={entity.id} source=v2 step={current_v2.flow_step}")
            queue_today_upsert(
                peer_id=entity.id,
                name=name,
                username=username,
                chat_link=chat_link,
                auto_reply_enabled=auto_toggle_value,
                tech_step=(v2_runtime.get(entity.id).flow_step if text_lower in START_COMMANDS else None),
                sender_role="operator",
                dialog_mode=("ON" if text_lower in START_COMMANDS else "OFF"),
                step_snapshot=(v2_runtime.get(entity.id).flow_step if text_lower in START_COMMANDS else ""),
                full_text=text,
                event_type_override=(
                    f"START1_RECOVER ({recover_source})" if text_lower in START_COMMANDS else None
                ),
                status=(None if text_lower in START_COMMANDS else MANUAL_OFF_STATUS),
            )
        else:
            status = "PAUSED" if text_lower in STOP_COMMANDS or text_lower not in START_COMMANDS else "ACTIVE"
            pause_store.set_status(peer_id, None, None, None, status, updated_by="manual")
    @client.on(events.NewMessage(chats=leads_group))
    async def on_lead_message(event):
        text = event.raw_text or ""
        try:
            group_data = parse_group_message(text)
            group_status = status_for_text(CONTACT_TEXT)
            if not enqueue_sheet_event("group_leads_upsert", {"data": group_data, "status": group_status}):
                try:
                    group_leads_sheet.upsert(tz, group_data, group_status)
                    print("AUTO_REPLY_CONTINUE despite_sheet_error peer=group_lead")
                except Exception as err:
                    print(f"⚠️ SHEETS_DIRECT_WRITE_FAIL group_leads: {type(err).__name__}: {err}")
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
        if is_paused(entity):
            print(f"⏭️ Призупинено для користувача: {entity.id}")
            return

        enabled_peers.add(entity.id)
        v2_enrollment.add(entity.id)
        v2_state = v2_runtime.get(entity.id)
        v2_state.flow_step = STEP_SCREENING_WAIT
        v2_state.auto_mode = "ON"
        v2_state.paused = False
        v2_runtime.set(v2_state)
        await send_v2_message(entity, SCREENING_INTRO_TEXT, STEP_SCREENING_WAIT, status="👋 Привітання")
        print(f"✅ V2 onboarding message sent: {entity.id}")

    if traffic_group:
        async def process_traffic_registration(chat_id: int, message_id: int):
            key = (chat_id, message_id)
            try:
                if REGISTRATION_PARSE_DELAY_SEC > 0:
                    await asyncio.sleep(REGISTRATION_PARSE_DELAY_SEC)
                msg = await client.get_messages(chat_id, ids=message_id)
                if not msg:
                    return
                if not is_media_registration_message(msg):
                    return
                text = msg.message or ""
                parsed = parse_registration_message(text)
                drive_link = ""
                if registration_drive:
                    try:
                        drive_link = await upload_media_to_drive(msg, chat_id, message_id, registration_drive)
                    except Exception as err:
                        print(f"⚠️ Drive upload error peer={chat_id} msg={message_id}: {type(err).__name__}: {err}")
                payload = {
                    **parsed,
                    "document_drive_link": drive_link,
                    "message_link": build_registration_message_link(chat_id, message_id),
                    "source_group": (getattr(getattr(msg, "chat", None), "title", None) or TRAFFIC_GROUP_TITLE),
                    "source_message_id": str(message_id),
                }
                if registration_sheet:
                    if not enqueue_sheet_event("registration_upsert", payload):
                        try:
                            registration_sheet.upsert(tz, payload)
                            print(f"AUTO_REPLY_CONTINUE despite_sheet_error peer={chat_id}")
                        except Exception as err:
                            print(f"⚠️ SHEETS_DIRECT_WRITE_FAIL registration peer={chat_id}: {type(err).__name__}: {err}")
                else:
                    print("⚠️ RegistrationSheet недоступний: рядок не записано")
            except Exception as err:
                print(f"⚠️ Registration ingest error peer={chat_id} msg={message_id}: {type(err).__name__}: {err}")
            finally:
                pending_registration_tasks.pop(key, None)

        async def schedule_traffic_registration(event):
            msg = event.message
            if not msg:
                return
            if not is_media_registration_message(msg):
                return
            chat_id = event.chat_id
            message_id = event.id
            key = (chat_id, message_id)
            existing = pending_registration_tasks.get(key)
            if existing and not existing.done():
                existing.cancel()
            pending_registration_tasks[key] = asyncio.create_task(process_traffic_registration(chat_id, message_id))

        @client.on(events.NewMessage(chats=traffic_group))
        async def on_traffic_registration_message(event):
            await schedule_traffic_registration(event)

        @client.on(events.MessageEdited(chats=traffic_group))
        async def on_traffic_registration_edit(event):
            await schedule_traffic_registration(event)

    async def apply_sheet_event(event):
        payload = event.payload or {}
        if event.event_type == "today_upsert":
            await asyncio.to_thread(sheet.upsert, tz=tz, **payload)
            return
        if event.event_type == "group_leads_upsert":
            group_data = payload.get("data") or {}
            await asyncio.to_thread(
                group_leads_sheet.upsert,
                tz,
                group_data,
                payload.get("status"),
            )
            try:
                updated = await asyncio.to_thread(sheet.refresh_today_from_group_lead, tz, group_data)
                if updated:
                    print(f"SHEETS_GROUP_REFRESH updated={updated} tg={group_data.get('tg', '')}")
            except Exception as err:
                print(f"⚠️ SHEETS_GROUP_REFRESH_FAIL: {type(err).__name__}: {err}")
            return
        if event.event_type == "registration_upsert":
            if registration_sheet:
                await asyncio.to_thread(registration_sheet.upsert, tz, payload)
            return
        if event.event_type == "faq_question_log":
            if faq_questions_sheet:
                await asyncio.to_thread(faq_questions_sheet.upsert_question, payload)
                count = int(payload.get("count", 1) or 1)
                if count >= 3 and faq_suggestions_sheet:
                    suggestion = {
                        "question_cluster": payload.get("cluster_key", ""),
                        "suggested_answer": payload.get("answer_preview", ""),
                        "source_examples": payload.get("question_raw", ""),
                        "review_status": "new",
                        "reviewed_at": "",
                        "reviewed_by": "",
                    }
                    await asyncio.to_thread(faq_suggestions_sheet.append_if_missing, suggestion)
            return
        raise ValueError(f"Unknown sheet event type: {event.event_type}")

    def extract_status_code(err: Exception) -> Optional[int]:
        if isinstance(err, APIError):
            resp = getattr(err, "response", None)
            code = getattr(resp, "status_code", None)
            try:
                return int(code)
            except (TypeError, ValueError):
                return None
        return None

    async def sheet_flush_loop():
        nonlocal last_queue_log_at
        if not sheets_queue:
            return
        while not stop_event.is_set():
            try:
                now_ts = time.time()
                batch = sheets_queue.fetch_batch(SHEETS_QUEUE_BATCH_SIZE, now_ts)
                for event in batch:
                    attempts = int(event.attempts) + 1
                    try:
                        await apply_sheet_event(event)
                        sheets_queue.mark_done(event.id)
                        print(f"SHEETS_QUEUE_FLUSH ok id={event.id} type={event.event_type} attempts={attempts}")
                    except Exception as err:
                        status_code = extract_status_code(err)
                        hard_error = status_code is not None and (400 <= status_code < 500) and status_code != 429
                        backoff = calculate_backoff_sec(attempts, hard_error=hard_error)
                        sheets_queue.mark_retry(event.id, attempts, backoff, f"{type(err).__name__}: {err}")
                        print(
                            f"SHEETS_QUEUE_FLUSH fail id={event.id} type={event.event_type} attempts={attempts} "
                            f"backoff={backoff:.1f}s err={type(err).__name__}: {err}"
                        )
                if (now_ts - last_queue_log_at) >= max(5, SHEETS_QUEUE_LOG_SEC):
                    stats = sheets_queue.stats()
                    pending = int(stats.get("pending") or 0)
                    oldest = stats.get("oldest_age_sec")
                    oldest_fmt = f"{oldest:.1f}" if isinstance(oldest, (int, float)) else "0"
                    print(f"SHEETS_QUEUE_BACKLOG pending={pending} oldest_sec={oldest_fmt}")
                    last_queue_log_at = now_ts
            except Exception as err:
                print(f"⚠️ SHEETS_QUEUE_LOOP_ERROR: {type(err).__name__}: {err}")
            await asyncio.sleep(max(0.2, SHEETS_QUEUE_FLUSH_SEC))

    @client.on(events.NewMessage(incoming=True))
    async def on_private_message(event):
        if not event.is_private:
            return
        sender = await event.get_sender()
        if not isinstance(sender, User) or sender.bot:
            return
        peer_id = sender.id
        text = event.raw_text or ""
        name = getattr(sender, "first_name", "") or "Unknown"
        username = getattr(sender, "username", "") or ""
        chat_link = build_chat_link_app(sender, sender.id)
        v2_step_snapshot = v2_runtime.get(peer_id).flow_step if FLOW_V2_ENABLED else ""
        current_step_snapshot = v2_step_snapshot or step_state.get(peer_id) or ""
        pause_status = pause_store.get_status(peer_id, username)
        if pause_status == "PAUSED" or peer_id in paused_peers:
            incoming_mode = "OFF"
        elif pause_status == "ACTIVE":
            incoming_mode = "ON"
        else:
            incoming_mode = "ON" if peer_id in enabled_peers else "OFF"

        queue_today_upsert(
            peer_id=sender.id,
            name=name,
            username=username,
            chat_link=chat_link,
            last_in=text[:200],
            status=(MANUAL_OFF_STATUS if incoming_mode == "OFF" else None),
            sender_role="lead",
            dialog_mode=incoming_mode,
            step_snapshot=current_step_snapshot,
            full_text=text,
        )

        if is_test_restart(sender, text):
            paused_peers.discard(peer_id)
            enabled_peers.add(peer_id)
            clear_qa_gate(peer_id)
            step_state.delete(peer_id)
            v2_runtime.delete(peer_id)
            last_reply_at.pop(peer_id, None)
            last_incoming_at.pop(peer_id, None)
            pending_question_resume.pop(peer_id, None)
            processing_peers.discard(peer_id)
            pause_store.set_status(sender.id, username, name, chat_link, "ACTIVE", updated_by="test")
            v2_enrollment.add(peer_id)
            v2_state = PeerRuntimeState(
                peer_id=peer_id,
                flow_step=STEP_SCREENING_WAIT,
                auto_mode="ON",
                paused=False,
                screening_started_at=time.time(),
            )
            v2_runtime.set(v2_state)
            await send_v2_message(sender, SCREENING_INTRO_TEXT, STEP_SCREENING_WAIT, status="👋 Привітання")
            print(f"✅ START8 switched to V2 flow peer={peer_id}")
            return
        is_test = is_test_user(sender)
        if is_paused(sender):
            if not is_test:
                print(f"⚠️ Filtered paused: {peer_id}")
                return
            print(f"✅ Test user bypassed paused: {peer_id}")
        if peer_id not in enabled_peers:
            if pause_status == "ACTIVE":
                enabled_peers.add(peer_id)
                print(f"ℹ️ Restored enabled from pause-state: {peer_id}")
            else:
                if not is_test:
                    print(f"⚠️ Filtered not enabled: {peer_id}")
                    return
                print(f"✅ Test user bypassed enabled check: {peer_id}")
        if peer_id in processing_peers:
            if FLOW_V2_ENABLED:
                bucket = buffered_incoming.setdefault(peer_id, deque())
                bucket.append(text)
                print(f"ℹ️ Buffered incoming peer={peer_id} size={len(bucket)}")
                return
            if not is_test:
                print(f"⚠️ Filtered already processing: {peer_id}")
                return
            print(f"✅ Test user bypassed processing check: {peer_id}")
        now_ts = time.time()
        last_ts = last_reply_at.get(peer_id)
        if last_ts and now_ts - last_ts < REPLY_DEBOUNCE_SEC:
            if not is_test:
                print(f"⚠️ Filtered debounce: {peer_id}")
                return
            print(f"✅ Test user bypassed debounce: {peer_id}")
        processing_peers.add(peer_id)

        try:
            last_incoming_at[peer_id] = time.time()
            pending_question_resume.pop(peer_id, None)
            followup_state.clear(peer_id)
            queue_today_upsert(
                peer_id=sender.id,
                name=name,
                username=username,
                chat_link=chat_link,
                followup_stage="",
                followup_next_at="",
                followup_last_sent_at="",
            )

            if FLOW_V2_ENABLED:
                handled_v2 = await process_v2_turn(sender, text)
                if handled_v2:
                    last_reply_at[peer_id] = time.time()
                    return
                return

            history = await build_ai_history(client, sender, limit=10)
            last_step_hint = step_state.get(peer_id)
            intent = await classify_candidate_intent(history, text, last_step_hint)
            gate = qa_gate_state.get(peer_id, {})
            gate_active = bool(gate.get("qa_gate_active"))
            gate_step = (gate.get("qa_gate_step") or last_step_hint or "").strip() or None

            if last_step_hint == STEP_CLARIFY and intent == Intent.STOP and is_clarify_uncertain_reply(text):
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    sender,
                    CLARIFY_NEGATIVE_FOLLOWUP_TEXT,
                    status_for_text(CLARIFY_TEXT),
                    use_ai=True,
                    draft=CLARIFY_NEGATIVE_FOLLOWUP_TEXT,
                    step_state=step_state,
                    step_name=STEP_CLARIFY,
                    followup_state=followup_state,
                )
                last_reply_at[peer_id] = time.time()
                print(f"ℹ️ Clarify override peer={peer_id}: short negative treated as request to clarify")
                return

            if gate_active:
                if intent == Intent.QUESTION:
                    sent = await send_ai_detailed_answer(sender, history_override=history, step_name=gate_step)
                    if not sent:
                        await send_and_update(
                            client,
                            sheet,
                            tz,
                            sender,
                            "Дякую за запитання. Уточню деталі коротко нижче 👇",
                            "знак питання",
                            use_ai=False,
                            delay_before=QUESTION_RESPONSE_DELAY_SEC,
                            step_state=step_state,
                            step_name=gate_step,
                            followup_state=followup_state,
                        )
                    await send_qa_gate_confirm(sender, gate_step)
                    set_qa_gate(peer_id, gate_step)
                    last_reply_at[peer_id] = time.time()
                    return
                if intent == Intent.ACK_CONTINUE:
                    clear_qa_gate(peer_id)
                    resolved_step = gate_step or await get_last_step(client, sender, step_state)
                    if not resolved_step:
                        resolved_step = STEP_SHIFT_QUESTION
                        step_state.set(peer_id, resolved_step)
                    if CONTINUE_DELAY_SEC > 0:
                        await asyncio.sleep(CONTINUE_DELAY_SEC)
                        if is_paused(sender):
                            return
                    await continue_flow(sender, resolved_step, text)
                    return
                if intent == Intent.OTHER:
                    await send_qa_gate_confirm(sender, gate_step)
                    set_qa_gate(peer_id, gate_step)
                    last_reply_at[peer_id] = time.time()
                    return

            skip_stop_for_this_message = peer_id in skip_stop_check_once
            if skip_stop_for_this_message:
                skip_stop_check_once.discard(peer_id)
                print(f"START1_RECOVER peer={peer_id} source=skip_stop step={step_state.get(peer_id) or ''}")
            if (not skip_stop_for_this_message) and intent == Intent.STOP:
                await send_and_update(
                    client,
                    sheet,
                    tz,
                    sender,
                    STOP_REPLY_TEXT,
                    AUTO_STOP_STATUS,
                    use_ai=True,
                    draft=STOP_REPLY_TEXT,
                    auto_reply_enabled=False,
                    step_state=step_state,
                    followup_state=followup_state,
                    schedule_followup=False,
                )
                paused_peers.add(peer_id)
                enabled_peers.discard(peer_id)
                clear_qa_gate(peer_id)
                pause_store.set_status(
                    sender.id,
                    username,
                    name,
                    chat_link,
                    "PAUSED",
                    updated_by="auto_stop",
                )
                return

            last_step = await get_last_step(client, sender, step_state)
            if not last_step:
                reconciled_step, reconcile_source = await reconcile_dialog_step(sender, use_cache=False)
                if reconciled_step:
                    last_step = reconciled_step
                    print(f"START1_RECOVER peer={peer_id} source={reconcile_source} step={last_step}")
                    queue_today_upsert(
                        peer_id=sender.id,
                        name=name,
                        username=username,
                        chat_link=chat_link,
                        tech_step=last_step,
                    )
                else:
                    fallback_step = STEP_SHIFT_QUESTION
                    print(f"MISSING_STEP_FALLBACK peer={peer_id} chosen={fallback_step}")
                    step_state.set(peer_id, fallback_step)
                    queue_today_upsert(
                        peer_id=sender.id,
                        name=name,
                        username=username,
                        chat_link=chat_link,
                        tech_step=fallback_step,
                    )
                    await send_and_update(
                        client,
                        sheet,
                        tz,
                        sender,
                        MISSING_STEP_RECOVERY_TEXT,
                        status_for_text(SHIFT_QUESTION_TEXT),
                        use_ai=False,
                        draft=MISSING_STEP_RECOVERY_TEXT,
                        step_state=step_state,
                        step_name=fallback_step,
                        followup_state=followup_state,
                    )
                    last_reply_at[peer_id] = time.time()
                    return

            if last_step == STEP_FORM:
                if message_has_question(text):
                    await send_and_update(
                        client,
                        sheet,
                        tz,
                        sender,
                        FORM_LOCK_REPLY_TEXT,
                        status_for_text(FORM_TEXT),
                        use_ai=False,
                        draft=FORM_LOCK_REPLY_TEXT,
                        step_state=step_state,
                        step_name=STEP_FORM,
                        followup_state=followup_state,
                    )
                    last_reply_at[peer_id] = time.time()
                return

            if intent == Intent.QUESTION:
                sent = await send_ai_detailed_answer(sender, history_override=history, step_name=last_step)
                if not sent:
                    print(f"⚠️ AI question-response fallback peer={peer_id}: no detailed AI answer")
                    await send_and_update(
                        client,
                        sheet,
                        tz,
                        sender,
                        "Дякую за запитання. Уточню, будь ласка, що саме цікавить найбільше?",
                        "знак питання",
                        use_ai=False,
                        delay_before=QUESTION_RESPONSE_DELAY_SEC,
                        step_name=last_step,
                        step_state=step_state,
                        followup_state=followup_state,
                    )
                await send_qa_gate_confirm(sender, last_step)
                set_qa_gate(peer_id, last_step)
                last_reply_at[peer_id] = time.time()
                return

            if intent == Intent.ACK_CONTINUE and is_short_neutral_ack(text):
                print(f"ℹ️ Intent ack_continue peer={peer_id} step={last_step or last_step_hint or ''} text='{text[:40]}'")

            if last_step in {STEP_FORMAT_QUESTION, STEP_VIDEO_FOLLOWUP, STEP_TRAINING, STEP_TRAINING_QUESTION}:
                format_choice = await detect_format_choice(history, text)
                if format_choice in {"video", "mini_course", "both"}:
                    handled = await handle_format_choice(sender, format_choice)
                    if handled in {"video", "mini_course", "both"}:
                        return

            if CONTINUE_DELAY_SEC > 0:
                await asyncio.sleep(CONTINUE_DELAY_SEC)
                if is_paused(sender):
                    return
            await continue_flow(sender, last_step, text)
        finally:
            processing_peers.discard(peer_id)
            if FLOW_V2_ENABLED:
                while True:
                    queued = buffered_incoming.get(peer_id)
                    if not queued:
                        break
                    next_text = queued.popleft()
                    if not queued:
                        buffered_incoming.pop(peer_id, None)
                    processing_peers.add(peer_id)
                    try:
                        await process_v2_turn(sender, next_text)
                        last_reply_at[peer_id] = time.time()
                    except Exception as err:
                        print(f"⚠️ Buffered V2 process error peer={peer_id}: {type(err).__name__}: {err}")
                    finally:
                        processing_peers.discard(peer_id)

    print("🤖 Автовідповідач запущено")
    async def followup_loop():
        while not stop_event.is_set():
            try:
                now = datetime.now(tz)
                if not FLOW_V2_ENABLED:
                    for peer_id, state in list(qa_gate_state.items()):
                        if not state.get("qa_gate_active"):
                            continue
                        opened_at = float(state.get("qa_gate_opened_at", 0) or 0)
                        reminder_sent = bool(state.get("qa_gate_reminder_sent"))
                        if reminder_sent:
                            continue
                        if now.timestamp() < opened_at + QA_GATE_REMINDER_DELAY_SEC:
                            continue
                        if peer_id not in enabled_peers:
                            continue
                        try:
                            entity = await client.get_entity(peer_id)
                        except Exception:
                            continue
                        if is_paused(entity):
                            continue
                        gate_step = (state.get("qa_gate_step") or "").strip() or None
                        await send_and_update(
                            client,
                            sheet,
                            tz,
                            entity,
                            QA_GATE_REMINDER_TEXT,
                            "знак питання",
                            use_ai=False,
                            schedule_followup=False,
                            step_state=step_state,
                            step_name=gate_step,
                            followup_state=followup_state,
                        )
                        state["qa_gate_reminder_sent"] = True
                        qa_gate_state[peer_id] = state

                if FLOW_V2_ENABLED:
                    for peer_id in list(v2_enrollment.data):
                        v2s = v2_runtime.get(peer_id)
                        if v2s.paused:
                            continue
                        try:
                            entity = await client.get_entity(peer_id)
                        except Exception:
                            continue
                        if v2s.qa_gate_active and not v2s.qa_gate_reminder_sent:
                            if time.time() >= float(v2s.qa_gate_opened_at or 0) + QA_GATE_REMINDER_DELAY_SEC:
                                await send_v2_message(entity, V2_GATE_REMINDER_TEXT, v2s.qa_gate_step or v2s.flow_step, status="знак питання")
                                v2s.qa_gate_reminder_sent = True
                                v2_runtime.set(v2s)
                        if v2s.flow_step == STEP_SCREENING_WAIT:
                            started = float(v2s.screening_started_at or 0)
                            answers = v2s.screening_answers or []
                            if started > 0 and len(answers) < 3:
                                if (time.time() - started) >= SCREENING_WAIT_SEC:
                                    await send_v2_message(entity, COMPANY_INTRO_TIMEOUT_TEXT, STEP_COMPANY_INTRO, status="🏢 Знайомство з компанією")
                                    v2s.flow_step = STEP_COMPANY_INTRO
                                    v2s.screening_started_at = 0.0
                                    v2s.screening_last_at = 0.0
                                    v2_runtime.set(v2s)
                                    continue
                        if v2s.flow_step == STEP_VOICE_WAIT and v2s.voice_stage in {VOICE_SENT, VOICE_FALLBACK_SENT}:
                            elapsed = time.time() - float(v2s.voice_sent_at or 0)
                            if v2s.voice_stage == VOICE_SENT and elapsed >= VOICE_FALLBACK_DELAY_SEC:
                                await send_v2_message(entity, VOICE_FALLBACK_TEXT, STEP_VOICE_WAIT, status="🎧 Голосове")
                                v2s.voice_stage = VOICE_FALLBACK_SENT
                                v2_runtime.set(v2s)
                            elif elapsed >= (VOICE_FALLBACK_DELAY_SEC + VOICE_AUTO_CONTINUE_DELAY_SEC):
                                await send_v2_message(entity, SCHEDULE_SHIFT_TEXT, STEP_SCHEDULE_SHIFT_WAIT, status="🕒 Графік")
                                v2s.voice_stage = VOICE_AUTO_ADVANCED
                                v2s.flow_step = STEP_SCHEDULE_SHIFT_WAIT
                                v2s.shift_prompted_at = time.time()
                                v2_runtime.set(v2s)
                        if v2s.flow_step == STEP_SCHEDULE_SHIFT_WAIT and float(v2s.shift_prompted_at or 0) > 0:
                            if (time.time() - float(v2s.shift_prompted_at or 0)) >= SCHEDULE_SHIFT_WAIT_SEC:
                                await send_v2_message(entity, SCHEDULE_DETAILS_TEXT, STEP_SCHEDULE_BLOCK, status="🕒 Графік")
                                await send_v2_message(entity, SCHEDULE_CONFIRM_TEXT, STEP_SCHEDULE_CONFIRM, status="🕒 Графік")
                                v2s.flow_step = STEP_SCHEDULE_CONFIRM
                                v2s.shift_prompted_at = 0.0
                                v2_runtime.set(v2s)

                if not FLOW_V2_ENABLED:
                    for key, state in list(followup_state.data.items()):
                        try:
                            peer_id = int(key)
                        except ValueError:
                            continue
                        if str(peer_id) == TEST_USER_ID:
                            continue
                        next_at = state.get("next_at")
                        stage = state.get("stage")
                        if next_at is None or stage is None:
                            continue
                        if now.timestamp() < float(next_at):
                            continue
                        delay_sec, text = FOLLOWUP_TEMPLATES[int(stage)]
                        if not within_followup_window(now):
                            adjusted = adjust_to_followup_window(now)
                            state["next_at"] = adjusted.timestamp()
                            followup_state.data[key] = state
                            followup_state._save()
                            continue
                        try:
                            entity = await client.get_entity(peer_id)
                        except Exception:
                            continue
                        await send_and_update(
                            client,
                            sheet,
                            tz,
                            entity,
                            text,
                            status_for_text(text),
                            use_ai=False,
                            schedule_followup=False,
                            followup_state=followup_state,
                        )
                        next_stage, next_dt = followup_state.mark_sent_and_advance(peer_id, tz)
                        stage_value = str(next_stage + 1) if next_stage is not None else ""
                        next_value = next_dt.isoformat(timespec="seconds") if next_dt else ""
                        queue_today_upsert(
                            peer_id=peer_id,
                            name=getattr(entity, "first_name", "") or "Unknown",
                            username=getattr(entity, "username", "") or "",
                            chat_link=build_chat_link_app(entity, peer_id),
                            followup_stage=stage_value,
                            followup_next_at=next_value,
                            followup_last_sent_at=datetime.now(tz).isoformat(timespec="seconds"),
                        )

                    for peer_id, state in list(pending_question_resume.items()):
                        if now.timestamp() < float(state.get("due_at", 0)):
                            continue
                        if peer_id in processing_peers:
                            continue
                        if peer_id not in enabled_peers:
                            pending_question_resume.pop(peer_id, None)
                            continue
                        baseline_in = float(state.get("last_incoming_at", 0))
                        latest_in = float(last_incoming_at.get(peer_id, 0))
                        if latest_in > baseline_in:
                            pending_question_resume.pop(peer_id, None)
                            continue
                        try:
                            entity = await client.get_entity(peer_id)
                        except Exception:
                            continue
                        if is_paused(entity):
                            pending_question_resume.pop(peer_id, None)
                            continue
                        step_name = state.get("step")
                        if not step_name:
                            pending_question_resume.pop(peer_id, None)
                            continue
                        processing_peers.add(peer_id)
                        try:
                            await continue_flow(entity, step_name, "")
                        except Exception as err:
                            print(f"⚠️ Question-resume error: {err}")
                        finally:
                            processing_peers.discard(peer_id)
                            pending_question_resume.pop(peer_id, None)
            except Exception as err:
                print(f"⚠️ Followup loop error: {err}")
            await asyncio.sleep(FOLLOWUP_CHECK_SEC)

    sheets_task = None
    followup_task = None
    try:
        if sheets_queue:
            sheets_task = asyncio.create_task(sheet_flush_loop())
        followup_task = asyncio.create_task(followup_loop())
        while not stop_event.is_set():
            await asyncio.sleep(0.5)
    finally:
        try:
            if sheets_task:
                sheets_task.cancel()
            if followup_task:
                followup_task.cancel()
        except Exception:
            pass
        await client.disconnect()
        release_lock(SESSION_LOCK)
        release_lock(AUTO_REPLY_LOCK)


if __name__ == "__main__":
    asyncio.run(main())
