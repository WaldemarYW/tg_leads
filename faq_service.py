from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Callable, List, Optional

VOICE_TEXT_FALLBACK_BLOCK_1 = (
    "Коротко поясню умови текстом.\n\n"
    "Furioza Company понад 10 років працює з міжнародною дейтінговою платформою. "
    "Робота акаунт-менеджера повністю віддалена: потрібно вести текстове спілкування "
    "від імені клієнтки з користувачами сайту, без дзвінків і продажів. "
    "Працювати можна лише з ПК або ноутбука.\n\n"
    "Перед стартом є навчання з адміністратором, яке займає кілька годин, а після нього "
    "починається двотижневе стажування з супроводом."
)

VOICE_TEXT_FALLBACK_BLOCK_2 = (
    "По оплаті: у перший місяць є гарантована ставка 300 $ + 48% базових + 20% бонусних, "
    "а подарунки рахуються 20-27%. З другого місяця діє 40% базових із можливістю зростання "
    "до 45-47% за KPI + бонуси.\n\n"
    "Є денна зміна 14:00-23:00 або нічна 23:00-08:00, 5-7 плаваючих вихідних на місяць, "
    "аванс 20-30% у будь-який день і повний розрахунок з 8 по 15 число."
)

SALES_SCRIPT_PATH = "sales-script.md"


@dataclass
class AnswerResult:
    text: str
    source: str
    cluster_key: str
    question_norm: str


def normalize_question(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return re.sub(r"[^\w\sа-яіїєґ]", "", text, flags=re.IGNORECASE).strip()


def build_cluster_key(question_norm: str) -> str:
    return (question_norm or "")[:160]


def load_faq_corpus() -> str:
    chunks: List[str] = []
    for path in ("faq-for-ai.txt", "telegraph-faq.txt"):
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    chunks.append(f.read().strip())
            except OSError:
                continue
    return "\n\n".join([c for c in chunks if c])


def load_sales_script_context() -> str:
    if not os.path.exists(SALES_SCRIPT_PATH):
        return ""
    try:
        with open(SALES_SCRIPT_PATH, "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return ""


def _compact_text_block(text: str) -> str:
    lines = [line.strip() for line in (text or "").splitlines()]
    compact = "\n".join([line for line in lines if line])
    return compact.strip()


def _extract_markdown_section(markdown: str, heading: str) -> str:
    if not markdown:
        return ""
    pattern = re.compile(
        rf"^##\s+{re.escape(heading)}\s*$\n(.*?)(?=^##\s+|\Z)",
        flags=re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(markdown)
    if not match:
        return ""
    return _compact_text_block(match.group(1))

def build_voice_text_recap_blocks() -> List[str]:
    sales_script = load_sales_script_context()
    voice_block_1 = _extract_markdown_section(sales_script, "Voice Recap 1")
    voice_block_2 = _extract_markdown_section(sales_script, "Voice Recap 2")
    if voice_block_1 and voice_block_2:
        return [voice_block_1, voice_block_2]

    faq_text = ""
    path = "faq-for-ai.txt"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                faq_text = f.read()
        except OSError:
            faq_text = ""

    if faq_text:
        paragraphs = [p.strip() for p in faq_text.split("\n\n") if p.strip()]
        if len(paragraphs) >= 2:
            split_at = max(1, len(paragraphs) // 2)
            first = _compact_text_block("\n\n".join(paragraphs[:split_at]))
            second = _compact_text_block("\n\n".join(paragraphs[split_at:]))
            if first and second:
                return [first, second]

    return [VOICE_TEXT_FALLBACK_BLOCK_1, VOICE_TEXT_FALLBACK_BLOCK_2]


async def answer_from_faq(
    question: str,
    step: str,
    history: list,
    dialog_suggest: Callable,
    mode: str = "detailed",
) -> Optional[AnswerResult]:
    question_norm = normalize_question(question)
    cluster_key = build_cluster_key(question_norm)
    faq_corpus = load_faq_corpus()
    sales_script = load_sales_script_context()
    length_rule = "до 6-8 коротких речень" if mode == "detailed" else "до 3 коротких речень"
    draft = (
        "Відповідай лише українською. "
        "Звертайтесь до кандидата виключно на «Ви». "
        "Відповідай тільки в межах фактів з контексту FAQ та sales script нижче. "
        "Основне завдання: дати пряму відповідь на запитання кандидата і, якщо доречно, мʼяко повернути його до наступного етапу. "
        f"Формат відповіді: {length_rule}. "
        "Якщо питання поза FAQ, чесно скажи що уточниш деталі.\n\n"
        f"Поточний крок сценарію: {step}\n"
        f"Питання кандидата: {question}\n\n"
        f"Sales script:\n{sales_script[:8000]}\n\n"
        f"FAQ контекст:\n{faq_corpus[:12000]}"
    )
    text = await dialog_suggest(history, draft, no_questions=True)
    if not text:
        return None
    return AnswerResult(text=text.strip(), source="faq-merged", cluster_key=cluster_key, question_norm=question_norm)
