from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Callable, List, Optional

VOICE_TEXT_FALLBACK_BLOCK_1 = (
    "Коротко поясню умови текстом.\n\n"
    "Сайт працює так: чоловіки оплачують кожну хвилину спілкування, листи, фото та відео. "
    "Робота чат-менеджера — вести цікаву комунікацію, створювати інвайти та листи, "
    "щоб підтримувати активність у чаті.\n\n"
    "Робота віддалена, 8 годин за ПК/ноутбуком, без дзвінків і відеозвʼязку. "
    "На старті є стажування з тімлідом: він допомагає адаптуватися та веде по процесу. "
    "Після успішного завершення стажування відкривається доступ до першої виплати."
)

VOICE_TEXT_FALLBACK_BLOCK_2 = (
    "Щодо оплати: дохід формується з відсотка від балансу анкети та активності в чаті.\n\n"
    "У перший місяць базовий відсоток становить 48%, окремо враховуються реальні подарунки. "
    "Є чітка тарифікація дій (чат, листи, фото, відео), тому дохід напряму залежить від "
    "якості комунікації та регулярності роботи.\n\n"
    "Також на платформі є вбудований перекладач, оскільки основна аудиторія — користувачі з Америки."
)


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


def _compact_text_block(text: str) -> str:
    lines = [line.strip() for line in (text or "").splitlines()]
    compact = "\n".join([line for line in lines if line])
    return compact.strip()


def build_voice_text_recap_blocks() -> List[str]:
    faq_text = ""
    path = "faq-for-ai.txt"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                faq_text = f.read()
        except OSError:
            faq_text = ""

    marker = "Як складається баланс:"
    if faq_text and marker in faq_text:
        before, after = faq_text.split(marker, 1)
        first = _compact_text_block(before)
        second = _compact_text_block(f"{marker}\n{after}")
        if first and second:
            return [first, second]

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
    length_rule = "до 6-8 коротких речень" if mode == "detailed" else "до 3 коротких речень"
    draft = (
        "Відповідай лише українською. "
        "Звертайтесь до кандидата виключно на «Ви». "
        "Відповідай тільки в межах фактів з контексту FAQ нижче. "
        f"Формат відповіді: {length_rule}. "
        "Якщо питання поза FAQ, чесно скажи що уточниш деталі.\n\n"
        f"Поточний крок сценарію: {step}\n"
        f"Питання кандидата: {question}\n\n"
        f"FAQ контекст:\n{faq_corpus[:12000]}"
    )
    text = await dialog_suggest(history, draft, no_questions=True)
    if not text:
        return None
    return AnswerResult(text=text.strip(), source="faq-merged", cluster_key=cluster_key, question_norm=question_norm)
