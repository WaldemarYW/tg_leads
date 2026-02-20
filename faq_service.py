from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Callable, List, Optional


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
