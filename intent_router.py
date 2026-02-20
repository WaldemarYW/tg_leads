from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from auto_reply_classifiers import classify_local_intent, Intent


@dataclass
class IntentResult:
    intent: str
    confidence: float


def detect_intent(text: str, last_step: Optional[str] = None) -> IntentResult:
    local = classify_local_intent(text, last_step=last_step)
    if local == Intent.QUESTION:
        return IntentResult(intent="question", confidence=0.9)
    if local == Intent.ACK_CONTINUE:
        return IntentResult(intent="ack_continue", confidence=0.8)
    if local == Intent.STOP:
        return IntentResult(intent="stop", confidence=0.9)
    return IntentResult(intent="other", confidence=0.5)
