from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo


@dataclass
class QuestionLog:
    created_at: str
    peer_id: int
    step: str
    question_raw: str
    question_norm: str
    cluster_key: str
    count: int
    last_seen_at: str
    answer_preview: str
    resolved_status: str


def build_question_log(tz: ZoneInfo, peer_id: int, step: str, question_raw: str, question_norm: str, cluster_key: str, answer_preview: str) -> QuestionLog:
    now = datetime.now(tz).isoformat(timespec="seconds")
    return QuestionLog(
        created_at=now,
        peer_id=peer_id,
        step=step or "",
        question_raw=question_raw,
        question_norm=question_norm,
        cluster_key=cluster_key,
        count=1,
        last_seen_at=now,
        answer_preview=(answer_preview or "")[:500],
        resolved_status="new",
    )
