from dataclasses import dataclass, field
from typing import Callable, List, Optional


STEP_CONTACT = "contact"
STEP_INTEREST = "interest"
STEP_DATING = "dating"
STEP_DUTIES = "duties"
STEP_CLARIFY = "clarify"
STEP_SHIFTS = "shifts"
STEP_SHIFT_QUESTION = "shift_question"
STEP_FORMAT = "format"
STEP_FORMAT_QUESTION = "format_question"
STEP_VIDEO_FOLLOWUP = "video_followup"
STEP_TRAINING = "training"
STEP_TRAINING_QUESTION = "training_question"
STEP_FORM = "form"

STEP_ORDER = {
    STEP_CONTACT: 0,
    STEP_INTEREST: 1,
    STEP_DATING: 2,
    STEP_DUTIES: 3,
    STEP_CLARIFY: 4,
    STEP_SHIFTS: 5,
    STEP_SHIFT_QUESTION: 6,
    STEP_FORMAT: 7,
    STEP_FORMAT_QUESTION: 8,
    STEP_VIDEO_FOLLOWUP: 9,
    STEP_TRAINING: 10,
    STEP_TRAINING_QUESTION: 11,
    STEP_FORM: 12,
}


@dataclass
class FlowContext:
    is_question: Callable[[str], bool]


@dataclass
class FlowActions:
    route: str
    operations: List[str] = field(default_factory=list)


@dataclass
class SendResult:
    success: bool
    text_used: str
    error: Optional[str] = None


@dataclass
class PeerStateSnapshot:
    peer_id: int
    step: Optional[str]
    source: str


async def send_message_with_fallback(
    text: str,
    *,
    ai_enabled: bool,
    no_questions: bool,
    ai_suggest,
    strip_question_trail,
    send,
) -> SendResult:
    message_text = text
    if ai_enabled:
        suggested = await ai_suggest(text)
        if suggested:
            message_text = suggested
    if no_questions:
        message_text = strip_question_trail(message_text)
    try:
        await send(message_text)
    except Exception as err:
        return SendResult(success=False, text_used=message_text, error=str(err))
    return SendResult(success=True, text_used=message_text)


def advance_flow(last_step: str, message_text: str, context: FlowContext) -> FlowActions:
    if last_step == STEP_CONTACT:
        return FlowActions(
            route="contact_chain",
            operations=["send_interest", "send_dating", "ask_clarify_if_needed"],
        )
    if last_step == STEP_CLARIFY:
        return FlowActions(
            route="clarify_chain",
            operations=["send_shifts", "send_shift_question"],
        )
    if last_step == STEP_SHIFT_QUESTION:
        return FlowActions(
            route="shift_question_chain",
            operations=["send_format", "auto_send_both_formats"],
        )
    if last_step == STEP_FORMAT_QUESTION:
        return FlowActions(
            route="format_choice",
            operations=["detect_format_choice", "deliver_format_content"],
        )
    if last_step == STEP_VIDEO_FOLLOWUP:
        return FlowActions(
            route="video_followup_chain",
            operations=["send_training", "ask_training_if_needed"],
        )
    if last_step == STEP_TRAINING_QUESTION:
        return FlowActions(route="training_question_chain", operations=["send_form"])
    if last_step == STEP_FORM:
        if context.is_question(message_text):
            return FlowActions(route="form_locked", operations=["send_form_lock_reply"])
        return FlowActions(route="form_done", operations=[])
    return FlowActions(route="noop", operations=[])
