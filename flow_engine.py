from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


FLOW_V2 = "v2"

STEP_SCREENING_INTRO = "screening_intro"
STEP_SCREENING_FIT = "screening_fit"
STEP_AGE_REJECTED = "age_rejected"
STEP_VALUE_HOOK = "value_hook"
STEP_WORK_MODEL = "work_model"
STEP_FIT_COMMITMENT = "fit_commitment"
STEP_SHIFT_CLOSE = "shift_close"
STEP_OBJECTION_GATE = "objection_gate"
STEP_INCOME_MODEL = "income_model"
STEP_PROOF_FORWARD = "proof_forward"
STEP_TEST_CLOSE = "test_close"
STEP_FORM_HANDOFF = "form_handoff"
STEP_HANDOFF = "handoff"

STEP_SCREENING_WAIT = STEP_SCREENING_FIT
STEP_COMPANY_INTRO = STEP_VALUE_HOOK
STEP_VOICE_WAIT = STEP_WORK_MODEL
STEP_SCHEDULE_BLOCK = STEP_FIT_COMMITMENT
STEP_SCHEDULE_SHIFT_WAIT = STEP_SHIFT_CLOSE
STEP_SCHEDULE_CONFIRM = STEP_OBJECTION_GATE
STEP_BALANCE_CONFIRM = STEP_INCOME_MODEL
STEP_TEST_REVIEW = STEP_TEST_CLOSE
STEP_FORM_FORWARD = STEP_FORM_HANDOFF

LEGACY_STEP_NAME_MAP = {
    "screening_wait": STEP_SCREENING_FIT,
    "company_intro": STEP_VALUE_HOOK,
    "voice_wait": STEP_WORK_MODEL,
    "schedule_block": STEP_FIT_COMMITMENT,
    "schedule_shift_wait": STEP_SHIFT_CLOSE,
    "schedule_confirm": STEP_OBJECTION_GATE,
    "balance_confirm": STEP_INCOME_MODEL,
    "test_review": STEP_TEST_CLOSE,
    "form_forward": STEP_FORM_HANDOFF,
}

CHECKPOINT_AFTER_VALUE_HOOK = "after_value_hook"
CHECKPOINT_AFTER_WORK_MODEL = "after_work_model"
CHECKPOINT_AFTER_SHIFT_CLOSE = "after_shift_close"
CHECKPOINT_AFTER_OBJECTION_GATE = "after_objection_gate"

BALANCE_CHECKPOINT_AFTER_COMPANY_INTRO_OFFER_VOICE = CHECKPOINT_AFTER_VALUE_HOOK
BALANCE_CHECKPOINT_AFTER_VOICE_WAIT = CHECKPOINT_AFTER_WORK_MODEL
BALANCE_CHECKPOINT_AFTER_SCHEDULE_SHIFT_PROMPT = CHECKPOINT_AFTER_SHIFT_CLOSE
BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION = CHECKPOINT_AFTER_OBJECTION_GATE

LEGACY_CHECKPOINT_NAME_MAP = {
    "after_company_intro_offer_voice": CHECKPOINT_AFTER_VALUE_HOOK,
    "after_voice_wait": CHECKPOINT_AFTER_WORK_MODEL,
    "after_schedule_shift_prompt": CHECKPOINT_AFTER_SHIFT_CLOSE,
    "after_schedule_confirm_question": CHECKPOINT_AFTER_OBJECTION_GATE,
}

VOICE_IDLE = "idle"
VOICE_SENT = "sent"
VOICE_FALLBACK_SENT = "fallback_sent"
VOICE_AUTO_ADVANCED = "auto_advanced"

BALANCE_DETOUR_CHECKPOINT_BY_STEP = {
    STEP_VALUE_HOOK: CHECKPOINT_AFTER_VALUE_HOOK,
    STEP_WORK_MODEL: CHECKPOINT_AFTER_WORK_MODEL,
    STEP_SHIFT_CLOSE: CHECKPOINT_AFTER_SHIFT_CLOSE,
    STEP_OBJECTION_GATE: CHECKPOINT_AFTER_OBJECTION_GATE,
}

BALANCE_RESUME_STEP_BY_CHECKPOINT = {
    CHECKPOINT_AFTER_VALUE_HOOK: STEP_VALUE_HOOK,
    CHECKPOINT_AFTER_WORK_MODEL: STEP_WORK_MODEL,
    CHECKPOINT_AFTER_SHIFT_CLOSE: STEP_SHIFT_CLOSE,
    CHECKPOINT_AFTER_OBJECTION_GATE: STEP_OBJECTION_GATE,
}

BALANCE_RESUME_MESSAGE_BY_CHECKPOINT = {
    CHECKPOINT_AFTER_VALUE_HOOK:
        "Коротко пояснив модель доходу. Якщо зручно, перейдемо далі до формату роботи та наступного кроку.",
    CHECKPOINT_AFTER_WORK_MODEL:
        "По доходу коротко зорієнтував. Якщо все ок, зафіксуємо, який формат зміни Вам підходить.",
    CHECKPOINT_AFTER_SHIFT_CLOSE:
        "По доходу коротко зорієнтував. Підкажіть, будь ласка, яку зміну Вам зручніше обрати: денну чи нічну?",
    CHECKPOINT_AFTER_OBJECTION_GATE:
        "По доходу коротко зорієнтував. Якщо ще залишилися питання по формату роботи, із радістю уточню. Якщо все зрозуміло, рухаємось далі.",
}


@dataclass
class PeerRuntimeState:
    peer_id: int
    flow_step: str = STEP_SCREENING_WAIT
    flow_version: str = FLOW_V2
    qa_gate_active: bool = False
    qa_gate_step: str = ""
    qa_gate_reminder_sent: bool = False
    qa_gate_opened_at: float = 0.0
    voice_stage: str = VOICE_IDLE
    voice_sent_at: float = 0.0
    rejected_by_age: str = "none"
    referral_after_reject_sent: bool = False
    auto_mode: str = "ON"
    paused: bool = False
    screening_answers: List[str] = field(default_factory=list)
    screening_started_at: float = 0.0
    screening_last_at: float = 0.0
    screening_q1_asked: bool = False
    screening_q2_asked: bool = False
    screening_q1_answer: str = ""
    screening_q2_answer: str = ""
    shift_prompted_at: float = 0.0
    shift_choice: str = ""
    schedule_shift_fit_check_pending: bool = False
    schedule_confirm_clarify_count: int = 0
    balance_confirm_clarify_count: int = 0
    test_answers: List[str] = field(default_factory=list)
    test_prompted_at: float = 0.0
    test_help_sent: bool = False
    test_message_count: int = 0
    test_last_message: str = ""
    test_ready_clarify_count: int = 0
    form_waiting_photo: bool = False
    form_prompted_at: float = 0.0
    form_photo_reminder_sent: bool = False
    step_wait_started_at: float = 0.0
    step_wait_step: str = ""
    step_followup_stage: int = 0
    step_followup_last_at: float = 0.0
    resume_step_after_balance: str = ""
    resume_checkpoint_after_balance: str = ""
    balance_block_shown: bool = False
    balance_block_skipped: bool = False


@dataclass
class FlowActions:
    route: str
    messages: List[str] = field(default_factory=list)
    forwards: List[str] = field(default_factory=list)
    set_state: Dict[str, object] = field(default_factory=dict)
    timers: List[Dict[str, object]] = field(default_factory=list)
    await_confirmation: bool = False


def canonical_step_name(step_name: str) -> str:
    normalized = (step_name or "").strip()
    if not normalized:
        return ""
    return LEGACY_STEP_NAME_MAP.get(normalized, normalized)


def canonical_checkpoint_name(checkpoint: str) -> str:
    normalized = (checkpoint or "").strip()
    if not normalized:
        return ""
    return LEGACY_CHECKPOINT_NAME_MAP.get(normalized, normalized)


def balance_detour_checkpoint(step_name: str) -> str:
    return BALANCE_DETOUR_CHECKPOINT_BY_STEP.get(canonical_step_name(step_name), "")


def balance_resume_step(checkpoint: str) -> str:
    return BALANCE_RESUME_STEP_BY_CHECKPOINT.get(canonical_checkpoint_name(checkpoint), "")


def balance_resume_message(checkpoint: str) -> str:
    return BALANCE_RESUME_MESSAGE_BY_CHECKPOINT.get(canonical_checkpoint_name(checkpoint), "")


def advance_flow(peer_state: PeerRuntimeState, intent: str, context: Optional[Dict[str, object]] = None) -> FlowActions:
    context = context or {}
    step = canonical_step_name(peer_state.flow_step)

    if step == STEP_SCREENING_FIT:
        return FlowActions(route="value_hook", set_state={"flow_step": STEP_VALUE_HOOK})

    if step == STEP_VALUE_HOOK:
        if intent == "ack_continue":
            return FlowActions(route="shift_close", set_state={"flow_step": STEP_SHIFT_CLOSE})
        if intent == "other":
            return FlowActions(route="work_model_wait")
        return FlowActions(route="work_model")

    if step == STEP_WORK_MODEL:
        if intent == "ack_continue":
            return FlowActions(route="shift_close", set_state={"flow_step": STEP_SHIFT_CLOSE})
        return FlowActions(route="work_model_wait")

    if step == STEP_SHIFT_CLOSE:
        return FlowActions(route="objection_gate", set_state={"flow_step": STEP_OBJECTION_GATE}, await_confirmation=True)

    if step == STEP_OBJECTION_GATE:
        if intent == "ack_continue":
            return FlowActions(route="proof_forward", set_state={"flow_step": STEP_PROOF_FORWARD})
        return FlowActions(route="objection_gate")

    if step == STEP_INCOME_MODEL:
        if intent == "ack_continue":
            resume_step_name = balance_resume_step(peer_state.resume_checkpoint_after_balance)
            if resume_step_name:
                return FlowActions(route="resume_after_balance", set_state={"flow_step": resume_step_name})
            return FlowActions(route="proof_forward", set_state={"flow_step": STEP_PROOF_FORWARD})
        return FlowActions(route="income_model")

    if step == STEP_PROOF_FORWARD:
        return FlowActions(route="test_close", set_state={"flow_step": STEP_TEST_CLOSE})

    if step == STEP_TEST_CLOSE:
        return FlowActions(route="form_handoff", set_state={"flow_step": STEP_FORM_HANDOFF})

    if step == STEP_FORM_HANDOFF:
        return FlowActions(route="handoff", set_state={"flow_step": STEP_HANDOFF})

    return FlowActions(route="idle")
