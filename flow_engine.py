from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


FLOW_V2 = "v2"

STEP_SCREENING_INTRO = "screening_intro"
STEP_SCREENING_WAIT = "screening_wait"
STEP_AGE_REJECTED = "age_rejected"
STEP_COMPANY_INTRO = "company_intro"
STEP_VOICE_WAIT = "voice_wait"
STEP_SCHEDULE_BLOCK = "schedule_block"
STEP_SCHEDULE_SHIFT_WAIT = "schedule_shift_wait"
STEP_SCHEDULE_CONFIRM = "schedule_confirm"
STEP_PROOF_FORWARD = "proof_forward"
STEP_TEST_REVIEW = "test_review"
STEP_FORM_FORWARD = "form_forward"
STEP_HANDOFF = "handoff"

VOICE_IDLE = "idle"
VOICE_SENT = "sent"
VOICE_FALLBACK_SENT = "fallback_sent"
VOICE_AUTO_ADVANCED = "auto_advanced"


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
    shift_prompted_at: float = 0.0
    shift_choice: str = ""
    test_answers: List[str] = field(default_factory=list)


@dataclass
class FlowActions:
    route: str
    messages: List[str] = field(default_factory=list)
    forwards: List[str] = field(default_factory=list)
    set_state: Dict[str, object] = field(default_factory=dict)
    timers: List[Dict[str, object]] = field(default_factory=list)
    await_confirmation: bool = False


def advance_flow(peer_state: PeerRuntimeState, intent: str, context: Optional[Dict[str, object]] = None) -> FlowActions:
    context = context or {}
    step = peer_state.flow_step

    if peer_state.rejected_by_age in {"under18", "over40"}:
        return FlowActions(route="age_rejected_blocked")

    if step == STEP_SCREENING_WAIT:
        age_bucket = str(context.get("age_bucket") or "unknown")
        if age_bucket in {"under18", "over40"}:
            return FlowActions(
                route="age_reject",
                set_state={
                    "flow_step": STEP_AGE_REJECTED,
                    "rejected_by_age": age_bucket,
                    "auto_mode": "OFF",
                    "paused": True,
                },
            )
        return FlowActions(route="company_intro", set_state={"flow_step": STEP_COMPANY_INTRO})

    if step == STEP_COMPANY_INTRO:
        if intent == "ack_continue":
            return FlowActions(route="schedule_shift_wait", set_state={"flow_step": STEP_SCHEDULE_SHIFT_WAIT})
        if intent == "other":
            return FlowActions(route="voice_branch_wait")
        return FlowActions(route="voice_branch")

    if step == STEP_VOICE_WAIT:
        if intent == "ack_continue":
            return FlowActions(route="schedule_shift_wait", set_state={"flow_step": STEP_SCHEDULE_SHIFT_WAIT})
        return FlowActions(route="voice_wait")

    if step == STEP_SCHEDULE_SHIFT_WAIT:
        return FlowActions(route="schedule_confirm", set_state={"flow_step": STEP_SCHEDULE_CONFIRM}, await_confirmation=True)

    if step == STEP_SCHEDULE_CONFIRM:
        if intent == "ack_continue":
            return FlowActions(route="proof_forward", set_state={"flow_step": STEP_PROOF_FORWARD})
        return FlowActions(route="schedule_confirm")

    if step == STEP_PROOF_FORWARD:
        return FlowActions(route="test_review", set_state={"flow_step": STEP_TEST_REVIEW})

    if step == STEP_TEST_REVIEW:
        return FlowActions(route="form_forward", set_state={"flow_step": STEP_FORM_FORWARD})

    if step == STEP_FORM_FORWARD:
        return FlowActions(route="handoff", set_state={"flow_step": STEP_HANDOFF})

    return FlowActions(route="idle")
