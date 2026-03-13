import unittest

from flow_engine import (
    BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION,
    PeerRuntimeState,
    STEP_COMPANY_INTRO,
    STEP_PROOF_FORWARD,
    STEP_SCREENING_WAIT,
    STEP_SCHEDULE_CONFIRM,
    STEP_SCHEDULE_SHIFT_WAIT,
    advance_flow,
    balance_detour_checkpoint,
    balance_resume_message,
    balance_resume_step,
)


class FlowEngineV2Tests(unittest.TestCase):
    def test_age_reject_route(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_SCREENING_WAIT)
        actions = advance_flow(st, "ack_continue", {"age_bucket": "under18"})
        self.assertEqual(actions.route, "age_reject")

    def test_screening_to_company_intro(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_SCREENING_WAIT)
        actions = advance_flow(st, "ack_continue", {"age_bucket": "ok"})
        self.assertEqual(actions.route, "company_intro")

    def test_company_intro_branch(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_COMPANY_INTRO)
        actions = advance_flow(st, "question", {})
        self.assertEqual(actions.route, "voice_branch")

    def test_schedule_confirm_ack(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_SCHEDULE_CONFIRM)
        actions = advance_flow(st, "ack_continue", {})
        self.assertEqual(actions.route, "proof_forward")
        self.assertEqual(actions.set_state["flow_step"], STEP_PROOF_FORWARD)

    def test_balance_resume_checkpoint_helpers(self):
        self.assertEqual(balance_detour_checkpoint(STEP_SCHEDULE_CONFIRM), BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION)
        self.assertEqual(balance_resume_step(BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION), STEP_SCHEDULE_CONFIRM)
        self.assertIn("робочий процес", balance_resume_message(BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION).lower())

    def test_balance_confirm_ack_resumes_to_checkpoint(self):
        st = PeerRuntimeState(
            peer_id=1,
            flow_step="balance_confirm",
            resume_step_after_balance=STEP_SCHEDULE_SHIFT_WAIT,
            resume_checkpoint_after_balance="after_schedule_shift_prompt",
        )
        actions = advance_flow(st, "ack_continue", {})
        self.assertEqual(actions.route, "resume_after_balance")
        self.assertEqual(actions.set_state["flow_step"], STEP_SCHEDULE_SHIFT_WAIT)


if __name__ == "__main__":
    unittest.main()
