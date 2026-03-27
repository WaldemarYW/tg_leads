import unittest

from flow_engine import (
    BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION,
    PeerRuntimeState,
    STEP_OBJECTION_GATE,
    STEP_PROOF_FORWARD,
    STEP_SCREENING_FIT,
    STEP_SHIFT_CLOSE,
    STEP_VALUE_HOOK,
    canonical_checkpoint_name,
    canonical_step_name,
    advance_flow,
    balance_detour_checkpoint,
    balance_resume_message,
    balance_resume_step,
)


class FlowEngineV2Tests(unittest.TestCase):
    def test_screening_ignores_age_bucket_and_moves_forward(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_SCREENING_FIT)
        actions = advance_flow(st, "ack_continue", {"age_bucket": "under18"})
        self.assertEqual(actions.route, "value_hook")

    def test_screening_to_company_intro(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_SCREENING_FIT)
        actions = advance_flow(st, "ack_continue", {"age_bucket": "ok"})
        self.assertEqual(actions.route, "value_hook")

    def test_company_intro_branch(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_VALUE_HOOK)
        actions = advance_flow(st, "question", {})
        self.assertEqual(actions.route, "work_model")

    def test_schedule_confirm_ack(self):
        st = PeerRuntimeState(peer_id=1, flow_step=STEP_OBJECTION_GATE)
        actions = advance_flow(st, "ack_continue", {})
        self.assertEqual(actions.route, "proof_forward")
        self.assertEqual(actions.set_state["flow_step"], STEP_PROOF_FORWARD)

    def test_balance_resume_checkpoint_helpers(self):
        self.assertEqual(balance_detour_checkpoint(STEP_OBJECTION_GATE), BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION)
        self.assertEqual(balance_resume_step(BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION), STEP_OBJECTION_GATE)
        self.assertIn("формату роботи", balance_resume_message(BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION).lower())

    def test_balance_confirm_ack_resumes_to_checkpoint(self):
        st = PeerRuntimeState(
            peer_id=1,
            flow_step="balance_confirm",
            resume_step_after_balance=STEP_SHIFT_CLOSE,
            resume_checkpoint_after_balance="after_schedule_shift_prompt",
        )
        actions = advance_flow(st, "ack_continue", {})
        self.assertEqual(actions.route, "resume_after_balance")
        self.assertEqual(actions.set_state["flow_step"], STEP_SHIFT_CLOSE)

    def test_canonical_names_keep_legacy_runtime_compatible(self):
        self.assertEqual(canonical_step_name("schedule_confirm"), STEP_OBJECTION_GATE)
        self.assertEqual(canonical_checkpoint_name("after_schedule_confirm_question"), BALANCE_CHECKPOINT_AFTER_SCHEDULE_CONFIRM_QUESTION)


if __name__ == "__main__":
    unittest.main()
