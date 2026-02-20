import unittest

from flow_engine import (
    PeerRuntimeState,
    STEP_COMPANY_INTRO,
    STEP_SCREENING_WAIT,
    STEP_SCHEDULE_CONFIRM,
    advance_flow,
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


if __name__ == "__main__":
    unittest.main()
