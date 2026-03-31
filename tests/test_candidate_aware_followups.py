import importlib
import os
import sys
import types
import unittest


os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "test-hash")
os.environ.setdefault("SESSION_FILE", "/tmp/test.session")
os.environ.setdefault("SHEET_NAME", "test-sheet")
os.environ.setdefault("GOOGLE_CREDS", "/tmp/test-creds.json")

dotenv_mod = types.ModuleType("dotenv")
dotenv_mod.load_dotenv = lambda *args, **kwargs: None
sys.modules.setdefault("dotenv", dotenv_mod)

telethon_mod = types.ModuleType("telethon")
telethon_mod.TelegramClient = object
telethon_mod.events = types.SimpleNamespace(NewMessage=object)
sys.modules.setdefault("telethon", telethon_mod)

telethon_errors_mod = types.ModuleType("telethon.errors")
telethon_errors_mod.UsernameNotOccupiedError = type("UsernameNotOccupiedError", (Exception,), {})
telethon_errors_mod.PhoneNumberInvalidError = type("PhoneNumberInvalidError", (Exception,), {})
sys.modules.setdefault("telethon.errors", telethon_errors_mod)

telethon_tl_mod = types.ModuleType("telethon.tl")
telethon_tl_mod.functions = types.SimpleNamespace()
sys.modules.setdefault("telethon.tl", telethon_tl_mod)

telethon_tl_types_mod = types.ModuleType("telethon.tl.types")
telethon_tl_types_mod.User = type("User", (), {})
sys.modules.setdefault("telethon.tl.types", telethon_tl_types_mod)

gspread_mod = types.ModuleType("gspread")
gspread_mod.authorize = lambda *args, **kwargs: None
sys.modules.setdefault("gspread", gspread_mod)

gspread_exceptions_mod = types.ModuleType("gspread.exceptions")
gspread_exceptions_mod.APIError = type("APIError", (Exception,), {})
gspread_exceptions_mod.WorksheetNotFound = type("WorksheetNotFound", (Exception,), {})
sys.modules.setdefault("gspread.exceptions", gspread_exceptions_mod)

google_mod = types.ModuleType("google")
sys.modules.setdefault("google", google_mod)
google_oauth2_mod = types.ModuleType("google.oauth2")
sys.modules.setdefault("google.oauth2", google_oauth2_mod)
google_service_account_mod = types.ModuleType("google.oauth2.service_account")


class _Credentials:
    @classmethod
    def from_service_account_file(cls, *args, **kwargs):
        return cls()

    def with_scopes(self, *args, **kwargs):
        return self


google_service_account_mod.Credentials = _Credentials
sys.modules.setdefault("google.oauth2.service_account", google_service_account_mod)

auto_reply = importlib.import_module("auto_reply")
followup_training = importlib.import_module("followup_training")
PeerRuntimeState = auto_reply.PeerRuntimeState


class CandidateAwareFollowupTests(unittest.TestCase):
    def test_soft_shift_choice_is_treated_as_signal(self):
        self.assertTrue(auto_reply.is_soft_shift_choice("Думаю що денна"))
        self.assertEqual(
            auto_reply.classify_candidate_signal(auto_reply.STEP_SCHEDULE_SHIFT_WAIT, "Думаю що денна"),
            auto_reply.CANDIDATE_SIGNAL_SOFT_CHOICE,
        )

    def test_form_step_is_wait_step(self):
        self.assertIn(auto_reply.STEP_FORM_FORWARD, auto_reply.WAIT_STEP_SET)

    def test_company_intro_ack_followup_does_not_repeat_whole_block(self):
        state = PeerRuntimeState(peer_id=1)
        state.last_candidate_signal = auto_reply.CANDIDATE_SIGNAL_ACK
        state.last_candidate_signal_step = auto_reply.STEP_COMPANY_INTRO
        text = auto_reply.build_candidate_aware_followup_text(auto_reply.STEP_COMPANY_INTRO, 0, state)
        self.assertIn("можемо перейти далі", text)
        self.assertNotIn("Furioza Company", text)

    def test_shift_question_followup_answers_then_resumes(self):
        state = PeerRuntimeState(peer_id=2)
        state.last_candidate_signal = auto_reply.CANDIDATE_SIGNAL_QUESTION
        state.last_candidate_signal_step = auto_reply.STEP_SCHEDULE_SHIFT_WAIT
        state.last_candidate_signal_text = "А вихідні фіксовані?"
        text = auto_reply.build_candidate_aware_followup_text(auto_reply.STEP_SCHEDULE_SHIFT_WAIT, 0, state)
        self.assertIn("Коротко", text)
        self.assertIn("денну чи нічну", text)

    def test_schedule_confirm_followup_mentions_selected_shift(self):
        state = PeerRuntimeState(peer_id=3, shift_choice="денна")
        state.last_candidate_signal = auto_reply.CANDIDATE_SIGNAL_ACK
        state.last_candidate_signal_step = auto_reply.STEP_SCHEDULE_CONFIRM
        text = auto_reply.build_candidate_aware_followup_text(auto_reply.STEP_SCHEDULE_CONFIRM, 0, state)
        self.assertIn("Зафіксував денна зміну", text)

    def test_balance_question_followup_is_transparent(self):
        state = PeerRuntimeState(peer_id=4)
        state.last_candidate_signal = auto_reply.CANDIDATE_SIGNAL_QUESTION
        state.last_candidate_signal_step = auto_reply.STEP_BALANCE_CONFIRM
        state.last_candidate_signal_text = "А якщо тут без ставки?"
        text = auto_reply.build_candidate_aware_followup_text(auto_reply.STEP_BALANCE_CONFIRM, 0, state)
        self.assertIn("немає фіксованої ставки", text)
        self.assertIn("анкети", text)

    def test_form_delay_followup_stays_on_last_step(self):
        state = PeerRuntimeState(peer_id=5)
        state.last_candidate_signal = auto_reply.CANDIDATE_SIGNAL_DELAY
        state.last_candidate_signal_step = auto_reply.STEP_FORM_FORWARD
        text = auto_reply.build_candidate_aware_followup_text(auto_reply.STEP_FORM_FORWARD, 1, state)
        self.assertIn("анкети", text)
        self.assertIn("останній крок", text)

    def test_arm_step_wait_clears_followup_antispam_state(self):
        state = PeerRuntimeState(
            peer_id=6,
            last_followup_text="duplicate",
            last_followup_step=auto_reply.STEP_COMPANY_INTRO,
            step_followup_stage=1,
        )
        auto_reply.arm_step_wait(state, auto_reply.STEP_COMPANY_INTRO, 123.0)
        self.assertEqual(state.last_followup_text, "")
        self.assertEqual(state.last_followup_step, "")
        self.assertEqual(state.step_followup_stage, 0)

    def test_v2_followup_has_only_two_send_stages(self):
        self.assertEqual(
            auto_reply.resolve_v2_followup_stage(auto_reply.STEP_CLARIFY_DELAY_SEC, 0),
            ("STEP_WAIT_CLARIFY_SENT", 0, 1),
        )
        self.assertEqual(
            auto_reply.resolve_v2_followup_stage(auto_reply.STEP_FALLBACK_1_DELAY_SEC, 1),
            ("STEP_WAIT_FALLBACK6H_SENT", 1, 2),
        )
        self.assertIsNone(
            auto_reply.resolve_v2_followup_stage(auto_reply.STEP_FALLBACK_2_DELAY_SEC + 10, 2)
        )

    def test_duplicate_followup_text_is_detected_for_same_step(self):
        state = PeerRuntimeState(
            peer_id=7,
            last_followup_text="Якщо зручно, можемо коротко продовжити.",
            last_followup_step=auto_reply.STEP_COMPANY_INTRO,
        )
        self.assertTrue(
            auto_reply.is_duplicate_v2_followup(
                state,
                auto_reply.STEP_COMPANY_INTRO,
                "Якщо зручно, можемо коротко продовжити.",
            )
        )
        self.assertFalse(
            auto_reply.is_duplicate_v2_followup(
                state,
                auto_reply.STEP_SCHEDULE_SHIFT_WAIT,
                "Якщо зручно, можемо коротко продовжити.",
            )
        )

    def test_training_examples_cover_required_blocks(self):
        counts = {}
        for item in followup_training.RETURN_TRAINING_EXAMPLES:
            counts[item["step_name"]] = counts.get(item["step_name"], 0) + 1
        self.assertGreaterEqual(counts.get(auto_reply.STEP_COMPANY_INTRO, 0), 15)
        self.assertGreaterEqual(counts.get(auto_reply.STEP_SCHEDULE_SHIFT_WAIT, 0), 20)
        self.assertGreaterEqual(counts.get(auto_reply.STEP_SCHEDULE_CONFIRM, 0), 15)
        self.assertGreaterEqual(counts.get(auto_reply.STEP_BALANCE_CONFIRM, 0), 15)
        self.assertGreaterEqual(counts.get(auto_reply.STEP_FORM_FORWARD, 0), 10)


if __name__ == "__main__":
    unittest.main()
