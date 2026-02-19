import asyncio
import unittest

from auto_reply_flow import (
    FlowContext,
    STEP_CLARIFY,
    STEP_CONTACT,
    STEP_FORM,
    STEP_FORMAT_QUESTION,
    STEP_SHIFT_QUESTION,
    STEP_TRAINING_QUESTION,
    STEP_VIDEO_FOLLOWUP,
    advance_flow,
    send_message_with_fallback,
)


class FlowTests(unittest.TestCase):
    def test_advance_flow_routes(self):
        ctx = FlowContext(is_question=lambda text: "?" in text)
        self.assertEqual(advance_flow(STEP_CONTACT, "", ctx).route, "contact_chain")
        self.assertEqual(advance_flow(STEP_CLARIFY, "", ctx).route, "clarify_chain")
        self.assertEqual(advance_flow(STEP_SHIFT_QUESTION, "", ctx).route, "shift_question_chain")
        self.assertEqual(advance_flow(STEP_FORMAT_QUESTION, "", ctx).route, "format_choice")
        self.assertEqual(advance_flow(STEP_VIDEO_FOLLOWUP, "", ctx).route, "video_followup_chain")
        self.assertEqual(advance_flow(STEP_TRAINING_QUESTION, "", ctx).route, "training_question_chain")
        self.assertEqual(advance_flow(STEP_FORM, "есть вопрос?", ctx).route, "form_locked")

    def test_send_message_with_fallback(self):
        sent = {}

        async def ai_suggest(_text):
            return "AI text?"

        def strip_q(text):
            return text.replace("?", "")

        async def sender(text):
            sent["text"] = text

        result = asyncio.run(send_message_with_fallback(
            "base",
            ai_enabled=True,
            no_questions=True,
            ai_suggest=ai_suggest,
            strip_question_trail=strip_q,
            send=sender,
        ))

        self.assertTrue(result.success)
        self.assertEqual(result.text_used, "AI text")
        self.assertEqual(sent["text"], "AI text")


if __name__ == "__main__":
    unittest.main()
