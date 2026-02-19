import asyncio
import unittest

from auto_reply_classifiers import (
    Decision,
    Intent,
    classify_format_choice,
    classify_intent,
    classify_stop_continue,
    is_continue_phrase,
    is_neutral_ack,
    is_short_neutral_ack,
    is_stop_phrase,
    message_has_question,
)


class ClassifierTests(unittest.TestCase):
    def test_question_detection(self):
        self.assertTrue(message_has_question("Як це працює"))
        self.assertTrue(message_has_question("ok?"))
        self.assertTrue(message_has_question("подскажи по графику"))
        self.assertFalse(message_has_question("я по ночам работаю"))
        self.assertFalse(message_has_question("Дякую, все зрозуміло"))

    def test_stop_phrase(self):
        self.assertTrue(is_stop_phrase("мені не цікаво"))
        self.assertFalse(is_stop_phrase("все зрозуміло, дякую"))

    def test_continue_and_ack(self):
        self.assertTrue(is_continue_phrase("питань нема"))
        self.assertTrue(is_neutral_ack("ок, зрозуміло"))
        self.assertTrue(is_short_neutral_ack("нема"))

    def test_classify_intent_local(self):
        async def ai_client(_history, _text):
            return "other"

        q = asyncio.run(classify_intent("подскажи по оплате", [], last_step="clarify", ai_client=ai_client))
        self.assertEqual(q, Intent.QUESTION)

        ack = asyncio.run(classify_intent("нема", [], last_step="clarify", ai_client=ai_client))
        self.assertEqual(ack, Intent.ACK_CONTINUE)

    def test_classify_stop_continue_fallback_and_ai(self):
        async def ai_client(_history, _text):
            return True

        decision_direct = asyncio.run(classify_stop_continue("не буду работать", [], ai_client=None))
        self.assertEqual(decision_direct, Decision.STOP)

        decision_ai = asyncio.run(classify_stop_continue("мм", [], ai_client=ai_client))
        self.assertEqual(decision_ai, Decision.STOP)

    def test_classify_format_choice(self):
        async def ai_client(_history, _text):
            return "mini_course"

        explicit = asyncio.run(classify_format_choice("хочу відео", [], ai_client=ai_client))
        self.assertEqual(explicit, "video")

        unknown_via_ai = asyncio.run(classify_format_choice("подумати", [], ai_client=ai_client))
        self.assertEqual(unknown_via_ai, "mini_course")


if __name__ == "__main__":
    unittest.main()
