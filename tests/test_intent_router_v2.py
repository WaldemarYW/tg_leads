import unittest

from intent_router import detect_intent


class IntentRouterTests(unittest.TestCase):
    def test_question_detected(self):
        result = detect_intent("Який графік роботи", "schedule")
        self.assertEqual(result.intent, "question")

    def test_ack_detected(self):
        result = detect_intent("так", "schedule")
        self.assertEqual(result.intent, "ack_continue")


if __name__ == "__main__":
    unittest.main()
