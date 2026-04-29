import unittest

from faq_service import (
    build_cluster_key,
    build_voice_text_recap_blocks,
    load_sales_script_context,
    normalize_question,
)


class FAQServiceTests(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(normalize_question(" ЯКИЙ графік? "), "який графік")

    def test_cluster_key(self):
        key = build_cluster_key("a" * 200)
        self.assertEqual(len(key), 160)

    def test_build_voice_text_recap_blocks(self):
        blocks = build_voice_text_recap_blocks()
        self.assertEqual(len(blocks), 2)
        self.assertTrue(all(bool((b or "").strip()) for b in blocks))
        self.assertIn("дох", blocks[1].lower())

    def test_sales_script_loaded(self):
        script = load_sales_script_context()
        self.assertIn("Furioza Company HR Sales Script", script)
        self.assertIn("Документ і ГПД", script)


if __name__ == "__main__":
    unittest.main()
