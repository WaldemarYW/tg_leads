import os
import tempfile
import unittest

from hr_filter_store import HrFilterStore, HrForwardDeduper


class HrFilterStoreTests(unittest.TestCase):
    def test_upsert_match_list_and_delete_rule(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = os.path.join(tmpdir.name, "hr_filters.json")
        store = HrFilterStore(path, cache_ttl_sec=0)

        rule = store.upsert_rule("@RedFox1378", "https://t.me/hr_target")
        self.assertEqual(rule["username_norm"], "redfox1378")
        self.assertEqual(rule["target_group_link"], "https://t.me/hr_target")

        matched = store.match_rule("redfox1378")
        self.assertIsNotNone(matched)
        self.assertEqual(matched["target_group_link"], "https://t.me/hr_target")

        listed = store.list_rules(force=True)
        self.assertEqual(len(listed), 1)
        self.assertEqual(listed[0]["username_norm"], "redfox1378")

        self.assertTrue(store.delete_rule("@redfox1378"))
        self.assertIsNone(store.match_rule("@redfox1378"))


class HrForwardDeduperTests(unittest.TestCase):
    def test_claim_allows_first_process_only(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        path = os.path.join(tmpdir.name, "hr_filter_forwards.json")
        deduper = HrForwardDeduper(path)

        self.assertTrue(deduper.claim(-100123, 55, "primary"))
        self.assertFalse(deduper.claim(-100123, 55, "alt"))
        self.assertTrue(deduper.claim(-100123, 56, "alt"))


if __name__ == "__main__":
    unittest.main()
