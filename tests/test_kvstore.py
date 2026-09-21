"""Unit tests for the replicated key/value state machine."""

import unittest

from tinyraft.kvstore import KVStore


class KVStoreTest(unittest.TestCase):
    def test_basic_ops(self):
        kv = KVStore()
        self.assertIsNone(kv.apply({"op": "get", "key": "a"}))
        self.assertEqual(kv.apply({"op": "set", "key": "a", "value": "1"}), "OK")
        self.assertEqual(kv.apply({"op": "get", "key": "a"}), "1")
        self.assertTrue(kv.apply({"op": "delete", "key": "a"}))
        self.assertFalse(kv.apply({"op": "delete", "key": "a"}))
        self.assertIsNone(kv.apply({"op": "get", "key": "a"}))

    def test_cas(self):
        kv = KVStore()
        self.assertTrue(kv.apply(
            {"op": "cas", "key": "k", "expect": None, "value": "v1"}))
        self.assertFalse(kv.apply(
            {"op": "cas", "key": "k", "expect": "wrong", "value": "v2"}))
        self.assertEqual(kv.apply({"op": "get", "key": "k"}), "v1")
        self.assertTrue(kv.apply(
            {"op": "cas", "key": "k", "expect": "v1", "value": "v2"}))

    def test_snapshot_restore(self):
        kv = KVStore()
        kv.apply({"op": "set", "key": "a", "value": "1"})
        kv.apply({"op": "set", "key": "b", "value": "2"})
        snap = kv.snapshot()
        other = KVStore()
        other.restore(snap)
        self.assertEqual(other.apply({"op": "get", "key": "b"}), "2")

    def test_unknown_op_rejected(self):
        with self.assertRaises(ValueError):
            KVStore().apply({"op": "explode", "key": "x"})


if __name__ == "__main__":
    unittest.main()
