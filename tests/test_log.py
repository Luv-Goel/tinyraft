"""Unit tests for the persistent Raft log."""

import tempfile
import unittest
from pathlib import Path

from tinyraft.log import LogEntry, RaftLog


class RaftLogTest(unittest.TestCase):
    def test_append_and_index(self):
        log = RaftLog()
        self.assertEqual(log.last_index, 0)
        self.assertEqual(log.last_term, 0)
        log.append([LogEntry(1, {"op": "set", "key": "a", "value": "1"}),
                    LogEntry(1, {"op": "set", "key": "b", "value": "2"}),
                    LogEntry(2, {"op": "delete", "key": "a"})])
        self.assertEqual(log.last_index, 3)
        self.assertEqual(log.last_term, 2)
        self.assertEqual(log.term_at(0), 0)
        self.assertEqual(log.term_at(1), 1)
        self.assertEqual(log.term_at(3), 2)
        with self.assertRaises(IndexError):
            log.term_at(4)

    def test_entries_from(self):
        log = RaftLog()
        log.append([LogEntry(i, {"n": i}) for i in (1, 1, 2, 3)])
        tail = log.entries_from(3)
        self.assertEqual([e.command["n"] for e in tail], [2, 3])

    def test_truncate_suffix(self):
        log = RaftLog()
        log.append([LogEntry(1, {"n": 1}), LogEntry(2, {"n": 2}),
                    LogEntry(2, {"n": 3})])
        log.truncate_suffix(2)
        self.assertEqual(log.last_index, 1)
        self.assertEqual(log.last_term, 1)
        with self.assertRaises(ValueError):
            log.truncate_suffix(0)

    def test_persistence_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "log.jsonl")
            log = RaftLog(path)
            log.append([LogEntry(4, {"op": "set", "key": "x", "value": "9"})])
            log.append([LogEntry(5, {"op": "delete", "key": "x"})])
            reloaded = RaftLog(path)
            self.assertEqual(reloaded.last_index, 2)
            self.assertEqual(reloaded.term_at(1), 4)
            self.assertEqual(reloaded.term_at(2), 5)
            self.assertEqual(reloaded.entry(2).command["op"], "delete")

    def test_truncate_is_persistent(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / "log.jsonl")
            log = RaftLog(path)
            log.append([LogEntry(1, {"n": 1}), LogEntry(3, {"n": 2})])
            log.truncate_suffix(2)
            reloaded = RaftLog(path)
            self.assertEqual(reloaded.last_index, 1)
            self.assertEqual(reloaded.last_term, 1)


if __name__ == "__main__":
    unittest.main()
