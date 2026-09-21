"""Replicated Raft log with JSON-lines persistence.

The log is 1-indexed (index 0 is a sentinel) to match the Raft paper.
Every mutation is appended to a WAL file and fsynced before the caller
may treat it as durable, mirroring the paper's persistence rules.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Any, Iterator, Optional


@dataclass
class LogEntry:
    term: int
    command: Any  # opaque state-machine command, must be JSON-serializable


class RaftLog:
    """Persistent, 1-indexed Raft log."""

    # Hard limits keep fuzz/soak tests from holding unbounded queues in memory.
    MAX_PENDING_ENTRIES = 10_000

    def __init__(self, path: Optional[str] = None):
        self._entries: list[LogEntry] = []  # index i in list == log index i+1
        self._path = path
        if path and os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        raw = json.loads(line)
                        self._entries.append(LogEntry(term=raw["term"], command=raw["command"]))

    # ------------------------------------------------------------------ #
    # inspection
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return len(self._entries)

    def __iter__(self) -> Iterator[LogEntry]:
        return iter(self._entries)

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def term_at(self, index: int) -> int:
        """Term of the entry at 1-based ``index``; 0 for the sentinel."""
        if index == 0:
            return 0
        if index < 0 or index > len(self._entries):
            raise IndexError(f"log index {index} out of range ({len(self._entries)})")
        return self._entries[index - 1].term

    def entry(self, index: int) -> LogEntry:
        if index < 1 or index > len(self._entries):
            raise IndexError(f"log index {index} out of range ({len(self._entries)})")
        return self._entries[index - 1]

    def entries_from(self, index: int) -> list[LogEntry]:
        """All entries with 1-based index >= ``index``."""
        return self._entries[max(index - 1, 0):]

    # ------------------------------------------------------------------ #
    # mutation (each call persists before returning)
    # ------------------------------------------------------------------ #

    def append(self, entries: list[LogEntry]) -> None:
        for e in entries:
            self._entries.append(e)
        self._persist(entries)

    def truncate_suffix(self, from_index: int) -> None:
        """Drop all entries with index >= ``from_index`` (1-based)."""
        if from_index < 1:
            raise ValueError("cannot truncate the sentinel")
        if from_index > len(self._entries):
            return
        del self._entries[from_index - 1:]
        self._rewrite()

    # ------------------------------------------------------------------ #
    # persistence
    # ------------------------------------------------------------------ #

    def _persist(self, new_entries: list[LogEntry]) -> None:
        if not self._path or not new_entries:
            return
        with open(self._path, "a", encoding="utf-8") as fh:
            for e in new_entries:
                fh.write(json.dumps(asdict(e)) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def _rewrite(self) -> None:
        if not self._path:
            return
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            for e in self._entries:
                fh.write(json.dumps(asdict(e)) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._path)
