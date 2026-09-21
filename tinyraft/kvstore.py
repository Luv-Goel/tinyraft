"""State machine replicated by the Raft log.

A linearizable key/value store with compare-and-swap. Commands arrive as
``LogEntry.command`` dicts; ``apply`` returns the client-visible result.
"""

from __future__ import annotations

from typing import Any, Optional


class KVStore:
    OPS = {"get", "set", "delete", "cas"}

    def __init__(self) -> None:
        self._data: dict[str, str] = {}

    def apply(self, command: dict) -> Any:
        op = command.get("op")
        key = command.get("key")
        if op not in self.OPS:
            raise ValueError(f"unknown op {op!r}")
        if op == "get":
            return self._data.get(key)
        if op == "set":
            self._data[key] = command["value"]
            return "OK"
        if op == "delete":
            existed = key in self._data
            self._data.pop(key, None)
            return existed
        if op == "cas":
            # compare-and-swap: swap in ``value`` only if current == ``expect``
            if self._data.get(key) == command.get("expect"):
                self._data[key] = command["value"]
                return True
            return False
        raise AssertionError("unreachable")

    def snapshot(self) -> dict[str, str]:
        return dict(self._data)

    def restore(self, data: dict[str, str]) -> None:
        self._data = dict(data)

    def __len__(self) -> int:
        return len(self._data)
