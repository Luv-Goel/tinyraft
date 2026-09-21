"""RPC message types exchanged between Raft nodes.

All messages are plain dataclasses serialized as JSON with a ``type``
discriminator so the wire format is inspectable with nothing more than
``nc`` or Wireshark.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from typing import Any, Optional


@dataclass
class RequestVote:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    preval: bool = False  # pre-vote probe (paper §9.6): no state mutated


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass
class AppendEntries:
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[dict]  # serialized LogEntry dicts
    leader_commit: int


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int = 0  # highest index known to match on the follower


# ---------------------------------------------------------------------- #
# wire format
# ---------------------------------------------------------------------- #

_TYPES = {
    "request_vote": RequestVote,
    "request_vote_reply": RequestVoteReply,
    "append_entries": AppendEntries,
    "append_entries_reply": AppendEntriesReply,
}


def encode(msg: Any) -> bytes:
    """Serialize a message to a length-prefixed JSON frame."""
    type_name = {v: k for k, v in _TYPES.items()}[type(msg)]
    payload = json.dumps({"type": type_name, "data": asdict(msg)}).encode()
    return len(payload).to_bytes(4, "big") + payload


def decode(buffer: bytes) -> tuple[Optional[Any], bytes]:
    """Split one frame off ``buffer``; returns (message, rest).

    Returns (None, buffer) when no complete frame is available yet.
    """
    if len(buffer) < 4:
        return None, buffer
    length = int.from_bytes(buffer[:4], "big")
    if len(buffer) < 4 + length:
        return None, buffer
    raw = json.loads(buffer[4:4 + length])
    cls = _TYPES[raw["type"]]
    return cls(**raw["data"]), buffer[4 + length:]
