"""Core Raft consensus node (asyncio).

Implements the Raft paper (Ongaro & Ousterhout, 2014) §5:

* leader election with randomized timeouts and pre-vote-free majority rule
* log replication with ``next_index`` backtracking
* commitment restricted to entries from the leader's *current* term (§5.4.2)
* persistent state (voted_for, current_term, log) survives restarts
* client interface with linearizable reads (read-index style: leader must
  have committed an entry in its current term before serving reads)

A node owns one asyncio server; peers are reached over TCP with the
length-prefixed JSON framing from ``messages.py``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

from .log import LogEntry, RaftLog
from .kvstore import KVStore
from . import messages
from .messages import (
    AppendEntries, AppendEntriesReply,
    RequestVote, RequestVoteReply,
)

log = logging.getLogger("tinyraft")


class State(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeaderError(Exception):
    def __init__(self, leader_id: Optional[str]):
        super().__init__(f"not the leader (leader={leader_id})")
        self.leader_id = leader_id


class RaftNode:
    MIN_TIMEOUT = 0.15  # seconds; scaled down for tests via ctor args
    MAX_TIMEOUT = 0.30
    HEARTBEAT = 0.05

    def __init__(
        self,
        node_id: str,
        peers: dict[str, tuple[str, int]],  # peer_id -> (host, port)
        data_dir: Optional[str] = None,
        election_timeout: Optional[tuple[float, float]] = None,
        heartbeat_interval: Optional[float] = None,
    ):
        if node_id in peers:
            raise ValueError("node_id must not appear in peers")
        self.id = node_id
        self.peers = dict(peers)
        self._lo, self._hi = election_timeout or (self.MIN_TIMEOUT, self.MAX_TIMEOUT)
        self._hb = heartbeat_interval if heartbeat_interval is not None else self.HEARTBEAT

        # persistent state
        self._dir = data_dir
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
        self.log = RaftLog(os.path.join(data_dir, "log.jsonl") if data_dir else None)
        self._state_path = os.path.join(data_dir, "state.json") if data_dir else None
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self._load_persistent_state()

        # volatile state
        self.state = State.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        self.leader_id: Optional[str] = None
        self.kv = KVStore()

        # leader volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # bookkeeping
        self._votes_received: set[str] = set()
        self._last_heartbeat = 0.0
        self._server: Optional[asyncio.AbstractServer] = None
        self._tasks: list[asyncio.Task] = []
        self._apply_cv = asyncio.Condition()
        self._stopped = asyncio.Event()
        self.on_applied: Optional[Callable[[int, LogEntry], None]] = None
        self._applied_results: dict[int, Any] = {}

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #

    @property
    def address(self) -> tuple[str, int]:
        return self.peers[self.id] if self.id in self.peers else self._bound_addr

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> tuple[str, int]:
        self._last_heartbeat = self._now()
        self._server = await asyncio.start_server(self._handle_conn, host, port)
        sock = self._server.sockets[0]
        self._bound_addr = sock.getsockname()[:2]
        self._tasks = [
            asyncio.create_task(self._election_timer_loop(), name=f"{self.id}-timer"),
            asyncio.create_task(self._applier_loop(), name=f"{self.id}-apply"),
        ]
        return self._bound_addr

    async def stop(self) -> None:
        self._stopped.set()
        for t in self._tasks:
            t.cancel()
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ------------------------------------------------------------------ #
    # client API
    # ------------------------------------------------------------------ #

    async def client_command(self, command: dict, timeout: float = 5.0) -> Any:
        """Replicate ``command`` through Raft and apply it. Leader-only."""
        if self.state is not State.LEADER:
            raise NotLeaderError(self.leader_id)
        entry = LogEntry(term=self.current_term, command=command)
        self.log.append([entry])
        index = self.log.last_index
        # step down immediately if we lost leadership while appending
        if self.state is not State.LEADER:
            raise NotLeaderError(self.leader_id)
        await self._wait_committed(index, timeout)
        # The entry may have been overwritten (truncated by a new leader)
        # while this node's leadership was unstable. Verify the entry now
        # sitting at ``index`` is really ours before reporting success —
        # otherwise a lost write would be acknowledged. If it was replaced,
        # the command never committed and the client must retry (paper §8).
        if (index > self.log.last_index
                or self.log.term_at(index) != entry.term
                or self.log.entry(index).command != command):
            raise NotLeaderError(self.leader_id)
        # wait for OUR applier to process it so writes are applied once and
        # the returned value is the real state-machine result
        await self._wait_applied(index, timeout)
        return self._applied_results.pop(index, None)

    async def read(self, key: str, timeout: float = 5.0) -> Optional[str]:
        """Linearizable read: served only after committing an entry this term."""
        if self.state is not State.LEADER:
            raise NotLeaderError(self.leader_id)
        # Raft §6.4: a leader must commit an entry from its current term
        # before serving reads, otherwise an old value could be returned.
        if self.log.last_index == 0 or self.log.last_term != self.current_term:
            await self.client_command({"op": "get", "key": "__read_barrier__"}, timeout)
        idx = self.commit_index
        await self._wait_applied(idx, timeout)
        return self.kv.apply({"op": "get", "key": key})

    async def _wait_committed(self, index: int, timeout: float) -> None:
        async def _wait():
            async with self._apply_cv:
                await self._apply_cv.wait_for(lambda: self.commit_index >= index)
        try:
            await asyncio.wait_for(_wait(), timeout)
        except asyncio.TimeoutError:
            raise TimeoutError(f"entry {index} not committed within {timeout}s")
        if self.log.term_at(index) != self.current_term:
            # entry was overwritten by a new leader: client must retry
            raise NotLeaderError(self.leader_id)

    async def _wait_applied(self, index: int, timeout: float) -> None:
        async def _wait():
            async with self._apply_cv:
                await self._apply_cv.wait_for(lambda: self.last_applied >= index)
        await asyncio.wait_for(_wait(), timeout)

    # ------------------------------------------------------------------ #
    # timers
    # ------------------------------------------------------------------ #

    def _now(self) -> float:
        return asyncio.get_event_loop().time()

    def _random_timeout(self) -> float:
        return random.uniform(self._lo, self._hi)

    async def _election_timer_loop(self) -> None:
        while True:
            timeout = self._random_timeout()
            await asyncio.sleep(timeout)
            if self.state is State.LEADER:
                continue
            if self._now() - self._last_heartbeat >= timeout:
                await self._start_election()

    async def _heartbeat_loop(self) -> None:
        while self.state is State.LEADER:
            await self._broadcast_heartbeat()
            await asyncio.sleep(self._hb)

    # ------------------------------------------------------------------ #
    # elections (paper §5.2)
    # ------------------------------------------------------------------ #

    async def _start_election(self) -> None:
        # Pre-vote (paper §9.6): only disturb the cluster with a real term
        # bump if a mock election says we could win. A stale or partitioned
        # node therefore keeps retrying silently instead of forcing the
        # sitting leader to step down over and over (liveness).
        if not await self._prevote_round():
            self._last_heartbeat = self._now()
            return
        self.state = State.CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self._persist_state()
        self.leader_id = None
        self._last_heartbeat = self._now()
        self._votes_received = {self.id}
        log.info("%s: starting election for term %d", self.id, self.current_term)

        async def ask(peer_id: str) -> Optional[RequestVoteReply]:
            req = RequestVote(
                term=self.current_term,
                candidate_id=self.id,
                last_log_index=self.log.last_index,
                last_log_term=self.log.last_term,
            )
            try:
                # a vote roundtrip must fit well inside an election timeout,
                # otherwise a slow/dead peer stalls the whole election
                return await self._rpc(peer_id, req, timeout=self._lo * 2)
            except (OSError, asyncio.TimeoutError, ConnectionError):
                return None

        replies = await asyncio.gather(*(ask(p) for p in self.peers),
                                       return_exceptions=True)
        # we may have stepped down while waiting (new term seen)
        if self.state is not State.CANDIDATE:
            return
        for r in replies:
            if isinstance(r, RequestVoteReply) and r.term > self.current_term:
                self._become_follower(r.term)
                return
        votes = 1  # we vote for ourselves
        for r in replies:
            if (isinstance(r, RequestVoteReply) and r.vote_granted
                    and r.term == self.current_term):
                votes += 1
        if votes * 2 > len(self.peers) + 1:  # strict majority of the cluster
            self._become_leader()

    async def _prevote_round(self) -> bool:
        """Ask every peer 'would you vote for me?' without mutating state.

        Returns True when a strict majority (self included) signals it
        would grant. Peers grant only when the candidate's log is
        up-to-date AND they have not heard from a live leader recently.
        """
        async def probe(peer_id: str) -> Optional[RequestVoteReply]:
            req = RequestVote(
                term=self.current_term + 1,
                candidate_id=self.id,
                last_log_index=self.log.last_index,
                last_log_term=self.log.last_term,
                preval=True,
            )
            try:
                return await self._rpc(peer_id, req, timeout=self._lo * 2)
            except (OSError, asyncio.TimeoutError, ConnectionError):
                return None

        replies = await asyncio.gather(*(probe(p) for p in self.peers),
                                       return_exceptions=True)
        grants = 1  # self
        for r in replies:
            if isinstance(r, RequestVoteReply) and r.vote_granted:
                grants += 1
        return grants * 2 > len(self.peers) + 1

    def _become_follower(self, term: int) -> None:
        log.info("%s: follower in term %d", self.id, term)
        self.state = State.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self._persist_state()

    def _become_leader(self) -> None:
        log.info("%s: LEADER for term %d", self.id, self.current_term)
        self.state = State.LEADER
        self.leader_id = self.id
        for p in self.peers:
            self.next_index[p] = self.log.last_index + 1
            self.match_index[p] = 0
        self._tasks.append(asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.id}-hb"))
        # commit a no-op so reads become linearizable quickly (§6.4 rec.)
        self.log.append([LogEntry(term=self.current_term,
                                  command={"op": "get", "key": "__noop__"})])
        asyncio.create_task(self._replicate_all())

    # ------------------------------------------------------------------ #
    # replication (paper §5.3)
    # ------------------------------------------------------------------ #

    async def _broadcast_heartbeat(self) -> None:
        await self._replicate_all()

    async def _replicate_all(self) -> None:
        if self.state is not State.LEADER:
            return
        await asyncio.gather(*(self._replicate_to(p) for p in self.peers),
                             return_exceptions=True)

    async def _replicate_to(self, peer_id: str) -> None:
        if self.state is not State.LEADER:
            return
        next_idx = self.next_index.get(peer_id, self.log.last_index + 1)
        prev_index = next_idx - 1
        prev_term = self.log.term_at(prev_index) if prev_index > 0 else 0
        entries = [ {"term": e.term, "command": e.command}
                    for e in self.log.entries_from(next_idx) ]
        req = AppendEntries(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_index,
            prev_log_term=prev_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        try:
            reply: AppendEntriesReply = await self._rpc(peer_id, req)
        except (OSError, asyncio.TimeoutError, ConnectionError):
            return
        if reply is None:
            return
        if reply.term > self.current_term:
            self._become_follower(reply.term)
            return
        if self.state is not State.LEADER or reply.term != self.current_term:
            return
        if reply.success:
            last_sent = prev_index + len(entries)
            self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), last_sent)
            self.next_index[peer_id] = self.match_index[peer_id] + 1
            await self._advance_commit()
        else:
            # log inconsistency: back off and retry (paper §5.3 fast path
            # via the follower's match_hint is left as future work)
            self.next_index[peer_id] = max(1, next_idx - 1)
            await self._replicate_to(peer_id)

    async def _advance_commit(self) -> None:
        """commit_index = max N such that N > commit_index, a majority has
        match_index >= N, and log[N].term == current_term (paper §5.4.2)."""
        cluster_size = len(self.peers) + 1
        for n in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(n) != self.current_term:
                continue  # §5.4.2: never commit prior-term entries by counting
            replicated = 1 + sum(
                1 for p in self.peers if self.match_index.get(p, 0) >= n)
            if replicated * 2 > cluster_size:
                self.commit_index = n
                async with self._apply_cv:
                    self._apply_cv.notify_all()
                break

    # ------------------------------------------------------------------ #
    # RPC handlers (receiver side)
    # ------------------------------------------------------------------ #

    async def _handle_conn(self, reader: asyncio.StreamReader,
                           writer: asyncio.StreamWriter) -> None:
        buf = b""
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    return
                buf += chunk
                while True:
                    msg, buf = messages.decode(buf)
                    if msg is None:
                        break
                    reply = await self._dispatch(msg)
                    if reply is not None:
                        writer.write(messages.encode(reply))
                        await writer.drain()
        except (ConnectionResetError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    async def _dispatch(self, msg: Any) -> Any:
        if isinstance(msg, RequestVote):
            return self._on_request_vote(msg)
        if isinstance(msg, AppendEntries):
            return await self._on_append_entries(msg)
        return None

    def _on_request_vote(self, req: RequestVote) -> RequestVoteReply:
        # §5.4.1: candidate's log must be at least as up-to-date as ours
        up_to_date = (
            req.last_log_term > self.log.last_term
            or (req.last_log_term == self.log.last_term
                and req.last_log_index >= self.log.last_index)
        )
        if req.preval:
            # §9.6 pre-vote: term untouched; refuse if a live leader is
            # still delivering heartbeats, or the candidate is stale
            leader_alive = (self._now() - self._last_heartbeat) < self._lo
            grant = up_to_date and not leader_alive and req.term > self.current_term
            return RequestVoteReply(self.current_term, grant)
        if req.term < self.current_term:
            return RequestVoteReply(self.current_term, False)
        if req.term > self.current_term:
            self._become_follower(req.term)
        may_vote = self.voted_for in (None, req.candidate_id)
        if up_to_date and may_vote:
            self.voted_for = req.candidate_id
            self._persist_state()
            self._last_heartbeat = self._now()
            return RequestVoteReply(self.current_term, True)
        return RequestVoteReply(self.current_term, False)

    async def _on_append_entries(self, req: AppendEntries) -> AppendEntriesReply:
        if req.term < self.current_term:
            return AppendEntriesReply(self.current_term, False, self.log.last_index)
        if req.term > self.current_term or self.state is not State.FOLLOWER:
            self._become_follower(req.term)  # persists term + clears vote
        self.leader_id = req.leader_id
        self._last_heartbeat = self._now()

        # consistency check
        if req.prev_log_index > 0:
            if req.prev_log_index > self.log.last_index:
                return AppendEntriesReply(self.current_term, False, self.log.last_index)
            if self.log.term_at(req.prev_log_index) != req.prev_log_term:
                # delete conflicting entry and everything after it
                self.log.truncate_suffix(req.prev_log_index)
                return AppendEntriesReply(self.current_term, False,
                                          self.log.last_index)

        # append new entries, deleting conflicts first (§5.3)
        idx = req.prev_log_index
        for raw in req.entries:
            idx += 1
            entry = LogEntry(term=raw["term"], command=raw["command"])
            if idx <= self.log.last_index:
                if self.log.term_at(idx) != entry.term:
                    self.log.truncate_suffix(idx)
                    self.log.append([entry])
            else:
                self.log.append([entry])

        if req.leader_commit > self.commit_index:
            self.commit_index = min(req.leader_commit, self.log.last_index)
            async with self._apply_cv:
                self._apply_cv.notify_all()
        return AppendEntriesReply(self.current_term, True, self.log.last_index)

    # ------------------------------------------------------------------ #
    # state machine application
    # ------------------------------------------------------------------ #

    async def _applier_loop(self) -> None:
        while True:
            async with self._apply_cv:
                await self._apply_cv.wait_for(
                    lambda: self.last_applied < self.commit_index)
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log.entry(self.last_applied)
                result = self.kv.apply(entry.command)
                self._applied_results[self.last_applied] = result
                if self.on_applied:
                    self.on_applied(self.last_applied, entry)
            # bound memory once nobody can still be waiting on old results
            if len(self._applied_results) > 4096:
                for old in list(self._applied_results)[:-2048]:
                    self._applied_results.pop(old, None)
            async with self._apply_cv:
                self._apply_cv.notify_all()

    # ------------------------------------------------------------------ #
    # transport
    # ------------------------------------------------------------------ #

    async def _rpc(self, peer_id: str, msg: Any, timeout: float = 2.0) -> Any:
        host, port = self.peers[peer_id]
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout)
        try:
            writer.write(messages.encode(msg))
            await writer.drain()
            buf = b""
            async def _read_frame() -> Any:
                nonlocal buf
                while True:
                    chunk = await reader.read(65536)
                    if not chunk:
                        return None
                    buf += chunk
                    reply, buf = messages.decode(buf)
                    if reply is not None:
                        return reply
            return await asyncio.wait_for(_read_frame(), timeout)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    # ------------------------------------------------------------------ #
    # persistence
    # ------------------------------------------------------------------ #

    def _persist_state(self) -> None:
        if not self._state_path:
            return
        tmp = self._state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"current_term": self.current_term,
                       "voted_for": self.voted_for}, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._state_path)

    def _load_persistent_state(self) -> None:
        if self._state_path and os.path.exists(self._state_path):
            with open(self._state_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            self.current_term = data["current_term"]
            self.voted_for = data["voted_for"]

    # informational
    def status(self) -> dict:
        return {
            "id": self.id,
            "state": self.state.value,
            "term": self.current_term,
            "leader": self.leader_id,
            "log_len": self.log.last_index,
            "commit_index": self.commit_index,
            "last_applied": self.last_applied,
            "kv_size": len(self.kv),
        }
