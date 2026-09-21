"""Test/demo harness: run a multi-node Raft cluster in one process.

Each node binds a real localhost TCP port, so this exercises the actual
transport — not a mocked network. Nodes can be crashed and restarted
(with the same data dir) to exercise crash-recovery.
"""

from __future__ import annotations

import asyncio
import socket
import tempfile
from pathlib import Path
from typing import Optional

from .node import RaftNode, NotLeaderError, State


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Cluster:
    def __init__(self, size: int = 3, data_dir: Optional[str] = None):
        self.size = size
        self.ids = [f"n{i}" for i in range(size)]
        self.ports = {nid: free_port() for nid in self.ids}
        self._base = Path(data_dir) if data_dir else Path(
            tempfile.mkdtemp(prefix="tinyraft-cluster-"))
        self.nodes: dict[str, Optional[RaftNode]] = {}
        self._started_once = False

    async def start(self) -> None:
        for nid in self.ids:
            await self._start_one(nid)
        self._started_once = True

    def _make_node(self, nid: str) -> RaftNode:
        peers = {
            other: ("127.0.0.1", self.ports[other])
            for other in self.ids if other != nid
        }
        return RaftNode(
            nid, peers,
            data_dir=str(self._base / nid),
            election_timeout=(0.10, 0.30),
            heartbeat_interval=0.02,
        )

    async def _start_one(self, nid: str) -> None:
        node = self._make_node(nid)
        self.nodes[nid] = node
        await node.start("127.0.0.1", self.ports[nid])

    async def crash(self, nid: str) -> None:
        node = self.nodes.get(nid)
        if node is not None:
            await node.stop()
            self.nodes[nid] = None

    async def restart(self, nid: str) -> None:
        assert self.nodes.get(nid) is None
        await self._start_one(nid)

    async def stop(self) -> None:
        for nid in list(self.nodes):
            await self.crash(nid)

    def live(self) -> list[RaftNode]:
        return [n for n in self.nodes.values() if n is not None]

    async def wait_for_leader(self, timeout: float = 10.0
                              ) -> tuple[str, RaftNode]:
        """Wait until every live node agrees on a single leader."""
        async def _poll() -> tuple[str, RaftNode]:
            while True:
                live = self.live()
                leaders = [n for n in live if n.state is State.LEADER]
                if leaders and all(n.leader_id == leaders[0].id for n in live):
                    return leaders[0].id, leaders[0]
                await asyncio.sleep(0.02)
        return await asyncio.wait_for(_poll(), timeout)

    async def wait_until_applied(self, nid: str, index: int,
                                 timeout: float = 10.0) -> None:
        async def _poll() -> None:
            while True:
                node = self.nodes.get(nid)
                if node is not None and node.last_applied >= index:
                    return
                await asyncio.sleep(0.02)
        await asyncio.wait_for(_poll(), timeout)
