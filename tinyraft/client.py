"""KV client that transparently follows the current Raft leader."""

from __future__ import annotations

import asyncio
from typing import Optional

from .node import RaftNode, NotLeaderError


class Client:
    """Wraps the in-process cluster nodes; real deployments would use TCP."""

    def __init__(self, nodes: dict[str, RaftNode]):
        self._nodes = nodes

    async def set(self, key: str, value: str, retries: int = 40) -> str:
        return await self._submit({"op": "set", "key": key, "value": value},
                                  retries)

    async def get(self, key: str, retries: int = 40) -> Optional[str]:
        return await self._submit({"op": "get", "key": key}, retries)

    async def delete(self, key: str, retries: int = 40) -> bool:
        return await self._submit({"op": "delete", "key": key}, retries)

    async def cas(self, key: str, expect: Optional[str], value: str,
                  retries: int = 40) -> bool:
        return await self._submit(
            {"op": "cas", "key": key, "expect": expect, "value": value},
            retries)

    async def _submit(self, command: dict, retries: int) -> object:
        last_err: Optional[Exception] = None
        for _ in range(retries):
            for nid, node in self._nodes.items():
                if node is None:
                    continue
                try:
                    return await node.client_command(dict(command))
                except (NotLeaderError, TimeoutError, asyncio.TimeoutError,
                        ConnectionError, OSError) as e:
                    last_err = e
                    continue
            await asyncio.sleep(0.1)  # between full sweeps over the nodes
        raise RuntimeError(f"no leader accepted the command: {last_err}")
