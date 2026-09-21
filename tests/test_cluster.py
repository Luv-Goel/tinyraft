"""Integration tests: leader election, log replication, failover.

These spin up real 3-node clusters on localhost TCP with fast timers.
"""

import asyncio
import unittest

from tinyraft.cluster import Cluster
from tinyraft.client import Client
from tinyraft.node import State


class ClusterTestBase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.cluster = Cluster(3)
        await self.cluster.start()

    async def asyncTearDown(self):
        await self.cluster.stop()


class ElectionTest(ClusterTestBase):
    async def test_single_leader_elected(self):
        leader_id, leader = await self.cluster.wait_for_leader()
        for node in self.cluster.live():
            self.assertEqual(node.leader_id, leader_id)
            self.assertEqual(node.current_term, leader.current_term)

    async def test_leader_reelected_after_crash(self):
        old_id, _ = await self.cluster.wait_for_leader()
        old_term = self.cluster.live()[0].current_term
        await self.cluster.crash(old_id)
        new_id, new_leader = await self.cluster.wait_for_leader()
        self.assertNotEqual(new_id, old_id)
        self.assertGreaterEqual(new_leader.current_term, old_term)

    async def test_no_new_commits_without_quorum(self):
        """An isolated leader (minority partition) cannot commit new entries."""
        leader_id, leader = await self.cluster.wait_for_leader()
        others = [n for n in self.cluster.ids if n != leader_id]
        await self.cluster.crash(others[0])
        await self.cluster.crash(others[1])
        # the isolated node may stay leader, but entries must never commit
        commit_before = leader.commit_index
        with self.assertRaises(TimeoutError):
            await leader.client_command({"op": "set", "key": "x", "value": "y"},
                                        timeout=1.0)
        self.assertEqual(leader.commit_index, commit_before)


class ReplicationTest(ClusterTestBase):
    async def test_write_replicates_to_all_nodes(self):
        _, leader = await self.cluster.wait_for_leader()
        client = Client({n.id: n for n in self.cluster.live()})
        await client.set("k1", "v1")
        # index to wait for: the leader's current tip (>= our write's index)
        index = leader.log.last_index
        for nid in self.cluster.ids:
            await self.cluster.wait_until_applied(nid, index)
        for node in self.cluster.live():
            self.assertEqual(node.kv.apply({"op": "get", "key": "k1"}), "v1")

    async def test_many_writes_converge(self):
        _, leader = await self.cluster.wait_for_leader()
        client = Client({n.id: n for n in self.cluster.live()})
        for i in range(20):
            await client.set(f"key-{i}", f"value-{i}")
        index = self.cluster.live()[0].log.last_index  # noqa: just upper bound
        last = leader.log.last_index
        for nid in self.cluster.ids:
            await self.cluster.wait_until_applied(nid, last)
        logs = [tuple((e.term, tuple(sorted(e.command.items())))
                       for e in n.log)
                for n in self.cluster.live()]
        self.assertTrue(all(l == logs[0] for l in logs),
                        "all replicated logs must be identical")
        for i in range(20):
            self.assertEqual(
                leader.kv.apply({"op": "get", "key": f"key-{i}"}),
                f"value-{i}")


class FailoverTest(ClusterTestBase):
    async def test_committed_data_survives_leader_crash(self):
        leader_id, leader = await self.cluster.wait_for_leader()
        client = Client({n.id: n for n in self.cluster.live()})
        await client.set("motto", "dont-lose-me")
        committed_index = leader.log.last_index
        # ensure at least one follower has it
        await self.cluster.wait_until_applied(
            next(n for n in self.cluster.ids if n != leader_id),
            committed_index)
        await self.cluster.crash(leader_id)
        _, new_leader = await self.cluster.wait_for_leader()
        value = await new_leader.read("motto")
        self.assertEqual(value, "dont-lose-me")

    async def test_restarted_node_catches_up(self):
        leader_id, leader = await self.cluster.wait_for_leader()
        follower = next(n for n in self.cluster.ids if n != leader_id)
        await self.cluster.crash(follower)
        client = Client({leader_id: leader})
        for i in range(5):
            await client.set(f"catchup-{i}", str(i))
        last = leader.log.last_index
        await self.cluster.restart(follower)
        await self.cluster.wait_until_applied(follower, last)
        node = self.cluster.nodes[follower]
        for i in range(5):
            self.assertEqual(
                node.kv.apply({"op": "get", "key": f"catchup-{i}"}), str(i))

    async def test_state_persists_across_restart(self):
        _, leader = await self.cluster.wait_for_leader()
        client = Client({n.id: n for n in self.cluster.live()})
        await client.set("durable", "yes")
        term_before = leader.current_term
        await self.cluster.crash(leader.id)
        await self.cluster.restart(leader.id)
        revived = self.cluster.nodes[leader.id]
        self.assertGreaterEqual(revived.current_term, term_before)
        # durable log must contain the committed write
        cmds = [e.command for e in revived.log]
        self.assertIn({"op": "get", "key": "__noop__"}, cmds)
        self.assertIn({"op": "set", "key": "durable", "value": "yes"}, cmds)


class ClientSemanticsTest(ClusterTestBase):
    async def test_read_your_writes_and_cas(self):
        await self.cluster.wait_for_leader()
        client = Client({n.id: n for n in self.cluster.live()})
        await client.set("counter", "1")
        self.assertEqual(await client.get("counter"), "1")
        self.assertTrue(await client.cas("counter", "1", "2"))
        self.assertFalse(await client.cas("counter", "1", "3"))
        self.assertEqual(await client.get("counter"), "2")
        self.assertTrue(await client.delete("counter"))
        self.assertIsNone(await client.get("counter"))


if __name__ == "__main__":
    unittest.main()
