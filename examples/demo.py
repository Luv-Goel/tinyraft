"""End-to-end demo: 3-node cluster, writes, leader failover, catch-up.

Run:  python examples/demo.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tinyraft.cluster import Cluster
from tinyraft.client import Client


def banner(text: str) -> None:
    print(f"\n{'=' * 12}  {text}  {'=' * 12}")


def show(cluster: Cluster, note: str = "") -> None:
    print(f"{'node':<6}{'state':<10}{'term':<6}{'leader':<8}{'log':<6}{'kv-size':<8}")
    for n in cluster.live():
        s = n.status()
        print(f"{s['id']:<6}{s['state']:<10}{s['term']:<6}"
              f"{str(s['leader']):<8}{s['log_len']:<6}{s['kv_size']:<8}")
    if note:
        print(f"  -> {note}")


async def main() -> None:
    banner("BOOT: starting 3-node Raft cluster on localhost")
    cluster = Cluster(3)
    await cluster.start()

    banner("ELECTION")
    leader_id, leader = await cluster.wait_for_leader()
    show(cluster, f"leader elected: {leader_id} (term {leader.current_term})")

    banner("WRITES: replicated to a majority before ack")
    client = Client({n.id: n for n in cluster.live()})
    await client.set("lang", "python")
    await client.set("consensus", "raft")
    await client.cas("consensus", "raft", "raft+paxos?-no-just-raft")
    last = leader.log.last_index
    for nid in cluster.ids:
        await cluster.wait_until_applied(nid, last)
    show(cluster, "3 entries committed and applied everywhere")
    print("  get 'consensus' =", await client.get("consensus"))

    banner(f"FAILOVER: killing the leader ({leader_id})")
    await cluster.crash(leader_id)
    new_id, new_leader = await cluster.wait_for_leader()
    show(cluster, f"new leader: {new_id} (term {new_leader.current_term})")
    print("  committed data survived:")
    print("  get 'lang'      =", await new_leader.read("lang"))
    print("  get 'consensus' =", await new_leader.read("consensus"))

    banner(f"RECOVERY: restarting {leader_id}, it catches up")
    await cluster.restart(leader_id)
    last = new_leader.log.last_index
    await cluster.wait_until_applied(leader_id, last)
    show(cluster, "restarted node has the full log again")

    await cluster.stop()
    print("\ntinyraft demo complete.")


if __name__ == "__main__":
    asyncio.run(main())
