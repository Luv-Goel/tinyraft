# API Reference

## `tinyraft.Cluster`

Manages a cluster of `RaftNode`s in the same process. Useful for testing and examples.

### `Cluster(num_nodes)`
Initializes the cluster.

### `await start()`
Starts all nodes and their event loops.

### `await wait_for_leader()`
Wait until a leader is elected in the cluster. Returns `(term, leader_node)`.

## `tinyraft.Client`

Client to interact with the Raft cluster. Tracks the current leader and retries transparently.

### `Client(peers)`
Initializes the client with the cluster nodes.

### `await set(key, value)`
Set a key to a value in the replicated store.

### `await get(key)`
Get the current value of a key with linearizability.

### `await cas(key, expected, new_value)`
Compare and swap a key to a new value if it matches the expected value. Returns `True` if swapped.

## `tinyraft.RaftNode`

The core node implementation.

### `status()`
Returns a dictionary containing the node's current status (state, term, log length, leader, etc).
