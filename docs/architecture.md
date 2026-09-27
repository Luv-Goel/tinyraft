# Architecture

TinyRaft is designed to be minimal but entirely faithful to the Raft consensus algorithm.

## Raft Node Architecture

```mermaid
graph TD
    Client[Client] -->|Read/Write Requests| Node1(Leader Node)
    
    subgraph TinyRaft Cluster
        Node1 -->|AppendEntries RPC| Node2(Follower Node)
        Node1 -->|AppendEntries RPC| Node3(Follower Node)
        
        Node2 -->|RequestVote RPC| Node1
        Node3 -->|RequestVote RPC| Node1
    end
    
    Node1 --> WAL1[(Write-Ahead Log)]
    Node2 --> WAL2[(Write-Ahead Log)]
    Node3 --> WAL3[(Write-Ahead Log)]
    
    Node1 -.-> StateMachine1[KV Store]
    Node2 -.-> StateMachine2[KV Store]
    Node3 -.-> StateMachine3[KV Store]
```

## Request Lifecycle

1. **Client Request**: Client sends a `set(key, value)` command to the leader.
2. **Log Append**: Leader appends the command to its local WAL.
3. **Replication**: Leader broadcasts `AppendEntries` to followers.
4. **Follower Append**: Followers validate the term and append to their WALs.
5. **Commit**: Once a majority of followers acknowledge, the leader marks the entry as committed.
6. **Apply**: Leader applies the committed command to the State Machine (KV Store).
7. **Response**: Leader responds to the client with the result.

## Failover Sequence (Pre-Vote)

```mermaid
sequenceDiagram
    participant F as Stale Follower
    participant L as Current Leader
    participant O as Other Follower

    F->>O: PreVote (Term+1)
    note right of F: Stale node awakes,<br/>initiates PreVote
    O-->>F: PreVote Denied
    note over O: Alive leader <br/>heartbeat received recently
    note right of F: Does not bump term,<br/>leader stays undisturbed
```
