# gocache
It's an in memory key store

Distributed Replicated Key-Value Store (gocache)A high-availability, low-latency, memory-cached database cluster that manages object lifecycles and elects its own master leadership automatically.text                  [ Multi-Node Cluster Network ]
                                 │
           ┌─────────────────────┼─────────────────────┐
           ▼                     ▼                     ▼
    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │ Node 1 (6379)│      │ Node 2 (6380)│      │ Node 3 (6381)│
    │  [ Leader ]  │ ───► │ [ Follower ] │      │ [ Follower ] │
    │ ── ── ── ──  │      │ ── ── ── ──  │      │ ── ── ── ──  │
    │ Memory Map   │      │ Memory Map   │      │ Memory Map   │
    │ TTL Janitor  │      │ TTL Janitor  │      │ TTL Janitor  │
    └──────────────┘      └──────────────┘      └──────────────┘
           ▲                     ▲                     ▲
           └────────────── (Raft Heartbeats / Votes) ──┘


We will build a thread-safe memory store, wrap it in a custom text protocol server, and then introduce two more nodes to form a true cluster that automatically replicates data.text                  [ Cluster Network Engine ]
                             │
       ┌─────────────────────┼─────────────────────┐
       ▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  Node 1 (C#) │ ───► │  Node 2 (C#) │ ───► │  Node 3 (C#) │
│ [Raft/Repl]  │ ◄─── │ [Raft/Repl]  │ ◄─── │ [Raft/Repl]  │
└──────────────┘      └──────────────┘      └──────────────┘
Use code with caution.Phase 1: The Core Engine & TTL ExpiryConcepts: ConcurrentDictionary, Read/Write Locking, Background Sweeper Threads.Goal: Build an in-memory key-value database that supports SET, GET, and keys that automatically delete themselves after a Time-To-Live (TTL) expires.

Phase 2: The RESP/Custom Network ProtocolConcepts: TcpListener, Async Sockets, Byte Array Parsing.Goal: Turn the database into a server. We can implement a simplified version of Redis's official network protocol (RESP) so you can literally use the official Redis CLI to connect to your database!

Phase 3: Cluster Discovery & Health PingsConcepts: Cluster Topology, Background Heartbeats, Connection State Machines.Goal: Spin up 3 different instances of your database container. Teach them to recognize each other's network addresses and continually ping one another to verify cluster health.

Phase 4: Active Replication (Leader/Follower)Concepts: Replication Logs, Read-Only Slaves, State Synchronization.Goal: When you write a key to Node 1, it acts as the Leader and automatically broadcasts the change down to Node 2 and Node 3 across the internal cluster network.

Phase 5: Raft Elections & Consensus (The Final Level)Concepts: Term Numbers, Quorum Voting, Split-Brain Prevention.Goal: If you manually kill Node 1 (The Leader), Node 2 and Node 3 will automatically detect the failure, hold an emergency election, vote on a new Leader, and keep the database cluster running smoothly without human intervention.

Commands to run
 go run ./cmd/server/ -id=node-3 -addr="127.0.0.1:6381" -peers="127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381"

 To SET the cache
 SET <key> <Value> EX <ttl>

 To GET the value of key in cache
 GET <key>


 To DELETE the value of cache
 DEL <key>