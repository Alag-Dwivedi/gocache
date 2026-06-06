Phase 5: The Raft Consensus Algorithm (Automated Leader Elections) [skills:load: stem-calculative-problem-solving].
Right now, if Node 1 crashes, the cluster becomes brainless. There is no leader to accept write commands or replicate them. In this final phase, we will implement a simplified version of the Raft consensus model.

The Core Mechanics of Raft Elections
Nodes have three states: Follower, Candidate, or Leader.
Heartbeats as a Lifeline: The Leader continually broadcasts heartbeats. Every follower maintains a randomized Election Timeout (e.g., between 150ms and 300ms).
Triggering an Election: If a Follower stops hearing from the Leader before its timeout expires, it assumes the Leader is dead. It increments its current logical time unit (called a Term), switches its status to Candidate, votes for itself, and requests votes from its peers.Winning Quorum: If a Candidate receives a majority of votes from the active cluster (called a Quorum), it is officially crowned the new Leader and starts broadcasting heartbeats to assert dominance.

 Step 5.1: Upgrading Cluster Manager with Raft States


Step 5.2: Updating Server Interceptions for Consensus SignalsNow we need to update our router engine inside internal/server/server.go. It needs to intercept our internal Raft commands (REQUEST_VOTE and APPEND_ENTRIES) so the nodes can interact natively over the TCP socket plane.

Step 5.3: Activating the Raft Engine on StartupOpen cmd/server/main.go and remove the static, hardcoded leader assignment lines from Phase 4. Instead, call our new automated consensus discovery loops right before the server starts.