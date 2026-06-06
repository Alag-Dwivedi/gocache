Phase 4: Active Replication (Leader/Follower Sync).
Right now, each database node is isolated. If you write data via SET to Node 1, Node 2 knows nothing about it. In this phase, we will configure Active Command Mirroring. We will designate Node 1 as the Leader, meaning whenever it executes a data modification request (SET or DEL), it will instantly replicate that command payload out to all active Follower nodes.

Step 4.1: Enhancing Cluster Manager for Command Broadcasting
Open internal/server/cluster.go and append the following features to your ClusterManager struct. This will allow the Leader node to broadcast commands over the network to any active peer:

// SetLeaderStatus explicitly toggles whether this specific node handles master data distribution writes.
func (cm *ClusterManager) SetLeaderStatus(isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.isLeader = isLeader
	if isLeader {
		fmt.Printf("[Cluster Manager] Node %s is acting as cluster LEADER.\n", cm.NodeID)
	} else {
		fmt.Printf("[Cluster Manager] Node %s is acting as cluster FOLLOWER.\n", cm.NodeID)
	}
}

// IsLeader exposes our current state safely across threads.
func (cm *ClusterManager) IsLeader() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.isLeader
}

// BroadcastWrite sends an identical operational command down to all alive follower sockets.
func (cm *ClusterManager) BroadcastWrite(cmd string) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	// Append safety newline padding if it's missing from the payload string
	if !strings.HasSuffix(cmd, "\n") {
		cmd += "\n"
	}

	for _, peer := range cm.Peers {
		if !peer.IsAlive {
			continue // Skip dead nodes to avoid clogging the network pipeline
		}

		// Fire-and-forget replication routine inside an isolated background thread context
		go func(addr string, payload string) {
			conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
			if err != nil {
				return
			}
			defer conn.Close()

			// Transmit replicated command directly over the wire
			_, _ = conn.Write([]byte(payload))
		}(peer.Addr, cmd)
	}
}


Step 4.2: Intercepting Writes in the Server Handler
Now we need to update our request processor inside internal/server/server.go. If a client sends a SET or DEL write command:If we are a Follower: We accept the write, but do not broadcast it (preventing endless infinite loops).If we are the Leader: We save it to our own memory, then call BroadcastWrite to sync it to our peers.