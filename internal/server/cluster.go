package server

import (
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

type NodeState string

const (
	StateFollower  NodeState = "FOLLOWER"
	StateCandidate NodeState = "CANDIDATE"
	StateLeader    NodeState = "LEADER"
)

type Peer struct {
	Addr    string
	IsAlive bool
}

type ClusterManager struct {
	mu       sync.RWMutex
	NodeID   string
	SelfAddr string
	Peers    map[string]*Peer

	// Raft Consensus Consensus State Data
	CurrentState  NodeState
	CurrentTerm   uint64
	VotedFor      string
	LastHeartbeat time.Time
}

func NewClusterManager(nodeID string, selfAddr string, peerAddrs []string) *ClusterManager {
	peers := make(map[string]*Peer)
	for _, addr := range peerAddrs {
		if addr != selfAddr {
			peers[addr] = &Peer{Addr: addr, IsAlive: false}
		}
	}

	return &ClusterManager{
		NodeID:        nodeID,
		SelfAddr:      selfAddr,
		Peers:         peers,
		CurrentState:  StateFollower,
		CurrentTerm:   0,
		LastHeartbeat: time.Now(),
	}
}

func (cm *ClusterManager) IsLeader() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.CurrentState == StateLeader
}

// ResetElectionTimeout generates a randomized duration to prevent split-vote deadlocks
func (cm *ClusterManager) getElectionTimeout() time.Duration {
	// Raft spec requires randomized intervals typically between 150ms and 300ms
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// StartRaftTicker monitors leader health and triggers elections if timeouts snap
func (cm *ClusterManager) StartRaftTicker() {
	go func() {
		for {
			timeout := cm.getElectionTimeout()
			time.Sleep(timeout)

			cm.mu.Lock()
			if cm.CurrentState != StateLeader && time.Since(cm.LastHeartbeat) > timeout {
				fmt.Printf("[Raft Election] Heartbeat missing! Triggering election for Term %d...\n", cm.CurrentTerm+1)
				cm.startElection()
			}
			cm.mu.Unlock()
		}
	}()

	// Start sending Leader heartbeats in the background as well
	go func() {
		for {
			time.Sleep(50 * time.Millisecond) // Heartbeats are frequent
			cm.mu.Lock()
			if cm.CurrentState == StateLeader {
				cm.broadcastHeartbeat()
			}
			cm.mu.Unlock()
		}
	}()
}

func (cm *ClusterManager) startElection() {
	cm.CurrentState = StateCandidate
	cm.CurrentTerm++
	cm.VotedFor = cm.NodeID
	cm.LastHeartbeat = time.Now()

	votesReceived := 1 // Vote for self
	totalNodes := len(cm.Peers) + 1
	quorumNeeded := (totalNodes / 2) + 1

	var wg sync.WaitGroup
	var voteMu sync.Mutex

	for _, peer := range cm.Peers {
		wg.Add(1)
		go func(p *Peer, term uint64) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", p.Addr, 100*time.Millisecond)
			if err != nil {
				return
			}
			defer conn.Close()

			// Simple custom inline syntax protocol: REQUEST_VOTE <term> <candidate_id>
			fmt.Fprintf(conn, "REQUEST_VOTE %d %s\n", term, cm.NodeID)
			reader := bufio.NewReader(conn)
			resp, err := reader.ReadString('\n')

			if err == nil && strings.Contains(strings.ToUpper(resp), "VOTE_GRANTED") {
				voteMu.Lock()
				votesReceived++
				voteMu.Unlock()
			}
		}(peer, cm.CurrentTerm)
	}

	// Block wait for parallel voting rounds to finish up
	wg.Wait()

	if votesReceived >= quorumNeeded && cm.CurrentState == StateCandidate {
		fmt.Printf("[Raft Election] Winner! Node %s received %d votes. Ascending to LEADER.\n", cm.NodeID, votesReceived)
		cm.CurrentState = StateLeader
	} else {
		fmt.Printf("[Raft Election] Lost or tied election with %d votes. Backing down to FOLLOWER.\n", votesReceived)
		cm.CurrentState = StateFollower
	}
}

func (cm *ClusterManager) broadcastHeartbeat() {
	for _, peer := range cm.Peers {
		go func(addr string, term uint64) {
			conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err != nil {
				return
			}
			defer conn.Close()

			// Heartbeat command: APPEND_ENTRIES <term> <leader_id>
			fmt.Fprintf(conn, "APPEND_ENTRIES %d %s\n", term, cm.NodeID)
		}(peer.Addr, cm.CurrentTerm)
	}
}

// HandleVoteRequest evaluates a candidate's vote eligibility based on term values
func (cm *ClusterManager) HandleVoteRequest(term uint64, candidateID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if term > cm.CurrentTerm {
		cm.CurrentTerm = term
		cm.CurrentState = StateFollower
		cm.VotedFor = candidateID
		cm.LastHeartbeat = time.Now()
		return true
	}
	return false
}

// ResetTimeoutFromLeader updates local tracking markers upon receiving a legitimate heartbeat
func (cm *ClusterManager) ResetTimeoutFromLeader(term uint64) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if term >= cm.CurrentTerm {
		cm.CurrentTerm = term
		cm.CurrentState = StateFollower
		cm.LastHeartbeat = time.Now()
	}
}

// BroadcastWrite remains exactly as we wrote it in Phase 4
func (cm *ClusterManager) BroadcastWrite(cmd string) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if !strings.HasSuffix(cmd, "\n") {
		cmd += "\n"
	}
	for _, peer := range cm.Peers {
		go func(addr string, payload string) {
			conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = conn.Write([]byte(payload))
		}(peer.Addr, cmd)
	}
}
