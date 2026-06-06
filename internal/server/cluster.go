package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
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

// AppendEntriesArgs is the payload sent from Leader to Followers
type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type Peer struct {
	Addr    string
	IsAlive bool
}

type LogEntry struct {
	Term      uint64
	Command   string // Still holds "SET key val" or "DEL key"
	ExpiresAt int64  // Absolute Unix timestamp in nanoseconds (0 = durable/no expiry)
}

// Update your ClusterManager struct
type ClusterManager struct {
	mu       sync.RWMutex
	NodeID   string
	SelfAddr string
	Peers    map[string]*Peer

	// Raft Consensus State Data
	CurrentState  NodeState
	CurrentTerm   uint64
	VotedFor      string
	LeaderID      string // <--- ADD THIS HERE
	LastHeartbeat time.Time

	// --- NEW: THE RAFT LOG ---
	Log []LogEntry

	// CommitIndex is the highest log entry known to be committed (replicated to majority)
	CommitIndex uint64

	// LastApplied is the highest log entry applied to our local store.Engine
	LastApplied uint64

	// Channel to notify the background worker that new logs are ready to be applied
	ApplyCh chan struct{}

	WalFile    *os.File
	WalEncoder *json.Encoder

	// --- NEW: COMPACTION OFFSETS ---
	// LastIncludedIndex is the Logical Raft Index of the last entry in the snapshot
	LastIncludedIndex uint64
	// LastIncludedTerm is the Term of that last entry
	LastIncludedTerm uint64
}

func NewClusterManager(nodeID string, selfAddr string, peerAddrs []string) *ClusterManager {
	peers := make(map[string]*Peer)
	for _, addr := range peerAddrs {
		if addr != selfAddr {
			peers[addr] = &Peer{Addr: addr, IsAlive: false}
		}
	}
	fileName := fmt.Sprintf("%s.wal", nodeID)
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(fmt.Sprintf("Failed to open WAL file: %v", err))
	}

	return &ClusterManager{
		NodeID:        nodeID,
		SelfAddr:      selfAddr,
		Peers:         peers,
		CurrentState:  StateFollower,
		Log:           []LogEntry{{Term: 0, Command: ""}},
		CurrentTerm:   0,
		LastHeartbeat: time.Now(),
		ApplyCh:       make(chan struct{}, 1),
		WalFile:       file,
		WalEncoder:    json.NewEncoder(file),
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
		cm.LeaderID = cm.NodeID // <--- ADD THIS HERE
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
func (cm *ClusterManager) BroadcastWriteWithAck(cmd string) int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if !strings.HasSuffix(cmd, "\n") {
		cmd += "\n"
	}

	// We start with 1 successful write (ourselves, the leader)
	successfulAcks := 1
	var ackMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range cm.Peers {
		wg.Add(1)
		go func(addr string, payload string) {
			defer wg.Done()

			// Dial with a short timeout. We don't want a slow peer
			// to block the entire client request indefinitely.
			conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err != nil {
				return
			}
			defer conn.Close()

			// Send the command
			_, err = conn.Write([]byte(payload))
			if err != nil {
				return
			}

			// Wait for an acknowledgment from the follower.
			// The follower's server.go must send "+OK\r\n" back!
			reader := bufio.NewReader(conn)
			resp, err := reader.ReadString('\n')

			if err == nil && strings.Contains(strings.ToUpper(resp), "+OK") {
				ackMu.Lock()
				successfulAcks++
				ackMu.Unlock()
			}
		}(peer.Addr, cmd)
	}

	// Wait for all peer requests to either succeed or timeout
	wg.Wait()

	return successfulAcks
}

// Submit appends a command to the leader's log and waits for it to be committed.
// It returns an error if this node is not the leader or if quorum fails.
// Submit appends a command to the leader's log and waits for it to be committed.
// It calculates the absolute TTL and handles the Replicate-then-Commit flow.
func (cm *ClusterManager) Submit(command string, ttl time.Duration) error {
	cm.mu.Lock()
	if cm.CurrentState != StateLeader {
		cm.mu.Unlock()
		return fmt.Errorf("not the leader")
	}

	var expiresAt int64 = 0
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl).UnixNano()
	}

	entry := LogEntry{
		Term:      cm.CurrentTerm,
		Command:   command,
		ExpiresAt: expiresAt,
	}

	cm.Log = append(cm.Log, entry)

	// Write to WAL
	cm.WalEncoder.Encode(entry)
	cm.WalFile.Sync()

	// ---> NEW: Get the true Logical Index for Raft <---
	logicalEntryIndex := cm.lastLogIndex()
	cm.mu.Unlock()

	// 3. Broadcast this specific log entry to followers (AppendEntries)
	fmt.Println("[Raft] Broadcasting to followers for quorum...")
	success := cm.replicateLogToFollowers(logicalEntryIndex)
	fmt.Println("[Raft] Broadcast complete. Quorum reached:", success)

	if !success {
		return fmt.Errorf("failed to reach cluster quorum")
	}

	cm.mu.Lock()
	if logicalEntryIndex > cm.CommitIndex {
		cm.CommitIndex = logicalEntryIndex

		select {
		case cm.ApplyCh <- struct{}{}:
		default:
		}
	}
	cm.mu.Unlock()

	return nil
}

func (cm *ClusterManager) replicateLogToFollowers(logicalEntryIndex uint64) bool {
	cm.mu.RLock()

	// Find where the previous log lives in our Go slice
	physicalPrevIndex := cm.getPhysicalIndex(logicalEntryIndex - 1)

	var prevTerm uint64
	if physicalPrevIndex == 0 {
		// If it's index 0, it's our dummy entry! Use the compacted term.
		prevTerm = cm.LastIncludedTerm
	} else if physicalPrevIndex > 0 {
		// Normal slice access
		prevTerm = cm.Log[physicalPrevIndex].Term
	}

	// Physical index of the NEW entry we just appended
	physicalCurrentIndex := cm.getPhysicalIndex(logicalEntryIndex)

	args := AppendEntriesArgs{
		Term:         cm.CurrentTerm,
		LeaderID:     cm.NodeID,
		PrevLogIndex: logicalEntryIndex - 1, // Logical!
		PrevLogTerm:  prevTerm,
		Entries:      []LogEntry{cm.Log[physicalCurrentIndex]},
		LeaderCommit: cm.CommitIndex,
	}
	cm.mu.RUnlock()

	// Serialize the complex struct to JSON
	payloadBytes, _ := json.Marshal(args)
	payloadStr := string(payloadBytes)

	// Wrap the JSON inside your custom RESP protocol format:
	// Array of 2 elements: [ "APPEND_ENTRIES", "{...json...}" ]
	rawBroadcast := fmt.Sprintf("*2\r\n$14\r\nAPPEND_ENTRIES\r\n$%d\r\n%s\r\n", len(payloadStr), payloadStr)

	// Block and wait for a majority of followers to reply "+OK"
	acks := cm.BroadcastWriteWithAck(rawBroadcast)

	quorumNeeded := (len(cm.Peers) / 2) + 1
	return acks >= quorumNeeded
}

func (cm *ClusterManager) recoverFromWAL() {
	// Rewind the file pointer to the beginning
	cm.WalFile.Seek(0, 0)

	decoder := json.NewDecoder(cm.WalFile)
	loadedCount := 0

	for decoder.More() {
		var entry LogEntry
		if err := decoder.Decode(&entry); err != nil {
			break // Reached end of valid JSON or EOF
		}

		cm.Log = append(cm.Log, entry)

		// Fast-forward our Term to match the latest log we have on disk
		if entry.Term > cm.CurrentTerm {
			cm.CurrentTerm = entry.Term
		}
		loadedCount++
	}

	// Because we just loaded these from disk, we know they were already
	// committed in the past. We can safely set our CommitIndex.
	if loadedCount > 0 {
		cm.CommitIndex = uint64(len(cm.Log) - 1)
		fmt.Printf("[Recovery] Bootstrapped %d logs from disk. Last Term: %d\n", loadedCount, cm.CurrentTerm)
	}
}

// lastLogIndex returns the absolute Logical Raft Index of our newest log
func (cm *ClusterManager) lastLogIndex() uint64 {
	// Physical length of the slice - 1 + our historical offset
	return uint64(len(cm.Log)-1) + cm.LastIncludedIndex
}

// lastLogTerm returns the Term of our newest log
func (cm *ClusterManager) lastLogTerm() uint64 {
	return cm.Log[len(cm.Log)-1].Term
}

// getPhysicalIndex translates a Raft Logical Index to a Go Slice Index.
// It returns -1 if the requested index has already been compacted into a snapshot.
func (cm *ClusterManager) getPhysicalIndex(logicalIndex uint64) int {
	if logicalIndex < cm.LastIncludedIndex {
		return -1 // This index is gone, it only lives in the .snap file now!
	}
	return int(logicalIndex - cm.LastIncludedIndex)
}
