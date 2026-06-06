package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"gocache/internal/protocol"
	"gocache/internal/store"
)

type Server struct {
	db         *store.Engine
	Cluster    *ClusterManager // Added cluster management integration link
	listenAddr string
}

func NewServer(listenAddr string, db *store.Engine, cm *ClusterManager) *Server {
	return &Server{
		db:         db,
		Cluster:    cm,
		listenAddr: listenAddr,
	}
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Printf("[Server] GoCache instance %s running on %s...\n", s.Cluster.NodeID, s.listenAddr)

	// Fire up background discovery health checking loops immediately on port startup
	s.Cluster.StartRaftTicker()

	// 2. LAUNCH THE APPLIER HERE:
	// It will sit quietly in the background waiting for logs to be committed
	s.StartLogApplier()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args, err := protocol.ParseRESPCommand(reader)
		if err != nil {
			return // Disconnect client safely on network error
		}

		fmt.Printf("[Front Door] Parsed args from client: %q\n", args)

		if len(args) == 0 {
			continue
		}

		command := strings.ToUpper(args[0])
		switch command {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))

		case "SET":
			if len(args) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			key := args[1]
			val := args[2]
			var ttl time.Duration = 0

			if len(args) >= 5 && strings.ToUpper(args[3]) == "EX" {
				seconds, err := strconv.Atoi(args[4])
				if err == nil {
					ttl = time.Duration(seconds) * time.Second
				}
			}

			// 1. Reconstruct the raw command for proxying
			rawRespCommand := fmt.Sprintf("*%d\r\n", len(args))
			for _, arg := range args {
				rawRespCommand += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			}

			// 2. PROXY CHECK: Are we a Follower?
			if !s.Cluster.IsLeader() {
				// Forward the raw command to the leader
				s.forwardToLeader(conn, rawRespCommand)

				// CRITICAL FIX: Stop executing local logic and wait for the next client command
				continue
			}

			// 3. LEADER EXECUTION: This only runs if we are the actual Leader
			fmt.Printf("[Leader] Received SET command: key=%s, val=%s\n", key, val)
			cmdString := fmt.Sprintf("SET %s %s", key, val)
			err := s.Cluster.Submit(cmdString, ttl)
			fmt.Println("[Leader] Submit finished. Error:", err)

			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n")) // MUST have \r\n
			} else {
				conn.Write([]byte("+OK\r\n")) // MUST have \r\n
			}

		case "GET":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			val, found := s.db.Get(args[1])
			if !found {
				conn.Write([]byte("$-1\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), string(val))))
			}

		case "DEL":
			if len(args) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'del' command\r\n"))
				continue
			}
			s.db.Delete(args[1])
			conn.Write([]byte("+OK\r\n"))

			// REPLICATION STEP: Sync deletions out to followers
			if s.Cluster.IsLeader() {
				rawBroadcast := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(args[1]), args[1])
				s.Cluster.BroadcastWriteWithAck(rawBroadcast)

			}

		case "APPEND_ENTRIES":
			if len(args) < 2 {
				continue
			}

			// 1. Unmarshal the JSON payload
			var req AppendEntriesArgs
			err := json.Unmarshal([]byte(args[1]), &req)
			if err != nil {
				conn.Write([]byte("-ERR malformed raft payload\r\n"))
				continue
			}

			s.Cluster.mu.Lock()

			// 2. Term Check: Reject old leaders
			if req.Term < s.Cluster.CurrentTerm {
				s.Cluster.mu.Unlock()
				conn.Write([]byte("-ERR stale term\r\n"))
				continue
			}

			// Step down to follower if this is a valid newer/current term
			s.Cluster.CurrentState = StateFollower
			s.Cluster.CurrentTerm = req.Term
			s.Cluster.LeaderID = req.LeaderID
			s.Cluster.LastHeartbeat = time.Now()

			// 3. LOG MATCHING CHECK: Does our log perfectly match the leader's history?
			lastIndex := uint64(len(s.Cluster.Log) - 1)

			if req.PrevLogIndex > lastIndex {
				// We are missing entries! Reject.
				s.Cluster.mu.Unlock()
				conn.Write([]byte("-ERR log mismatch: missing previous entries\r\n"))
				continue
			}

			if s.Cluster.Log[req.PrevLogIndex].Term != req.PrevLogTerm {
				// We have a conflicting entry at this index! Reject.
				s.Cluster.mu.Unlock()
				conn.Write([]byte("-ERR log mismatch: term conflict\r\n"))
				continue
			}

			// 4. Safe to Append! Add any new entries to our local log.
			if len(req.Entries) > 0 {
				s.Cluster.Log = s.Cluster.Log[:req.PrevLogIndex+1]
				s.Cluster.Log = append(s.Cluster.Log, req.Entries...)

				// ---> NEW: Followers must also persist to hard drive <---
				// Note: In a production system, you'd truncate the file to PrevLogIndex here,
				// but for learning, just appending the new valid entries is a great start.
				for _, e := range req.Entries {
					s.Cluster.WalEncoder.Encode(e)
				}
				s.Cluster.WalFile.Sync()
			}

			// 5. Update Commit Index
			if req.LeaderCommit > s.Cluster.CommitIndex {
				// The follower commits up to the Leader's commit index, or its own newest log
				lastNewIndex := uint64(len(s.Cluster.Log) - 1)
				if req.LeaderCommit < lastNewIndex {
					s.Cluster.CommitIndex = req.LeaderCommit
				} else {
					s.Cluster.CommitIndex = lastNewIndex
				}

				// Trigger the background applier to update the memory map
				select {
				case s.Cluster.ApplyCh <- struct{}{}:
				default:
				}
			}

			s.Cluster.mu.Unlock()
			conn.Write([]byte("+OK\r\n")) // Acknowledge success to the leader!
			continue

		case "REQUEST_VOTE":
			if len(args) < 3 {
				continue
			}
			term, _ := strconv.ParseUint(args[1], 10, 64)
			candidateID := args[2]

			if s.Cluster.HandleVoteRequest(term, candidateID) {
				conn.Write([]byte("+VOTE_GRANTED\r\n"))
			} else {
				conn.Write([]byte("-VOTE_REFUSED\r\n"))
			}
			continue

		default:
			conn.Write([]byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", command)))
		}
	}
}

// StartLogApplier runs in the background. When logs are committed,
// it parses them and applies them to the storage engine.
func (s *Server) StartLogApplier() {
	go func() {
		logsAppliedSinceSnapshot := 0 // Track iterations

		for range s.Cluster.ApplyCh {
			s.Cluster.mu.Lock()

			for s.Cluster.LastApplied < s.Cluster.CommitIndex {
				s.Cluster.LastApplied++
				entry := s.Cluster.Log[s.Cluster.LastApplied]
				s.applyCommandToEngine(entry)

				logsAppliedSinceSnapshot++
			}
			s.Cluster.mu.Unlock()

			// TRIGGER COMPACTION IF THRESHOLD REACHED
			if logsAppliedSinceSnapshot >= 100 {
				s.TakeSnapshot()
				logsAppliedSinceSnapshot = 0
			}
		}
	}()
}

// FIX: Dropped the "server." prefix from LogEntry since this file is already in package server
func (s *Server) applyCommandToEngine(entry LogEntry) {
	parts := strings.Fields(entry.Command)

	if len(parts) >= 3 && strings.ToUpper(parts[0]) == "SET" {
		key := parts[1]
		val := parts[2]

		var remainingTTL time.Duration = 0
		if entry.ExpiresAt > 0 {
			// Convert absolute timestamp back to relative time for the engine
			expirationTime := time.Unix(0, entry.ExpiresAt)
			remainingTTL = time.Until(expirationTime)

			// If the key is already expired by the time we apply it, skip saving it
			if remainingTTL <= 0 {
				return
			}
		}

		s.db.Set(key, []byte(val), remainingTTL)

	} else if len(parts) == 2 && strings.ToUpper(parts[0]) == "DEL" {
		s.db.Delete(parts[1])
	}
}

// TakeSnapshot captures the engine state and truncates the WAL
func (s *Server) TakeSnapshot() {
	s.Cluster.mu.Lock()
	defer s.Cluster.mu.Unlock()

	// 1. Grab the exact index and term we are snapshotting at
	lastIncludedIndex := s.Cluster.LastApplied
	lastIncludedTerm := s.Cluster.Log[lastIncludedIndex].Term

	// 2. Export the physical data from the storage engine
	engineState := s.db.ExportState()

	// 3. Create the Snapshot payload
	snapshotFile := fmt.Sprintf("%s.snap", s.Cluster.NodeID)
	file, err := os.Create(snapshotFile) // Overwrites old snapshot
	if err != nil {
		fmt.Printf("[Snapshot] Failed to create snapshot file: %v\n", err)
		return
	}
	defer file.Close()

	// We save the Raft metadata along with the engine state
	snapshotData := struct {
		LastIndex uint64
		LastTerm  uint64
		State     map[string]store.Item
	}{
		LastIndex: lastIncludedIndex,
		LastTerm:  lastIncludedTerm,
		State:     engineState,
	}

	encoder := json.NewEncoder(file)
	encoder.Encode(snapshotData)

	// ---> NEW: Update the cluster offsets <---
	s.Cluster.LastIncludedIndex = lastIncludedIndex
	s.Cluster.LastIncludedTerm = lastIncludedTerm

	// Truncate the in-memory Log
	newLog := make([]LogEntry, 0)

	// The dummy entry at index 0 now represents the snapshot's state
	newLog = append(newLog, LogEntry{
		Term:    lastIncludedTerm,
		Command: "SNAPSHOT_DUMMY",
	})

	// Append any logs that were committed but not yet applied (rare, but possible)
	if lastIncludedIndex < uint64(len(s.Cluster.Log)-1) {
		newLog = append(newLog, s.Cluster.Log[lastIncludedIndex+1:]...)
	}

	s.Cluster.Log = newLog

	// 5. Truncate the physical WAL file and rewrite the remaining logs
	s.Cluster.WalFile.Truncate(0)
	s.Cluster.WalFile.Seek(0, 0)
	for _, entry := range s.Cluster.Log {
		s.Cluster.WalEncoder.Encode(entry)
	}
	s.Cluster.WalFile.Sync()

	fmt.Printf("[Snapshot] Compaction complete up to log index %d\n", lastIncludedIndex)
}

// forwardToLeader acts as a transparent reverse proxy.
// It sends the client's raw command to the leader, and returns the leader's exact response.
func (s *Server) forwardToLeader(clientConn net.Conn, rawCommand string) {
	s.Cluster.mu.RLock()
	leaderID := s.Cluster.LeaderID
	leaderPeer, exists := s.Cluster.Peers[leaderID]
	s.Cluster.mu.RUnlock()

	if !exists || leaderID == "" {
		clientConn.Write([]byte("-ERR cluster is currently holding elections, try again\r\n"))
		return
	}

	// 1. Dial the actual Leader
	fmt.Printf("[Proxy] Attempting to proxy command to leader %s at %s\n", leaderID, leaderPeer.Addr)
	leaderConn, err := net.DialTimeout("tcp", leaderPeer.Addr, 2*time.Second)
	if err != nil {
		clientConn.Write([]byte("-ERR failed to proxy to leader\r\n"))
		return
	}
	defer leaderConn.Close()

	// 2. Forward the raw command
	fmt.Println("[Proxy] Connection established, sending data...")
	_, err = leaderConn.Write([]byte(rawCommand))
	if err != nil {
		clientConn.Write([]byte("-ERR leader connection dropped during proxy\r\n"))
		return
	}

	// 3. Read the response from the leader
	reader := bufio.NewReader(leaderConn)
	response, err := reader.ReadString('\n')
	if err != nil {
		clientConn.Write([]byte("-ERR failed to read leader response\r\n"))
		return
	}

	// 4. Pipe the leader's exact response back to our client!
	clientConn.Write([]byte(response))
}
