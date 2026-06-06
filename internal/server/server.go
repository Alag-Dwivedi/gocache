package server

import (
	"bufio"
	"fmt"
	"net"
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

			// 1. Save data locally to our own in-memory storage engine
			s.db.Set(key, []byte(val), ttl)
			conn.Write([]byte("+OK\r\n"))

			// 2. REPLICATION STEP: If we are the Leader, broadcast the exact same command to followers
			if s.Cluster.IsLeader() {
				// Reconstruct a clean RESP array packet manually for high performance replication
				rawBroadcast := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
				s.Cluster.BroadcastWrite(rawBroadcast)
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
				s.Cluster.BroadcastWrite(rawBroadcast)
			}

		case "APPEND_ENTRIES":
			if len(args) < 3 {
				continue
			}
			term, _ := strconv.ParseUint(args[1], 10, 64)
			s.Cluster.ResetTimeoutFromLeader(term)
			conn.Write([]byte("+OK\r\n"))
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
