package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"gocache/internal/server"
	"gocache/internal/store"
)

func main() {
	// Define dynamic parameter injection inputs via command line flags
	nodeID := flag.String("id", "node-1", "Unique identifier label for this cluster member")
	listenAddr := flag.String("addr", "127.0.0.1:6379", "The local address connection string to bind onto")
	peersList := flag.String("peers", "127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381", "Comma-separated list of all nodes in the topology cluster map")
	flag.Parse()

	// Explode comma-separated strings into a clean array slice
	clusterPeers := strings.Split(*peersList, ",")

	// 1. Storage instantiation
	db := store.NewEngine(5 * time.Second)

	// 2. Initialize cluster group state trackers
	cm := server.NewClusterManager(*nodeID, *listenAddr, clusterPeers)

	// 3. Assemble and launch network server module (Keep existing code above this line)
	srv := server.NewServer(*listenAddr, db, cm)

	// ACTIVATE RAFT CONSENSUS: Let the cluster automatically discover the leader dynamically
	fmt.Println("[Init] Activating automated Raft Consensus loop engine...")
	cm.StartRaftTicker()

	// 4. Start the server's main accept loop (blocks indefinitely)

	err := srv.Start()
	if err != nil {
		log.Fatalf("Critical system clustering initialization failure: %v", err)
	}
}
