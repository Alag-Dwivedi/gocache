Phase 3: Cluster Discovery & Health Pings.
In this phase, we move from a single standalone server to a replicated cluster topology. We will update our code so that multiple independent nodes can start up, find each other over the network, and continuously exchange background heartbeats to track who is alive.

Step 3.1: Expanding our Domain for Cluster awarenessTo coordinate a cluster, each node needs to know its own identity, who its peers are, and track the health status of those peers.Create a new file named internal/server/cluster.go

Step 3.2: Integrating Clustering into our ServerWe need to update our core Server struct inside internal/server/server.go so it hosts this cluster coordination engine block.Open internal/server/server.go and modify its struct definition and constructor function 

Step 3.3: Upgrading Main to accept command-line flagsTo simulate a cluster locally, we need to spin up multiple instances of our server on different ports. We will use Go's built-in flag package to supply a distinct node ID, port address, and cluster peer layout at startup.

