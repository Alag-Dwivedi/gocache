Create a brand new directory on your computer named gocache. Inside that directory, initialize your new module and create the folders for our clean architecture:

Our project layout for Phase 1 will look like this:textgocache/
├── go.mod
├── cmd/
│   └── server/
│       └── main.go       # Boots and runs the database
└── internal/
    └── store/
        └── engine.go     # Core memory maps and TTL janitor


 Step 1.2: Coding the Core Storage EngineCreate a new file named internal/store/engine.go [1]. Paste the following code into it [1].We will use a sync.RWMutex to protect a standard Go map [1]. Go maps are not thread-safe by default [1]; if two goroutines try to write to a map at the exact same time, the program will crash with a fatal error [1]. Our mutex prevents this [1].



 Step 1.3: Creating the Test Main Execution LoopCreate a new file named cmd/server/main.go [1]. Paste this code to simulate writes, reads, and watch the background TTL janitor automatically kick into action [1]