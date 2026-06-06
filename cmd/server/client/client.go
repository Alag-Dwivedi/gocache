package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run client.go <port> <command> [args...]")
		fmt.Println("Example: go run client.go 6379 SET mykey myvalue")
		return
	}

	port := os.Args[1]
	address := "127.0.0.1:" + port

	// 1. Connect to the specified node
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Failed to connect to %s: %v\n", address, err)
		return
	}
	defer conn.Close()

	// 2. Build the RESP array exactly how your server expects it
	args := os.Args[2:]
	respCmd := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		respCmd += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	// 3. Send the command
	_, err = conn.Write([]byte(respCmd))
	if err != nil {
		fmt.Println("Failed to send command:", err)
		return
	}

	// 4. Read and print the server's response
	buffer := make([]byte, 1024)

	// Set a timeout so the client never hangs forever
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	n, err := conn.Read(buffer)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			fmt.Println("Server did not respond within 3 seconds (Timeout).")
		} else {
			fmt.Println("Failed to read response:", err)
		}
		return
	}

	fmt.Print("Server replied: ", string(buffer[:n]))
}
