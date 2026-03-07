package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// Opens N raw TCP connections to a NATS server, does the minimal
// CONNECT handshake + SUB, then idles. Reports when all connected.
func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: %s <host:port> <num_clients>\n", os.Args[0])
		os.Exit(1)
	}
	addr := os.Args[1]
	n, _ := strconv.Atoi(os.Args[2])

	conns := make([]net.Conn, 0, n)
	var mu sync.Mutex

	var wg sync.WaitGroup
	sem := make(chan struct{}, 100) // limit concurrency

	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()

			conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				fmt.Fprintf(os.Stderr, "connect %d: %v\n", id, err)
				return
			}
			conn.SetDeadline(time.Now().Add(10 * time.Second))

			// Read INFO
			buf := make([]byte, 4096)
			conn.Read(buf)

			// Send CONNECT + SUB + PING
			msg := fmt.Sprintf("CONNECT {\"verbose\":false,\"pedantic\":false,\"lang\":\"go\",\"version\":\"1.0\",\"protocol\":1,\"headers\":true,\"no_responders\":true}\r\nSUB idle.%d %d\r\nPING\r\n", id, id)
			conn.Write([]byte(msg))

			// Read PONG
			conn.Read(buf)

			// Clear deadline — idle forever
			conn.SetDeadline(time.Time{})

			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	fmt.Printf("connected: %d / %d\n", len(conns), n)

	// Block until stdin is closed (parent kills us)
	os.Stdin.Read(make([]byte, 1))

	for _, c := range conns {
		c.Close()
	}
}
