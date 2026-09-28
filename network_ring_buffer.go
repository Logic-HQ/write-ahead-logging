package main

import (
	"bufio"
	"io"
	"net"
	"time"
)

type NetworkIngestor struct {
	packetQueue chan []byte
	bufferPool  sync.Pool
}

func NewNetworkIngestor(queueSize int) *NetworkIngestor {
	return &NetworkIngestor{
		packetQueue: make(chan []byte, queueSize),
		// Use a sync.Pool to reuse byte slices, eliminating Garbage Collection (GC) pauses
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 4096) // 4KB network packet buffer size
			},
		},
	}
}

// HandleConnection uses a dedicated ring-buffer strategy per TCP connection
func (n *NetworkIngestor) HandleConnection(conn net.Conn) {
	defer conn.Close()

	// Wrap the network connection in an OS-optimized 64KB buffered stream
	reader := bufio.NewReaderSize(conn, 64*1024)

	for {
		// 1. Borrow an allocated buffer from the memory pool
		buf := n.bufferPool.Get().([]byte)

		// 2. Read bytes into the buffer
		bytesRead, err := reader.Read(buf)
		if err != nil {
			if err != io.EOF {
				// Handle unexpected network errors here
			}
			n.bufferPool.Put(buf)
			break
		}

		// 3. Slice exactly what was read and hand it over to the processing ring channel
		payload := make([]byte, bytesRead)
		copy(payload, buf[:bytesRead])
		
		n.packetQueue <- payload // Sent to application workers
		
		// Return the temporary raw buffer back to the pool immediately
		n.bufferPool.Put(buf)
	}
}
