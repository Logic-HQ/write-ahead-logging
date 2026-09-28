package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

// DataPayload represents your high-throughput application event or metric
type DataPayload struct {
	Key       string
	Value     []byte
	Timestamp int64
}

type HighThroughputEngine struct {
	ringBuffer chan DataPayload
	walFile    *os.File
	writer     *bufio.Writer
	mu         sync.RWMutex
	memTable   map[string]DataPayload // In-memory hot storage
}

func NewEngine(walPath string, bufferSize int) (*HighThroughputEngine, error) {
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &HighThroughputEngine{
		ringBuffer: make(chan DataPayload, bufferSize),
		walFile:    file,
		writer:     bufio.NewWriterSize(file, 4*1024*1024), // 4MB OS Write Buffer
		memTable:   make(map[string]DataPayload),
	}, nil
}

// Write ingests data instantly into RAM without blocking on Disk I/O
func (e *HighThroughputEngine) Write(key string, value []byte) {
	payload := DataPayload{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	// 1. Instantly drop into the fast RAM Ring Buffer
	e.ringBuffer <- payload

	// 2. Optimistically update the in-memory hot storage state
	e.mu.Lock()
	e.memTable[key] = payload
	e.mu.Unlock()
}

// StartFlusher processes writes in batches using single-threaded sequential disk I/O
func (e *HighThroughputEngine) StartFlusher(batchInterval time.Duration) {
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.flushBatch()
		}
	}
}

func (e *HighThroughputEngine) flushBatch() {
	backlog := len(e.ringBuffer)
	if backlog == 0 {
		return
	}

	// Group-commit all items currently waiting in the channel
	for i := 0; i < backlog; i++ {
		payload := <-e.ringBuffer
		// Serialize your payload to the sequential disk buffer
		fmt.Fprintf(e.writer, "%d,%s,%s\n", payload.Timestamp, payload.Key, string(payload.Value))
	}

	// Execute a single, highly efficient sequential write to the file system
	_ = e.writer.Flush()
	_ = e.walFile.Sync() // OS Sync to guarantee physical durability
}
