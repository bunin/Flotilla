package daemon

import (
	"encoding/binary"
	"fmt"
	"time"
)

type publisher struct {
	peer
	id          int
	numMessages int
	messageSize int64
	test        test
	results     chan *result
}

func (p *publisher) start() {
	switch p.test {
	case throughput:
		p.testThroughput()
	case latency:
		p.testLatency()
	default:
		panic(fmt.Sprintf("Invalid test: %s", p.test))
	}
}

func (p *publisher) testThroughput() {
	message := make([]byte, p.messageSize)
	start := time.Now().UnixNano()
	for i := 0; i < p.numMessages; i++ {
		p.Send(message)
	}
	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	p.results <- &result{
		Duration:   ms,
		Throughput: 1000 * float32(p.numMessages) / ms,
	}
}

func (p *publisher) testLatency() {
	message := make([]byte, 9)
	start := time.Now().UnixNano()
	for i := 0; i < p.numMessages; i++ {
		binary.PutVarint(message, time.Now().UnixNano())
		p.Send(message)
	}
	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	p.results <- &result{
		Duration:   ms,
		Throughput: 1000 * float32(p.numMessages) / ms,
	}
}