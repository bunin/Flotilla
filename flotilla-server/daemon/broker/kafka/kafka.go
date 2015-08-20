package kafka

import (
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

const topic = "test"

// Peer implements the peer interface for Kafka.
type Peer struct {
	client   sarama.Client
	producer sarama.SyncProducer
	consumer sarama.PartitionConsumer
	send     chan []byte
	errors   chan error
	done     chan bool
}

// NewPeer creates and returns a new Peer for communicating with Kafka.
func NewPeer(host string) (*Peer, error) {
	sarama.Logger = log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)

	host = strings.Split(host, ":")[0] + ":9092"
	config := sarama.NewConfig()
	config.Consumer.Fetch.Default = 10 * 1024 * 1024
	client, err := sarama.NewClient([]string{host}, config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &Peer{
		client:   client,
		producer: producer,
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (k *Peer) Subscribe() error {
	consumer, err := sarama.NewConsumerFromClient(k.client)
	if err != nil {
		return err
	}
	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if nil != err {
		return err
	}
	k.consumer = pc
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (k *Peer) Recv() ([]byte, error) {
	select {
	case event := <-k.consumer.Messages():
		return event.Value, nil
	case err := <-k.consumer.Errors():
		return nil, err
	}
	return []byte{}, nil
}

// Send returns a channel on which messages can be sent for publishing.
func (k *Peer) Send() chan<- []byte {
	return k.send
}

// Errors returns the channel on which the peer sends publish errors.
func (k *Peer) Errors() <-chan error {
	return k.errors
}

// Done signals to the peer that message publishing has completed.
func (k *Peer) Done() {
	k.done <- true
}

// Setup prepares the peer for testing.
func (k *Peer) Setup() {
	go func() {
		for {
			select {
			case msg := <-k.send:
				if err := k.sendMessage(msg); err != nil {
					k.errors <- err
				}
			case <-k.done:
				return
			}
		}
	}()
}

func (k *Peer) sendMessage(message []byte) error {
	msg := &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.ByteEncoder(message)}
	_, _, err := k.producer.SendMessage(msg)
	if nil != err {
		return err
	}
	return nil
}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (k *Peer) Teardown() {
	k.producer.Close()
	if k.consumer != nil {
		k.consumer.Close()
	}
	k.client.Close()
}
