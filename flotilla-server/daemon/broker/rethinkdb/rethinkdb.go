package rethinkdb

import (
	"strings"

	r "github.com/dancannon/gorethink"
)

const (
	topic = "test"
	port  = "28015"
)

// Peer implements the peer interface for RethinkDB.
type Peer struct {
	session  *r.Session
	consumer *r.Cursor
	host     string
	messages chan []byte
	send     chan []byte
	errors   chan error
	done     chan bool
}

type rethinkMessage struct {
	ID   string `gorethink:"id,omitempty"`
	Data []byte `gorethink:"data,omitempty"`
}

type changeMessage struct {
	New rethinkMessage `gorethink:"new_val,omitempty"`
	Old rethinkMessage `gorethink:"old_val,omitempty"`
}

// NewPeer creates and returns a new Peer for communicating with RethinkDB.
func NewPeer(host string) (*Peer, error) {
	host = strings.Split(host, ":")[0] + ":" + port
	session, err := r.Connect(r.ConnectOpts{Address: host, Database: topic})
	if err != nil {
		return nil, err
	}

	err = r.DB(topic).TableDrop(topic).Exec(session)
	err = r.DB(topic).TableCreate(topic).Exec(session)
	if err != nil {
		return nil, err
	}

	return &Peer{
		host:     host,
		session:  session,
		messages: make(chan []byte, 10000),
		send:     make(chan []byte),
		errors:   make(chan error, 1),
		done:     make(chan bool),
	}, nil
}

// Subscribe prepares the peer to consume messages.
func (peer *Peer) Subscribe() error {
	conn := peer.session
	cursor, err := r.Table(topic).Changes().Run(conn)
	if nil != err {
		return err
	}

	peer.consumer = cursor
	return nil
}

// Recv returns a single message consumed by the peer. Subscribe must be called
// before this. It returns an error if the receive failed.
func (peer *Peer) Recv() ([]byte, error) {
	var result changeMessage
	peer.consumer.Next(&result)
	return result.New.Data, nil
}

// Send returns a channel on which messages can be sent for publishing.
func (peer *Peer) Send() chan<- []byte {
	return peer.send
}

// Errors returns the channel on which the peer sends publish errors.
func (peer *Peer) Errors() <-chan error {
	return peer.errors
}

// Done signals to the peer that message publishing has completed.
func (peer *Peer) Done() {
	peer.done <- true
}

// Setup prepares the peer for testing.
func (peer *Peer) Setup() {
	go func() {
		for {
			select {
			case msg := <-peer.send:
				message := new(rethinkMessage)
				message.Data = msg
				_, err := r.Table(topic).Insert(message).RunWrite(peer.session)
				if nil != err {
					peer.errors <- err
					return
				}
			case <-peer.done:
				return
			}
		}
	}()

}

// Teardown performs any cleanup logic that needs to be performed after the
// test is complete.
func (peer *Peer) Teardown() {
	if peer.consumer != nil && !peer.consumer.IsNil() {
		peer.consumer.Close()
	}
	peer.session.Close()
}
