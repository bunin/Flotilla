package rethinkdb

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	rethinkDB    = "rethinkdb"
	rethinkDBCmd = `docker run -d -p 28015:28015 %s`
)

// Broker is an implementation of the broker interface which handles orchestrating RethinkDB.
type Broker struct {
	ContainerID string
}

// Start will start the message broker and prepare it for testing.
func (broker *Broker) Start(host, port string) (interface{}, error) {
	cmd := fmt.Sprintf(rethinkDBCmd, rethinkDB)
	containerID, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s\n%s\n%s", rethinkDB, err.Error(), containerID, cmd)
		return "", err
	}
	log.Printf("Started container %s: %s", rethinkDB, containerID)

	broker.ContainerID = string(containerID)
	return string(containerID), nil
}

// Stop will stop the message broker.
func (broker *Broker) Stop() (interface{}, error) {
	containerID, e := exec.Command("/bin/sh", "-c", fmt.Sprintf("docker kill %s", broker.ContainerID)).Output()
	if e != nil {
		log.Printf("Failed to stop container %s: %s", rethinkDB, e.Error())
	} else {
		log.Printf("Stopped container %s: %s", rethinkDB, broker.ContainerID)
	}

	return string(containerID), e
}
