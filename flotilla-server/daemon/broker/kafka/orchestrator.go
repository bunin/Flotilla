package kafka

import (
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

// Broker implements the broker interface for Kafka.
type Broker struct {
	dockerDir string
}

// Start will start the message broker and prepare it for testing.
func (k *Broker) Start(host, port string) (interface{}, error) {

	_, filename, _, _ := runtime.Caller(1)
	k.dockerDir = filepath.Join(filepath.Dir(filename), "broker", "kafka", "kafka-docker")
	cmd := fmt.Sprintf("cd %s && docker-compose up -d", k.dockerDir)

	log.Printf("Command to start kafka: %s", cmd)

	output, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container: %s", err.Error())
		return "", err
	}
	log.Printf("Started container: %s", output)

	// NOTE: Leader election can take a while. For now, just sleep to try to
	// ensure the cluster is ready. Is there a way to avoid this or make it
	// better?
	time.Sleep(time.Second)
	log.Println("Kafka seems to have started")

	return string(output), nil
}

// Stop will stop the message broker.
func (k *Broker) Stop() (interface{}, error) {
	cmd := fmt.Sprintf("cd %s && docker-compose stop", k.dockerDir)
	result, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to stop container: %s", err.Error())
	} else {
		log.Printf("Stopped container: %s", result)
	}

	return string(result), err
}
