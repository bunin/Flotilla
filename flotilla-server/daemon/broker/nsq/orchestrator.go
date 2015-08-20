package nsq

import (
	"fmt"
	"log"
	"os/exec"
)

const (
	nsqlookupd      = "nsqio/nsqlookupd"
	nsqlookupName   = "nsqlookupd_test"
	nsqlookupdPort1 = "4160"
	nsqlookupdPort2 = "4161"
	nsqlookupdCmd   = "docker run -d --name %s -p %s:%s -p %s:%s %s"
	nsqd            = "nsqio/nsqd"
	internalPort    = "4150"
	nsqdPort        = "4151"
	nsqdCmd         = `docker run --link %s:%s -d -p %s:%s -p %s:%s %s \
	                       --broadcast-address=%s \
	                       --lookupd-tcp-address=%s:%s`
)

// Broker is an implementation of the broker interface which handles
// orchestrating NSQ.
type Broker struct {
	nsqlookupdContainerID string
	nsqdContainerID       string
}

// Start will start the message broker and prepare it for testing.
func (n *Broker) Start(host, port string) (interface{}, error) {
	if port == nsqlookupdPort1 || port == nsqlookupdPort2 || port == nsqdPort {
		return nil, fmt.Errorf("Port %s is reserved", port)
	}

	cmd := fmt.Sprintf(nsqlookupdCmd, nsqlookupName, nsqlookupdPort1, nsqlookupdPort1, nsqlookupdPort2,
		nsqlookupdPort2, nsqlookupd)
	nsqlookupdContainerID, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s\n%s\n%s", nsqlookupd, err.Error(), nsqlookupdContainerID, cmd)
		return "", err
	}
	log.Printf("Started container %s: %s", nsqlookupd, nsqlookupdContainerID)

	cmd = fmt.Sprintf(nsqdCmd, nsqlookupName, nsqlookupName, port, internalPort, nsqdPort, nsqdPort, nsqd,
		nsqlookupName, nsqlookupName, nsqlookupdPort1)
	nsqdContainerID, err := exec.Command("/bin/sh", "-c", cmd).Output()
	if err != nil {
		log.Printf("Failed to start container %s: %s", nsqd, err.Error())
		return "", err
	}

	log.Printf("Started container %s: %s", nsqd, nsqdContainerID)
	n.nsqlookupdContainerID = string(nsqlookupdContainerID)
	n.nsqdContainerID = string(nsqdContainerID)
	return string(nsqdContainerID), nil
}

// Stop will stop the message broker.
func (n *Broker) Stop() (interface{}, error) {
	_, err := exec.Command("/bin/sh", "-c", fmt.Sprintf("docker rm -f %s", nsqlookupName)).Output()
	if err != nil {
		log.Printf("Failed to stop container %s: %s", nsqlookupd, err.Error())
	} else {
		log.Printf("Stopped container %s: %s", nsqlookupd, n.nsqlookupdContainerID)
	}

	nsqdContainerID, e := exec.Command("/bin/sh", "-c", fmt.Sprintf("docker kill %s", n.nsqdContainerID)).Output()
	if e != nil {
		log.Printf("Failed to stop container %s: %s", nsqd, err.Error())
		err = e
	} else {
		log.Printf("Stopped container %s: %s", nsqd, n.nsqdContainerID)
	}

	return string(nsqdContainerID), err
}
