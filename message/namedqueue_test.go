// Integration and benchmark tests for queue manager operations
package message

import (
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/qulia/go-log/log"
)

var (
	serverAddress string
)

func TestNamedQueueManualAckSuccess(t *testing.T) {
	err := setup(t.Name(), false)
	if err != nil {
		t.Error(err)
	}
	nqr, _ := NewReceiveNamedQueueManager(serverAddress, t.Name(), false)
	nqs, _ := NewSendNamedQueueManager(serverAddress, t.Name())
	receive := make(chan bool)
	receiveComplete := make(chan bool)
	// Run receiver with success processing
	runReceiver(nqr, receive, receiveComplete, nil)
	err = sendOneAndWait(nqs, nqr, receive, receiveComplete, 0)
	if err != nil {
		t.Error(err)
	}
}

func TestNamedQueueManualAckFail(t *testing.T) {
	err := setup(t.Name(), false)
	if err != nil {
		t.Error(err)
	}

	nqr, _ := NewReceiveNamedQueueManager(serverAddress, t.Name(), false)
	nqs, _ := NewSendNamedQueueManager(serverAddress, t.Name())
	receive := make(chan bool)
	receiveComplete := make(chan bool)
	// Run receiver with failure processing
	runReceiver(nqr, receive, receiveComplete, errors.New("failed to process message"))
	err = sendOneAndWait(nqs, nqr, receive, receiveComplete, 1)
	if err != nil {
		t.Error(err)
	}
}

func TestNamedQueueAutoAckFail(t *testing.T) {
	err := setup(t.Name(), false)
	if err != nil {
		t.Error(err)
	}

	nqr, _ := NewReceiveNamedQueueManager(serverAddress, t.Name(), true)
	nqs, _ := NewSendNamedQueueManager(serverAddress, t.Name())
	receive := make(chan bool)
	receiveComplete := make(chan bool)
	// Run receiver with failure processing
	runReceiver(nqr, receive, receiveComplete, errors.New("failed to process message"))
	err = sendOneAndWait(nqs, nqr, receive, receiveComplete, 0)
	if err != nil {
		t.Error(err)
	}
}

/*
.com/qulia/go-message/message/namedqueue_test.go#111: GOMAXPROCS: 4
goos: linux
goarch: amd64
pkg: github.com/qulia/go-message/message
BenchmarkNamedQueueManager_SendReceive-4   	    5000	    355913 ns/op
PASS
*/
func BenchmarkNamedQueueManager_SendReceive(b *testing.B) {
	err := setup(b.Name(), true)
	if err != nil {
		b.Error(err)
	}

	nqr, _ := NewReceiveNamedQueueManager(serverAddress, b.Name(), false)
	nqs, _ := NewSendNamedQueueManager(serverAddress, b.Name())

	receive := make(chan bool)
	receiveComplete := make(chan bool)
	// Run receiver
	runReceiver(nqr, receive, receiveComplete, nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := nqs.Send([]byte(""))
		if err != nil {
			b.Error("Could not send message")
		}
		<-receive
	}
	log.V("Shutting down\n")

	b.StopTimer()
	err = nqr.Close()
	if err != nil {
		b.Error("Could not close")
	}

	err = nqs.Close()
	if err != nil {
		b.Error("Could not close")
	}
	<-receiveComplete
	waitExpectedCount(nqr.namedQueueManager.queue.Name, 0)
	b.StartTimer()
}

func setup(name string, benchmark bool) error {
	log.V("CPU count: %d\n", runtime.NumCPU())
	log.V("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))

	serverAddress = "amqp://guest:guest@localhost:5672/"
	if benchmark {
		log.SetLevel(log.None)
	} else {
		log.SetLevel(log.Verbose)
	}

	return drain(name)
}

func drain(queueName string) error {
	nq, _ := NewReceiveNamedQueueManager(serverAddress, queueName, true)
	receiveComplete := make(chan bool)
	// Run receiver
	go func() {
		nq.Receive(func(msg []byte) error {
			return nil
		})
		receiveComplete <- true
	}()
	waitExpectedCount(nq.namedQueueManager.queue.Name, 0)
	err := nq.Close()
	if err != nil {
		return nil
	}
	<-receiveComplete
	return nil
}

func sendOneAndWait(
	nqs *SendNamedQueueManager,
	nqr *ReceiveNamedQueueManager,
	receive chan bool,
	receiveComplete chan bool,
	expectedCount int) error {
	err := nqs.Send([]byte(""))
	if err != nil {
		return err
	}
	<-receive
	log.V("Shutting down\n")
	err = nqs.Close()
	if err != nil {
		return err
	}

	err = nqr.Close()
	if err != nil {
		return err
	}
	<-receiveComplete

	waitExpectedCount(nqr.namedQueueManager.queue.Name, expectedCount)

	return nil
}

func waitExpectedCount(queueName string, expectedCount int) {
	nq, _ := newNamedQueueManager(serverAddress, queueName)
	for {
		count := nq.getCount()
		log.V("Current/expected count %d/%d \n", count, expectedCount)
		if count == expectedCount {
			break
		}
		time.Sleep(time.Second)
	}
}

func runReceiver(
	nq *ReceiveNamedQueueManager,
	notifyReceive chan bool,
	notifyComplete chan bool,
	errOnReceive error) {
	go func() {
		nq.Receive(func(msg []byte) error {
			if notifyReceive != nil {
				notifyReceive <- true
			}
			return errOnReceive
		})
		if notifyComplete != nil {
			notifyComplete <- true
		}
	}()
}
