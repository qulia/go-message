package message

import (
	"github.com/qulia/go-log/log"
	"github.com/streadway/amqp"
)

// ReceiveNamedQueueManager Deals with RabbitMqQueue connection details
type ReceiveNamedQueueManager struct {
	namedQueueManager *NamedQueueManager
	autoAck           bool
	msgs              <-chan amqp.Delivery
}

// Receive is used to receive messages
// Runs until is set
// Calls onReceive on each message
func (rnqm *ReceiveNamedQueueManager) Receive(onReceive func([]byte) error) {
	for msg := range rnqm.msgs {
		log.V("Received a message on queue %s with length %d\n",
			rnqm.namedQueueManager.queue.Name, len(msg.Body))
		go func(delivery amqp.Delivery) {
			// TODO: For now, serialize per message
			//  add support fan-out/fan-in for single message processing
			err := onReceive(delivery.Body)
			if !rnqm.autoAck {
				if err == nil {
					err := delivery.Ack(false) // TODO: ACK does not work with *amqp.Delivery
					log.E(err, "Cannot ACK the message\n")
				} else {
					err := delivery.Nack(false, true /*requeue*/)
					log.E(err, "Cannot NACK the message\n")
				}
			}
		}(msg)
	}
}

// NewReceiveNamedQueueManager Create new queuemanager for sending and receiving data
func NewReceiveNamedQueueManager(serverAddress, queueName string, autoAck bool) (*ReceiveNamedQueueManager, error) {
	rnqm := new(ReceiveNamedQueueManager)
	rnqm.autoAck = autoAck
	nqm, err := NewNamedQueueManager(serverAddress, queueName)
	if err != nil {
		return nil, err
	}
	rnqm.namedQueueManager = nqm
	msgs, err := nqm.channel.Consume(
		nqm.queue.Name, // queue
		"",             // consumer
		rnqm.autoAck,   // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		log.E(err, "Cannot open channel for read %s\n", nqm.queue.Name)
		return nil, err
	}
	rnqm.msgs = msgs
	return rnqm, nil
}

// Close the queue manager
func (rnqm *ReceiveNamedQueueManager) Close() error {
	return rnqm.namedQueueManager.Close()
}

// GetCount of the queue
func (rnqm *ReceiveNamedQueueManager) GetCount() int {
	return rnqm.namedQueueManager.GetCount()
}
