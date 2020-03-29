package message

import (
	"github.com/qulia/go-log/log"
	"github.com/streadway/amqp"
)

// SendNamedQueueManager Deals with RabbitMqQueue connection details
type SendNamedQueueManager struct {
	namedQueueManager *NamedQueueManager
}

// NewSendNamedQueueManager Create new queuemanager for sending and receiving data
func NewSendNamedQueueManager(serverAddress, queueName string) (*SendNamedQueueManager, error) {
	snqm := new(SendNamedQueueManager)
	nqm, err := NewNamedQueueManager(serverAddress, queueName)
	if err != nil {
		return nil, err
	}
	snqm.namedQueueManager = nqm
	return snqm, nil
}

// Send is used to send message
func (snqm *SendNamedQueueManager) Send(msg []byte) error {
	err := snqm.namedQueueManager.channel.Publish(
		"",                                // exchange
		snqm.namedQueueManager.queue.Name, // routing key
		false,                             // mandatory
		false,                             // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		})
	if err != nil {
		log.E(err, "Failed sending message on queue %s with length %d\n", snqm.namedQueueManager.queue.Name, len(msg))
	} else {
		log.V("Sent message on queue %s with length %d\n", snqm.namedQueueManager.queue.Name, len(msg))
	}

	return err
}

// Close the queue manager
func (snqm *SendNamedQueueManager) Close() error {
	return snqm.namedQueueManager.Close()
}
