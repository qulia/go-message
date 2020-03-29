package message

import (
	"github.com/qulia/go-log/log"
	"github.com/streadway/amqp"
)

// NamedQueueManager Deals with RabbitMqQueue connection details
type NamedQueueManager struct {
	serverAddress string
	queue         *amqp.Queue
	channel       *amqp.Channel
}

func NewNamedQueueManager(serverAddress, queueName string) (*NamedQueueManager, error) {
	nqm := new(NamedQueueManager)
	nqm.serverAddress = serverAddress
	ch, q, err := getNamedQueue(serverAddress, queueName)
	nqm.channel = ch
	nqm.queue = q
	return nqm, err
}

// GetCount returns number of messages in the queue
func (qm *NamedQueueManager) GetCount() int {
	q, _ := qm.channel.QueueDeclare(
		qm.queue.Name, // server create the queue name if empty
		false,         // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)

	return q.Messages
}
func (qm *NamedQueueManager) Close() error {
	err := qm.channel.Close()
	log.E(err, "Failed closing the channel for %s\n", qm.queue.Name)
	return err
}

func getNamedQueue(serverAddress, queueName string) (*amqp.Channel, *amqp.Queue, error) {
	conn, err := amqp.Dial(serverAddress)
	if err != nil {
		log.E(err, "Failed to connect to RabbitMQ\n")
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.E(err, "Failed to open a channel\n")
		return nil, nil, err
	}

	q, err := ch.QueueDeclare(
		queueName, // server create the queue name if empty
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		log.E(err, "Failed to declare a queue\n")
		return nil, nil, err
	}
	return ch, &q, err
}
