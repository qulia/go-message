package message

import (
	"github.com/qulia/go-log/log"
	"github.com/streadway/amqp"
)

// namedQueueManager Deals with RabbitMqQueue connection details
type namedQueueManager struct {
	serverAddress string
	queue         *amqp.Queue
	channel       *amqp.Channel
}

func newNamedQueueManager(serverAddress, queueName string) (*namedQueueManager, error) {
	nqm := new(namedQueueManager)
	nqm.serverAddress = serverAddress
	ch, q, err := getNamedQueue(serverAddress, queueName)
	nqm.channel = ch
	nqm.queue = q
	return nqm, err
}

// GetCount returns number of messages in the queue
func (qm *namedQueueManager) getCount() int {
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
func (qm *namedQueueManager) close() error {
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
