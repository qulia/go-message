package message

import (
	"github.com/qulia/go-log/log"

	"github.com/streadway/amqp"
)

// ReceiveFanoutManager supports receive/send and explicit send
type ReceiveFanoutManager struct {
	receiveChannel *amqp.Channel
	receiveQueue   *amqp.Queue
	onReceive      func(*amqp.Delivery)
}

//NewReceiveFanoutManager creates new manager
func NewReceiveFanoutManager(serverAddress, receiveFanout string,
	onReceive func(*amqp.Delivery)) *ReceiveFanoutManager {

	rfm := new(ReceiveFanoutManager)
	rfm.onReceive = onReceive
	conn, err := amqp.Dial(serverAddress)
	log.F(err, "Failed to connect to RabbitMQ\n")

	ch, err := conn.Channel()
	log.F(err, "Failed to open a channel\n")
	err = ch.ExchangeDeclare(
		receiveFanout,
		"fanout", //kind string,
		false,    //durable bool,
		false,    //autoDelete bool,
		false,    //internal bool,
		false,    //noWait bool,
		nil)      //args amqp.Table)
	log.F(err, "Failed to declare exchange\n")
	rfm.receiveChannel = ch
	q, err := ch.QueueDeclare("", false, true /*autoDelete*/, false, false, nil)
	log.F(err, "Failed to get queue\n")
	err = ch.QueueBind(q.Name, "", receiveFanout, false, nil)
	log.F(err, "Failed to bind to queue %s\n", q.Name)
	rfm.receiveQueue = &q

	go rfm.receive()

	return rfm
}

func (rfm *ReceiveFanoutManager) receive() {
	msgs, _ := rfm.receiveChannel.Consume(
		rfm.receiveQueue.Name, "", true /*autoAck*/, false, false, false, nil)

	for msg := range msgs {
		msg := msg
		log.V("Received message on receive fanout")
		rfm.onReceive(&msg)
	}
}
