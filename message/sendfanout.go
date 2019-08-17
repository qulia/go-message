package message

import (
	"fmt"

	"github.com/qulia/go-log/log"
	"github.com/streadway/amqp"
)

// SendFanoutManager supports receive/send and explicit send
type SendFanoutManager struct {
	sendChannel *amqp.Channel
	sendFanout  string
}

//NewSendFanoutManager creates new manager
func NewSendFanoutManager(serverAddress, sendFanout string) *SendFanoutManager {
	fm := new(SendFanoutManager)
	conn, err := amqp.Dial(serverAddress)
	log.F(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	log.F(err, "Failed to open a channel")
	err = ch.ExchangeDeclare(
		sendFanout,
		"fanout", //kind string,
		false,    //durable bool,
		false,    //autoDelete bool,
		false,    //internal bool,
		false,    //noWait bool,
		nil)      //args amqp.Table)
	log.F(err, "Failed to declare exhange")
	fm.sendChannel = ch
	fm.sendFanout = sendFanout

	return fm
}

// Send fanout message
func (fm *SendFanoutManager) Send(msg *amqp.Publishing) error {
	log.V("Sending message on send fanout %s\n", fm.sendFanout)
	err := fm.sendChannel.Publish(fm.sendFanout, "", false, false, *msg)
	if err != nil {
		fmt.Printf("Failed sending %s", err)
	}

	return err
}
