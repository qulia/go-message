package message

import (
	"github.com/qulia/go-log/log"
	"github.com/streadway/amqp"
)

// SendReceiveFanoutManager supports receive/send and explicit send
type SendReceiveFanoutManager struct {
	receiveFanoutManager *ReceiveFanoutManager
	sendFanoutManager    *SendFanoutManager
	onReceive            func(*amqp.Delivery) *amqp.Publishing
}

//NewSendReceiveFanoutManager creates new manager
func NewSendReceiveFanoutManager(serverAddress, receiveFanout, sendFanout string,
	onReceive func(*amqp.Delivery) *amqp.Publishing) *SendReceiveFanoutManager {

	fm := new(SendReceiveFanoutManager)
	fm.onReceive = onReceive
	fm.receiveFanoutManager = NewReceiveFanoutManager(
		serverAddress, receiveFanout, func(msg *amqp.Delivery) {
			outgoing := onReceive(msg)
			if outgoing != nil {
				err := fm.sendFanoutManager.Send(outgoing)
				log.E(err, "Failed to send message %s\n", sendFanout)
			}
		})
	fm.sendFanoutManager = NewSendFanoutManager(serverAddress, sendFanout)

	return fm
}

// Send fanout message
func (fm *SendReceiveFanoutManager) Send(msg *amqp.Publishing) error {
	return fm.sendFanoutManager.Send(msg)
}
