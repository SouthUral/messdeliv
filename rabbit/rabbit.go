package rabbit

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Rabbit struct {
	url          string
	nameQueue    string
	rabbitConn   RabbitConn
	streamOffset int
	IsReadyConn  bool
}

func (rb *Rabbit) connRabbitloop() {
	for {
		if !rb.IsReadyConn {
			rb.connRabbit()
		} else {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

}

// подключение к rabbitMQ
func (r *Rabbit) connRabbit() {
	var err error
	r.rabbitConn.Connector, err = amqp.Dial(r.url)
	if err != nil {
		log.Infof("Connect Rabbit failed: %v\n", err)
		return
	}

	r.rabbitConn.Channel, err = r.rabbitConn.Connector.Channel()
	if err != nil {
		log.Infof("Channel Rabbit failed: %v\n", err)
		return
	}

	if err = r.rabbitConn.Channel.Qos(1, 0, false); err != nil {
		log.Infof("Qos Rabbit failed: %v\n", err)
		return
	}

	r.IsReadyConn = true
}

func (r *Rabbit) Consumer() (<-chan amqp.Delivery, error) {
	var args amqp.Table

	if r.streamOffset > 0 {
		args = amqp.Table{"x-stream-offset": r.streamOffset}
	} else {
		args = amqp.Table{"x-stream-offset": "last"}
	}

	return r.rabbitConn.Channel.Consume(
		r.nameQueue,    // queue
		"test_service", // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		args,           // args
	)
}

type RabbitConn struct {
	Connector *amqp.Connection
	Channel   *amqp.Channel
}

func InitRb(envs RbEnvs) *Rabbit {
	rb := &Rabbit{
		url:       envs.getUrl(),
		nameQueue: envs.NameQueue,
	}

	// rb.StreamOffset = offset
	// if rb.StreamOffset > 0 {
	// 	rb.StreamOffset += 1
	// }

	rb.connRabbitloop()

	return rb
}
