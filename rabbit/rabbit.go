package rabbit

import (
	"fmt"
	"time"

	ut "messdelive/utils"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Rabbit struct {
	Host         string
	Port         string
	VHost        string
	User         string
	Password     string
	NameQueue    string
	Heartbeat    int
	RabbitConn   RabbitConn
	StreamOffset int
	IsReadyConn  bool
}

func (rabbit *Rabbit) rabbitEnv() {
	rabbit.Host = ut.GetEnvStr("ASD_RMQ_HOST")
	rabbit.Port = ut.GetEnvStr("ASD_RMQ_PORT")
	rabbit.VHost = ut.GetEnvStr("ASD_RMQ_VHOST")
	rabbit.User = ut.GetEnvStr("SERVICE_RMQ_ENOTIFY_USERNAME")
	rabbit.Password = ut.GetEnvStr("SERVICE_RMQ_ENOTIFY_PASSWORD")
	rabbit.Heartbeat = ut.GetEnvInt("ASD_RMQ_HEARTBEAT")
	rabbit.NameQueue = ut.GetEnvStr("SERVICE_RMQ_QUEUE")
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

func (rabbit *Rabbit) connRabbit() {
	loginParameters := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", rabbit.User, rabbit.Password, rabbit.Host, rabbit.Port, rabbit.VHost)
	var err error
	rabbit.RabbitConn.Connector, err = amqp.Dial(loginParameters)
	if err != nil {
		log.Printf("Connect Rabbit failed: %v\n", err)
		return
	}

	rabbit.RabbitConn.Channel, err = rabbit.RabbitConn.Connector.Channel()
	if err != nil {
		log.Printf("Channel Rabbit failed: %v\n", err)
		return
	}

	if err = rabbit.RabbitConn.Channel.Qos(1, 0, false); err != nil {
		log.Printf("Qos Rabbit failed: %v\n", err)
		return
	}

	rabbit.IsReadyConn = true
}

func (rabbit *Rabbit) Consumer() (<-chan amqp.Delivery, error) {
	var args amqp.Table

	if rabbit.StreamOffset > 0 {
		args = amqp.Table{"x-stream-offset": rabbit.StreamOffset}
	} else {
		args = amqp.Table{"x-stream-offset": "last"}
	}

	return rabbit.RabbitConn.Channel.Consume(
		rabbit.NameQueue, // queue
		"test_service",   // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		args,             // args
	)
}

type RabbitConn struct {
	Connector *amqp.Connection
	Channel   *amqp.Channel
}

func RabbitMainConnect(offset int) Rabbit {
	configRabbit := Rabbit{}

	configRabbit.StreamOffset = offset
	if configRabbit.StreamOffset > 0 {
		configRabbit.StreamOffset += 1
	}

	configRabbit.rabbitEnv()
	configRabbit.connRabbitloop()

	return configRabbit
}
