package rabbit

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Consumer struct {
	chRb     <-chan amqp.Delivery
	chOutput chan msgEvent
	mx       sync.RWMutex
	works    bool
	cansel   func() // функция для отмены контекста
}

// возвращает статус работы Consumer
func (c *Consumer) GetStatus() bool {
	defer c.mx.RUnlock()
	c.mx.RLock()
	status := c.works
	return status
}

// возвращает исходящий от Consumer канал
func (c *Consumer) GetChannal() chan msgEvent {
	return c.chOutput
}

// установка статуса работы Consumer
func (c *Consumer) setStatus(status bool) {
	defer c.mx.Unlock()
	c.mx.Lock()
	c.works = status
}

// процесс консъюминга
func (c *Consumer) processCons(ctx context.Context) {
	defer log.Warning("the consumer stopped working")
	defer close(c.chOutput)
	defer c.setStatus(false)

	log.Info("process consuming start")
	for msg := range c.chRb {
		log.Debug("received a message from RabbitMQ")
		ctxEvent, cancel := context.WithCancel(context.Background())
		event := msgEvent{
			signal:    cancel,
			typeEvent: typeInputMsg,
			reverceCh: make(chan interface{}),
			message:   msg.Body,
			offset:    msg.Headers["x-stream-offset"].(int64),
		}

		c.chOutput <- event

		err := c.WaitingforAnswer(ctx, ctxEvent)
		if err != nil {
			log.Error(err)
			return
		}

		msg.Ack(true)
	}
}

// метод ожидает команды, либо завершение работы консюмера, либо продолжение работы
func (c *Consumer) WaitingforAnswer(ctxCons, ctxEvent context.Context) error {
	var err error
	for {
		select {
		case <-ctxCons.Done():
			// прекращение работы
			err = fmt.Errorf("consumer shutdown command")
			return err
		case <-ctxEvent.Done():
			// ожидание ответа
			// продолжение работы
			return err
		}
	}
}

// метод возвращает сообщение из канал консюмера
func (c *Consumer) GetMessage() (msgEvent, error) {
	var err error
	event, ok := <-c.GetChannal()
	if !ok {
		err = consumerActiveError{}
	}
	return event, err
}

// Закрывает активные горутины Consumer
func (c *Consumer) ConsumerShutdown() {
	c.cansel()
	c.setStatus(false)
}

// функция инициализации консюмера
func InitConsumer(ch <-chan amqp.Delivery) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())

	cons := &Consumer{
		chOutput: make(chan msgEvent),
		cansel:   cancel,
		chRb:     ch,
	}

	log.Info("new consumer inited")
	cons.setStatus(true)
	go cons.processCons(ctx)

	return cons
}
