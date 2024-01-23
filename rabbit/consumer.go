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
	offset   int
}

// возвращает статус работы Consumer
func (c *Consumer) GetStatus() bool {
	defer c.mx.RUnlock()
	c.mx.RLock()
	return c.works
}

// установка статуса работы Consumer
func (c *Consumer) SetStatus(status bool) {
	defer c.mx.Unlock()
	c.mx.Lock()
	c.works = status
}

// метод получения аргументов для запуска консюмера
func (c *Consumer) getArgs() amqp.Table {
	var args amqp.Table

	if c.offset > 0 {
		args = amqp.Table{"x-stream-offset": c.offset}
	} else {
		args = amqp.Table{"x-stream-offset": "last"}
	}

	return args
}

// процесс консъюминга
func (c *Consumer) ProcessCons(ctx context.Context) {
	go func() {
		defer log.Warning("the consumer stopped working")
		defer close(c.chOutput)
		defer c.SetStatus(false)

		for msg := range c.chRb {
			ctxEvent, cancel := context.WithCancel(context.Background())
			event := msgEvent{
				signal:    cancel,
				typeEvent: typeInputMsg,
				reverceCh: make(chan interface{}),
				message:   msg.Body,
				offset:    msg.Headers["x-stream-offset"].(int64),
			}

			c.chOutput <- event

			select {
			case <-ctx.Done():
				// прекращение работы
				return
			case <-ctxEvent.Done():
				// ожидание ответа
				// продолжение работы
				msg.Ack(true)
			}

		}
	}()
}

// Метод создает и запускает consumer
// Возвращает: канал, ошибку
func (r *RabbitConn) NewConsumer(ctx context.Context, streamOffset int, nameQueue, nameConsumer string) (*Consumer, error) {
	var err error

	cons := &Consumer{
		offset:   streamOffset,
		chOutput: make(chan msgEvent),
	}

	if !r.CheckStatusReady() {
		err = fmt.Errorf("the connection or the RabbitMQ channel is not ready")
		log.Error(err)
		return cons, err
	}

	cons.chRb, err = r.Consume(nameQueue, nameConsumer, cons.getArgs())
	if err != nil {
		log.Error(err)
		return cons, err
	}

	cons.SetStatus(true)
	cons.ProcessCons(ctx)
	return cons, err
}
