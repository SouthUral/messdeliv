package rabbit

import (
	"context"
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
func (c *Consumer) processCons(ctx context.Context) {
	defer log.Warning("the consumer stopped working")
	defer close(c.chOutput)
	defer c.setStatus(false)

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

}

// Закрывает активные горутины Consumer
func (c *Consumer) ConsumerShutdown() {
	c.cansel()
	c.setStatus(false)
}
