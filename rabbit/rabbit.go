package rabbit

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Rabbit struct {
	url          string
	nameQueue    string
	rabbitConn   RabbitConn
	streamOffset int
	IsReadyConn  bool // флаг активности подключения rabbitMQ
	IsReadyCh    bool // флаг активности канала rabbitMQ
	mx           sync.RWMutex
	outgoingCh   chan interface{}
	cancel       func()
}

func (r *Rabbit) GetChan() chan interface{} {
	return r.outgoingCh
}

func (r *Rabbit) GetIsReadyConn() bool {
	defer r.mx.RUnlock()
	r.mx.RLock()
	return r.IsReadyConn
}

func (r *Rabbit) SetIsReadyConn(value bool) {
	defer r.mx.Unlock()
	r.mx.Lock()
	r.IsReadyConn = value
}

func (r *Rabbit) GetIsReadyCh() bool {
	defer r.mx.RUnlock()
	r.mx.RLock()
	return r.IsReadyCh
}

func (r *Rabbit) SetIsReadyCh(value bool) {
	defer r.mx.Unlock()
	r.mx.Lock()
	r.IsReadyCh = value
}

func (r *Rabbit) processConnRb(ctx context.Context, numberAttempts, timeSleep int) {
	var numbeRestarts int

	go func() {
		defer log.Warning("processConnRb has finished its work")
		log.Info("start processConnRb")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if r.GetIsReadyConn() && r.GetIsReadyCh() {
					numbeRestarts = 0
					time.Sleep(time.Duration(timeSleep) * time.Second)

					// обновления статусов
					r.updataStatusConn()
					r.updataStatusCh()
				} else {
					if !r.GetIsReadyConn() {
						err := r.connection(numberAttempts)
						if err != nil {
							// закончить работу, подключение не выполено
						}
					}

					if !r.GetIsReadyCh() {
						err := r.createChann()
						if err != nil {
							if numbeRestarts < numberAttempts {
								// канал не может быть создан только по причине отсутствия коннекта, т.е. нужно опять проверить коннект
								numbeRestarts++
								continue
							} else {
								// закончить работу, канал не устанавливается, при этом коннект есть, нужно разбираться в чем причина
							}

						}
					}
				}
			}
		}

	}()
}

// метод для обновления статуса коннекта
func (r *Rabbit) updataStatusConn() {
	if r.rabbitConn.Connector.IsClosed() {
		r.SetIsReadyConn(false)
	} else {
		r.SetIsReadyConn(true)
	}
}

// метод обновления статуса канала
func (r *Rabbit) updataStatusCh() {
	if r.rabbitConn.Channel.IsClosed() {
		r.SetIsReadyCh(false)
	} else {
		r.SetIsReadyCh(true)
	}
}

func (rb *Rabbit) connection(numberAttempts int) error {
	var err error
	for i := 0; i < numberAttempts; i++ {
		if !rb.IsReadyConn {
			rb.createConnect()
		} else {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	err = fmt.Errorf("the number of attempts to connect to RabbitMQ has ended")
	return err

}

// подключение к rabbitMQ
func (r *Rabbit) createConnect() {
	var err error

	r.rabbitConn.Connector, err = amqp.Dial(r.url)
	if err != nil {
		log.Infof("Connect Rabbit failed: %v\n", err)
	}

	r.SetIsReadyConn(true)
}

// метод создания канала
func (r *Rabbit) createChann() error {
	var err error

	r.rabbitConn.Channel, err = r.rabbitConn.Connector.Channel()
	if err != nil {
		log.Infof("Channel Rabbit failed: %v\n", err)
		r.SetIsReadyCh(false)
		return err
	}

	if err = r.rabbitConn.Channel.Qos(1, 0, false); err != nil {
		r.SetIsReadyCh(false)
		log.Infof("Qos Rabbit failed: %v\n", err)
	}

	r.SetIsReadyCh(true)

	return err
}

func (r *Rabbit) consumerProcess(ctx context.Context, timeWait int) {
	go func() {
		offset, err := r.getStreamOffset(timeWait)
		if err != nil {
			// нужно закрыть программу
			r.RabbitShutdown()
			return
		}
		chanRb, err := r.createConsumer(offset)
		if err != nil {
			// нужно что-то делать если подписчик не был создан
		}
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-chanRb:
				revCh := make(chan interface{})
				event := msgEvent{
					typeEvent: typeInputMsg,
					reverceCh: revCh,
					message:   msg.Body,
					offset:    msg.Headers["x-stream-offset"].(int64),
				}
				r.outgoingCh <- event
				log.Debug("the message has been sent for writing to the database")

				_, err := r.getResponse(revCh, 3)
				if err != nil {
					log.Error(err)
					// завершить программу
					r.RabbitShutdown()
					return
				}
				msg.Ack(true)

			}
		}
	}()
}

// метод для получения offset из БД
func (r *Rabbit) getStreamOffset(timeWait int) (int, error) {
	var offset int

	revCh := make(chan interface{})
	event := msgEvent{
		typeEvent: typeGetOffset,
		reverceCh: revCh,
	}

	r.outgoingCh <- event
	answer, err := r.getResponse(revCh, timeWait)
	if answer.GetOffset() > 0 {
		offset = answer.GetOffset() + 1
	}

	return offset, err
}

// Метод получения ответа
func (r *Rabbit) getResponse(ch chan interface{}, timeWait int) (answerEvent, error) {
	var err error
	var answer answerEvent
	var ok bool

	ctxWait, _ := context.WithTimeout(context.Background(), time.Duration(timeWait)*time.Second)
	select {
	case <-ctxWait.Done():
		err = fmt.Errorf("the waiting time has ended")
	case msg := <-ch:
		answer, ok = msg.(answerEvent)
		if !ok {
			err = fmt.Errorf("type conversion error")
		}
	}
	return answer, err
}

// Метод создания consumer.
// Возвращает: канал, ошибку
func (r *Rabbit) createConsumer(streamOffset int) (<-chan amqp.Delivery, error) {
	var args amqp.Table

	if streamOffset > 0 {
		args = amqp.Table{"x-stream-offset": streamOffset}
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

// прекращение всех процессов Rabbit
func (r *Rabbit) RabbitShutdown() {
	r.cancel()
	// r.CloseConn()
	log.Warning("rabbit has finished its work")
}

func InitRb(envs envs) *Rabbit {
	rb := &Rabbit{
		url:        envs.GetUrl(),
		nameQueue:  envs.GetNameQueue(),
		outgoingCh: make(chan interface{}, 5),
		mx:         sync.RWMutex{},
		rabbitConn: RabbitConn{},
	}

	return rb
}

// метод стартует основные процессы Rabbit
func (r *Rabbit) StartRb() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	r.processConnRb(ctx, 10, 3)
	time.Sleep(50 * time.Millisecond)
	r.consumerProcess(ctx, 10)
}
