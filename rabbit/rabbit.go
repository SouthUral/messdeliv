package rabbit

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Rabbit struct {
	url          string
	nameQueue    string
	nameConsumer string
	rabbitConn   *RabbitConn
	consumer     *Consumer
	mx           sync.RWMutex
	timeWaitBD   int // время ожидания ответа от БД в секундах
	outgoingCh   chan interface{}
	cancel       func()
}

func (r *Rabbit) GetChan() chan interface{} {
	return r.outgoingCh
}

// процесс мониторинга и переподключения коннекта RabbitMQ.
// ctx: общий контекст Rabbit;
// numberAttempts: количество попыток переподключения;
// timeSleep: время ожидания между проверками подключения.
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
				if r.rabbitConn.CheckStatusReady() {
					numbeRestarts = 0
					time.Sleep(time.Duration(timeSleep) * time.Second)
				} else {
					if numbeRestarts < numberAttempts {
						err := r.rabbitConn.CreateConnChan()
						if err != nil {
							numbeRestarts++
							continue
						}
					} else {
						r.RabbitShutdown()
					}
				}
			}
		}

	}()
}

// Принимает общий для всех горутин rabbit контекст.
// Контролирует состояние Consumer, пересоздает его, если старый перестал работать.
// ctx: общий контекст Rabbit;
// numberAttempts: количество попыток создания Consumer;
// timeWait: интервал между проверками, передается внутрь функции CreateConsumer.
func (r *Rabbit) controlConsumers(ctx context.Context, numberAttempts, timeWait int) {

	go func() {
		defer log.Warning("ControlConsumers has finished its work")
		log.Info("start ControlConsumers")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if r.rabbitConn != nil {
					if r.rabbitConn.GetStatus() {
						if r.consumer == nil {
							err := r.createConsumer(ctx, numberAttempts, timeWait)
							if err != nil {
								log.Error("the number of attempts to create a consumer has ended")
								r.RabbitShutdown()
								return
							}
						} else {
							if !r.consumer.GetStatus() {
								r.consumer = nil
							}
						}
					}
				}
				time.Sleep(time.Duration(timeWait) * time.Second)
			}
		}
	}()
}

// Создает Consumer.
// Делает numberAttempts количество попыток с интервалом timeWait.
// Если все попытки были неудачными то вернет false
// ctx: общий контекст Rabbit;
// numberAttempts: количество попыток создания Consumer;
// timeWait: интервал между попытками.
func (r *Rabbit) createConsumer(ctx context.Context, numberAttempts, timeWait int) error {
	var numbeRestarts int
	var err error
	var cons *Consumer
	var offset int

	for numbeRestarts < numberAttempts {
		offset, err = r.getStreamOffset()
		if err != nil {
			return err
		}
		cons, err = r.rabbitConn.NewConsumer(ctx, offset, r.nameQueue, r.nameConsumer)
		if err != nil {
			numbeRestarts++
			time.Sleep(time.Duration(timeWait) * time.Second)
			continue
		} else {
			r.consumer = cons
			return err
		}
	}

	return err
}

// получает события от Consumer и отправляет на сохранение в БД.
// ctx: общий контекст Rabbit;
// waitingTime: время ожидания между обращениями к потребителю в миллисекундах
func (r *Rabbit) sendingMessages(ctx context.Context, waitingTime int) {
	go func() {
		defer log.Warning("sendingMessages has finished its work")
		log.Info("start sendingMessages")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				event, err := r.getConsEvent()
				if err != nil {
					// log.Warning(err)
					// небольшое ожидание
					time.Sleep(time.Duration(waitingTime) * time.Millisecond)
				} else {
					r.outgoingCh <- event
					_, err := r.getResponse(event.reverceCh, r.timeWaitBD)
					if err != nil {
						log.Error(err)
						r.RabbitShutdown()
						return
					}
					// отправка сигнала, для разблокировки Consumer
					event.signal()
				}
			}
		}
	}()
}

// метод получает событие от Consumer
func (r *Rabbit) getConsEvent() (msgEvent, error) {
	var event msgEvent
	var err error

	if r.consumer != nil {
		if r.consumer.GetStatus() {
			select {
			case event = <-r.consumer.chOutput:
				return event, err
			default:
				err = fmt.Errorf("the event was not received from the Consumer")
				return event, err
			}
		} else {
			err = fmt.Errorf("Consumer is not active")
			return event, err
		}
	} else {
		err = fmt.Errorf("Consumer has not been defined yet")
		return event, err
	}
}

// метод для получения offset из БД.
func (r *Rabbit) getStreamOffset() (int, error) {
	var offset int

	revCh := make(chan interface{})
	event := msgEvent{
		typeEvent: typeGetOffset,
		reverceCh: revCh,
	}

	r.outgoingCh <- event
	answer, err := r.getResponse(revCh, r.timeWaitBD)
	if err != nil {
		err = fmt.Errorf("error getting offset %v", err)
		log.Error(err)
		return offset, err
	}

	if answer.GetOffset() > 0 {
		offset = answer.GetOffset() + 1
	}

	return offset, err
}

// Метод получения ответа от БД на отправленный запрос.
// ch: канал получения ответа от БД;
// timeWait: время ожидания ответа от БД в секундах, рекомендуемый параметр 5-30 секунд
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

// прекращение всех процессов Rabbit
func (r *Rabbit) RabbitShutdown() {
	r.cancel()
	r.rabbitConn.Shutdown()
	log.Warning("rabbit has finished its work")
}

// Функция создания структуры Rabbit
// envs: парметры необходимые для запуска;
// timeWaitBD: время ожидания ответа от БД, рекомендуется поставить 5-30 секунд
func InitRb(envs envs, timeWaitBD int) *Rabbit {
	rb := &Rabbit{
		url:        envs.GetUrl(),
		nameQueue:  envs.GetNameQueue(),
		outgoingCh: make(chan interface{}, 5),
		timeWaitBD: timeWaitBD,
	}

	rc, err := InitRabbitConn(envs.GetUrl(), 15, 1)
	if err != nil {
		log.Error("connection error when starting Rabbit")
	}

	rb.rabbitConn = rc

	return rb
}

// метод стартует основные процессы Rabbit
// numberAttempts: количество попыток;
// timeWait: время ожидания между поптыками в секундах;
// waitingTime: время ожидания между обращениями к потребителю в миллисекундах
func (r *Rabbit) StartRb(numberAttempts, timeWait, waitingTime int) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	r.processConnRb(ctx, numberAttempts, timeWait)
	r.controlConsumers(ctx, numberAttempts, timeWait)
	r.sendingMessages(ctx, waitingTime)

	return ctx
}
