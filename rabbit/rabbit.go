package rabbit

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// func init() {
// 	log.FieldKeyFile := "rabbit"
// }

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

// Процесс мониторинга и переподключения коннекта RabbitMQ.
// ctx: общий контекст Rabbit;
// numberAttempts: количество попыток переподключения;
// timeSleep: время ожидания между проверками подключения.
func (r *Rabbit) processConnRb(ctx context.Context, numberAttempts, timeSleep int) {
	defer log.Warning("processConnRb has finished its work")
	log.Info("start processConnRb")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if r.rabbitConn == nil {
				rc, err := InitRabbitConn(r.url, numberAttempts, timeSleep)
				if err != nil {
					log.Error("connection error during RabbitConn initialization")
					r.RabbitShutdown()
					return
				}
				r.mx.Lock()
				r.rabbitConn = rc
				r.mx.Unlock()
			}

			if r.rabbitConn.CheckStatusReady() {
				time.Sleep(time.Duration(timeSleep) * time.Second)
			} else {
				err := r.rabbitConn.CreateConnChan()
				if err != nil {
					log.Error("connection to RabbitMQ could not be restored")
					r.RabbitShutdown()
					return
				}
			}
		}
	}
}

// Процесс:
// Принимает общий для всех горутин rabbit контекст.
// Контролирует состояние Consumer, пересоздает его, если старый перестал работать.
// ctx: общий контекст Rabbit;
// numberAttempts: количество попыток создания Consumer;
// timeWait: интервал между проверками, передается внутрь функции CreateConsumer.
func (r *Rabbit) controlConsumers(ctx context.Context, numberAttempts, timeWait int) {
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

}

// Метод:
// создает Consumer.
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

// Процесс:
// получает события от Consumer и отправляет на сохранение в БД.
// ctx: общий контекст Rabbit;
// waitingTime: время ожидания между обращениями к потребителю в миллисекундах
func (r *Rabbit) sendingMessages(ctx context.Context, waitingTime int) {
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
	if r.rabbitConn != nil {
		r.rabbitConn.Shutdown()
	}
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

	return rb
}

// метод стартует основные процессы Rabbit
// numberAttempts: количество попыток;
// timeWait: время ожидания между поптыками в секундах;
// waitingTime: время ожидания между обращениями к потребителю в миллисекундах
func (r *Rabbit) StartRb(numberAttempts, timeWait, waitingTime int) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel

	go r.processConnRb(ctx, numberAttempts, timeWait)
	go r.controlConsumers(ctx, numberAttempts, timeWait)
	go r.sendingMessages(ctx, waitingTime)

	return ctx
}
