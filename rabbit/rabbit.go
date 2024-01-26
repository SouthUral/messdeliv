package rabbit

import (
	"context"
	"errors"
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

// метод возвращает ссылку на Consumer или ошибку
func (r *Rabbit) getConsumer() (*Consumer, error) {
	return r.consumer, r.checkConsumer()
}

// метод проверят определен и активен ли Consumer
func (r *Rabbit) checkConsumer() error {
	var err error

	// если consumer не определен
	if r.consumer == nil {
		err = fmt.Errorf("%w", consumerNotDedineError{})
		return err
	}

	// если consumer не активен
	if !r.consumer.GetStatus() {
		err = consumerActiveError{}
		return err
	}

	return err
}

// метод возвращает ссылку на RabbitConn или ошибку
func (r *Rabbit) getRabbitConn() (*RabbitConn, error) {
	return r.rabbitConn, r.checkRabbitConn()
}

// проверяет определен и активен ли RabbitConn
func (r *Rabbit) checkRabbitConn() error {
	var err error

	if r.rabbitConn == nil {
		err = rabbitConnNotDefineError{}
		return err
	}

	if err = r.rabbitConn.CheckStatusReady(); err != nil {
		return err
	}

	return err
}

func (r *Rabbit) initRabbitConn(numberAttempts, timeSleep int) error {
	var err error
	var rc *RabbitConn

	rc, err = InitRabbitConn(r.url, numberAttempts, timeSleep)
	if err != nil {
		log.Error(err)
		return err
	}

	r.mx.Lock()
	r.rabbitConn = rc
	r.mx.Unlock()

	return err
}

// Процесс мониторинга и переподключения коннекта RabbitMQ.
//
//   - ctx: общий контекст Rabbit;
//   - numberAttempts: количество попыток переподключения;
//   - timeSleep: время ожидания между проверками подключения.
func (r *Rabbit) processConnRb(ctx context.Context, numberAttempts, timeSleep int) {
	defer log.Warning("processConnRb has finished its work")
	log.Info("start processConnRb")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := r.checkRabbitConn()
			switch {
			case errors.Is(err, rabbitConnNotDefineError{}):
				err := r.initRabbitConn(numberAttempts, timeSleep)
				if err != nil {
					r.RabbitShutdown(err)
					return
				}
			case errors.Is(err, connRabbitNotReadyError{}):
				err := r.rabbitConn.CreateConnChan()
				if err != nil {
					log.Error("connection to RabbitMQ could not be restored")
					r.RabbitShutdown(err)
					return
				}
			}
			time.Sleep(time.Duration(timeSleep) * time.Second)
		}
	}
}

// Процесс принимает общий для всех горутин rabbit контекст.
//
// Контролирует состояние Consumer, пересоздает его, если старый перестал работать.
//   - ctx: общий контекст Rabbit;
//   - numberAttempts: количество попыток создания Consumer;
//   - timeWaitCheck: интервал между проверками, передается внутрь функции CreateConsumer.
func (r *Rabbit) controlConsumers(ctx context.Context, numberAttempts, timeWaitCheck, timeWaitCreate int) {
	defer log.Warning("ControlConsumers has finished its work")
	log.Info("start ControlConsumers")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			errConn := r.checkRabbitConn()
			if errConn != nil {
				r.deleteConsumer()
				time.Sleep(time.Duration(timeWaitCheck) * time.Second)
				continue
			}

			errCons := r.checkConsumer()
			switch {
			case errors.Is(errCons, consumerNotDedineError{}):
				log.Error(errCons)
				err := r.createConsumer(ctx, numberAttempts, timeWaitCreate)
				if err != nil {
					log.Error("the number of attempts to create a consumer has ended")
					r.RabbitShutdown(err)
					return
				}
			case errors.Is(errCons, consumerActiveError{}):
				r.deleteConsumer()
				continue
			}

			time.Sleep(time.Duration(timeWaitCheck) * time.Second)
		}
	}

}

// метод создает Consumer.
//
// Делает numberAttempts количество попыток с интервалом timeWait.
// Если все попытки были неудачными то вернет false.
//   - ctx: общий контекст Rabbit;
//   - numberAttempts: количество попыток создания Consumer;
//   - timeWait: интервал между попытками.
func (r *Rabbit) createConsumer(ctx context.Context, numberAttempts, timeWait int) error {
	var numbeRestarts int
	var err error
	var cons *Consumer
	var offset int

	for numbeRestarts < numberAttempts {
		select {
		case <-ctx.Done():
			return err
		default:
			offset, err = r.getStreamOffset()
			if err != nil {
				return err
			}
			cons, err = r.rabbitConn.NewConsumer(offset, r.nameQueue, r.nameConsumer)
			if err != nil {
				numbeRestarts++
				time.Sleep(time.Duration(timeWait) * time.Second)
				continue
			} else {
				r.mx.Lock()
				r.consumer = cons
				r.mx.Unlock()
				return err
			}
		}
	}

	return err
}

// метод проверяет, есть ли активный Consumer, если есть, то возвращает сообщение от него
func (r *Rabbit) getConsEvent() (msgEvent, error) {
	var event msgEvent
	var err error

	err = r.checkConsumer()
	if err != nil {
		return event, err
	}

	event = r.consumer.GetMessage()
	return event, err
}

// метод останавливает активные процессы у Consumer, и удаляет его из структуры Rabbit.
// После срабатывания метода, указатель на Consumer будет равен nil.
func (r *Rabbit) deleteConsumer() {
	if r.consumer != nil {
		r.consumer.ConsumerShutdown()
		r.consumer = nil
		log.Info("consumer is deleted")
	}
}

// Процесс получает события от Consumer и отправляет на сохранение в БД.
//
//   - ctx: общий контекст Rabbit;
//   - waitingTime: время ожидания между обращениями к потребителю (в миллисекундах)
//   - waitingErrTime: время ожидания, если потребитель не работает или не существует (в секундах)
func (r *Rabbit) sendingMessages(ctx context.Context, waitingTime, waitingErrTime int) {
	defer log.Warning("sendingMessages has finished its work")
	var err error
	var event msgEvent
	log.Info("start sendingMessages")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if event, err = r.getConsEvent(); err != nil {
				switch {
				case errors.Is(err, noEventError{}):
					time.Sleep(time.Duration(waitingTime) * time.Millisecond)
				default:
					time.Sleep(time.Duration(waitingErrTime) * time.Second)
				}
			} else {
				r.outgoingCh <- event
				_, err = r.getResponse(event.reverceCh, r.timeWaitBD)
				if err != nil {
					r.RabbitShutdown(err)
					return
				}
				// отправка сигнала, для разблокировки Consumer
				event.signal()
			}
		}
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
		err = fmt.Errorf("%w: %w", gettingOffsetError{}, err)
		log.Error(err)
		return offset, err
	}

	offset = answer.GetOffset()

	return offset, err
}

// Метод получения ответа от БД на отправленный запрос.
//   - ch: канал получения ответа от БД;
//   - timeWait: время ожидания ответа от БД в секундах, рекомендуемый параметр 5-30 секунд
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
func (r *Rabbit) RabbitShutdown(err error) {
	log.Errorf("Rabbit shutdown due to: %v", err)
	r.cancel()
	r.deleteConsumer()
	if r.rabbitConn != nil {
		r.rabbitConn.Shutdown()
	}
	log.Warning("rabbit has finished its work")
}

func (r *Rabbit) deleteConn() {
	if r.rabbitConn != nil {
		r.rabbitConn.Shutdown()
		r.rabbitConn = nil
		log.Info("rabbitConn is deleted")
	}
}

func (r *Rabbit) restartAll() {
	r.deleteConsumer()
	r.deleteConn()
}

// Функция создания структуры Rabbit
//   - envs : парметры необходимые для запуска;
func InitRb(envs envs) *Rabbit {
	rb := &Rabbit{
		url:        envs.GetUrl(),
		nameQueue:  envs.GetNameQueue(),
		outgoingCh: make(chan interface{}, 5),
	}

	return rb
}

// метод стартует основные процессы Rabbit.
//   - numberAttemptsReonnect : количество попыток реконнекта к RabbitMQ;
//   - numberAttemptsCreateConsumer : количество попыток создать потребителя;
//   - timeWaitReconnect : время ожидания между реконнектами к RabbitMQ (секунды);
//   - timeWaitCheckConsumer : время ожидания между проверками состояния потребителя (секунды);
//   - waitingTimeMess : ожидание между проверками сообщения из очереди RabbitMQ (миллисекунды);
//   - waitingErrTime : ожидание сообщения из очереди в случае возникновения ошибки (секунды);
//   - timeWaitBD : время ожидания ответа от БД, рекомендуется поставить 5-30 секунд (секунды);
//   - timeWaitCreate : время ожидания между попытками создания Consumer (секунды);
func (r *Rabbit) StartRb(numberAttemptsReonnect, numberAttemptsCreateConsumer, timeWaitReconnect, timeWaitCheckConsumer, waitingTimeMess, waitingErrTime, timeWaitBD, timeWaitCreate int) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.timeWaitBD = timeWaitBD

	go r.processConnRb(ctx, numberAttemptsReonnect, timeWaitReconnect)
	go r.controlConsumers(ctx, numberAttemptsCreateConsumer, timeWaitCheckConsumer, timeWaitCreate)
	go r.sendingMessages(ctx, waitingTimeMess, waitingErrTime)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(1 * time.Minute)
				r.restartAll()
			}
		}
	}()

	return ctx
}
