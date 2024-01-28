package rabbit

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

type Rabbit struct {
	url          string
	nameQueue    string
	nameConsumer string
	Connector    *amqp.Connection
	Channel      *amqp.Channel
	Consumer     *Consumer
	timeWaitBD   int // время ожидания ответа от БД в секундах
	outgoingCh   chan interface{}
	cancel       func()
}

func InitRabbit(url, nameQueue, nameConsumer string, timeWaitBD int) *Rabbit {
	res := &Rabbit{
		url:          url,
		nameQueue:    nameQueue,
		nameConsumer: nameConsumer,
		timeWaitBD:   timeWaitBD,
		outgoingCh:   make(chan interface{}),
	}

	return res
}

func (r *Rabbit) GetChan() chan interface{} {
	return r.outgoingCh
}

func (r *Rabbit) RabbitShutdown(err error) {
	log.Infof("RabbitMQ has shut down for a reason: %s", err)
	r.cancel()
}

func (r *Rabbit) StartRb() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	r.cancel = cancel

	go r.processRabbit(ctx)

	log.Info("processRabbit start")
	return ctx
}

func (r *Rabbit) processRabbit(ctx context.Context) {
	defer log.Info("processRabbit is closed")
	defer r.closeConn()
	defer r.closeChan()
	defer r.dellConsumer()

	for {
		select {
		case <-ctx.Done():
			// закрыть консюмера
			// закрыть канал
			// закрыть коннект
			return
		default:
			if err := r.checkConn(); err != nil {
				err = r.attemptCreateConn(ctx, 20, 2)
				if err != nil {
					r.RabbitShutdown(err)
					return
				}
			}

			if err := r.checkChan(); err != nil {
				err = r.initChan()
				if err != nil {
					r.RabbitShutdown(err)
					return
				}
			}
			err := r.checkConsumer()
			if err != nil {
				if errors.Is(err, consumerNotDedineError{}) {
					r.initConsumer(ctx)
					continue
				}

				if errors.Is(err, consumerActiveError{}) {
					r.dellConsumer()
					err := r.recreateChan()
					if err != nil {
						log.Info("Continue")
						continue
					}
					r.initConsumer(ctx)
					continue
				}
			}

			msg, err := r.Consumer.GetMessage()
			if err != nil {
				log.Error(err)
				continue
			}

			r.outgoingCh <- msg
			_, err = r.getResponse(ctx, msg.GetReverceCh(), r.timeWaitBD)
			if err != nil {
				r.RabbitShutdown(err)
				return
			}
			msg.signal()
		}
	}
}

// метод производит попытки переподключения к RabbitMQ
func (r *Rabbit) attemptCreateConn(ctx context.Context, attempt, timeWait int) error {
	var err error
	for i := 0; i < attempt; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
			err = r.initConn()
			if err == nil {
				return nil
			}
			log.Error(err)
			time.Sleep(time.Duration(timeWait) * time.Second)
		}
	}
	return fmt.Errorf("%w: %w", connectAttemptsError{}, err)
}

// Создание коннекта к RabbitMQ
func (r *Rabbit) initConn() error {
	conn, err := amqp.Dial(r.url)
	if err != nil {
		err = fmt.Errorf("%w: %w", connRabbitError{}, err)
		return err
	}

	r.Connector = conn
	log.Info("connect RabbitMQ created")
	return nil
}

// проверка коннекта
func (r *Rabbit) checkConn() error {
	if r.Connector == nil {
		return rabbitConnNotDefineError{}
	}

	if r.Connector.IsClosed() {
		return connRabbitNotReadyError{}
	}

	return nil
}

// закрытие коннекта
func (r *Rabbit) closeConn() error {
	err := r.checkConn()
	if err == nil {
		err = r.Connector.Close()
		if err != nil {
			return err
		}
		r.Connector = nil
		log.Info("connect RabbitMQ is closed")
	}

	if errors.Is(err, connRabbitNotReadyError{}) {
		r.Connector = nil
		log.Warning("connect RabbitMQ is already closed")
	}

	return nil
}

// Создание канала RabbitMQ
func (r *Rabbit) initChan() error {
	chanRabbit, err := r.Connector.Channel()
	if err != nil {
		err = fmt.Errorf("%w: %w", createChanRabbitError{}, err)
		return err
	}

	r.Channel = chanRabbit
	r.Channel.Qos(1, 0, false)
	log.Info("channel RabbitMQ created")
	return nil
}

// проверка канала RabbitMQ
func (r *Rabbit) checkChan() error {
	if r.Channel == nil {
		return chanRabbitNotDefineError{}
	}

	if r.Channel.IsClosed() {
		return chanRabbitIsClosedError{}
	}

	return nil
}

func (r *Rabbit) closeChan() error {
	err := r.checkChan()
	if err == nil {
		err = r.Channel.Close()
		if err != nil {
			return err
		}
		r.Channel = nil
		log.Info("chan RabbitMQ is closed")
	}

	if errors.Is(err, chanRabbitIsClosedError{}) {
		log.Warning("the chan RabbitMQ is already closed")
		r.Channel = nil
	}

	return nil
}

func (r *Rabbit) recreateChan() error {
	r.closeChan()
	err := r.initChan()
	if err != nil {
		log.Error("failed to recreate the RabbitMQ channel")
		return err
	}
	return nil
}

func (r *Rabbit) initConsumer(ctx context.Context) error {
	args, err := r.defineOffset(ctx)
	if err != nil {
		err = fmt.Errorf("%w: %w", consumCreateError{}, err)
		log.Error(err)
		return err
	}
	log.Info("offset received")
	ch, err := r.createConsumer(args)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Info("consumer channel has been created")
	cons := InitConsumer(ch)
	r.Consumer = cons
	log.Info("consumer RebbitMQ created")
	return nil
}

func (r *Rabbit) checkConsumer() error {
	if r.Consumer == nil {
		return consumerNotDedineError{}
	}

	if !r.Consumer.GetStatus() {
		return consumerActiveError{}
	}

	return nil
}

func (r *Rabbit) dellConsumer() {
	if r.Consumer != nil {
		r.Consumer.ConsumerShutdown()
		r.Consumer = nil
		log.Info("consumer is dell")
	}
	log.Warning("consumer is already dell")
}

// Создание консюмера
func (r *Rabbit) createConsumer(args amqp.Table) (<-chan amqp.Delivery, error) {
	ch, err := r.Channel.Consume(
		r.nameQueue,    // queue
		r.nameConsumer, // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		args,           // args
	)
	if err != nil {
		err = fmt.Errorf("%w: %w", consumCreateError{}, err)
	}
	return ch, err
}

// функция определения offset
func (r *Rabbit) defineOffset(ctx context.Context) (amqp.Table, error) {
	log.Info("defineOffset")

	args := amqp.Table{"x-stream-offset": "last"}

	lastOffsetDB, err := r.getStreamOffset(ctx)
	if err != nil {
		err = fmt.Errorf("%w: %w", createOffsetError{}, err)
		return args, err
	}

	if lastOffsetDB == 0 {
		// если streamOffset == 0, значит в БД нет записей, или offset затерт
		return args, nil
	}

	ch, err := r.createConsumer(args)
	if err != nil {
		err = fmt.Errorf("%w: %w", createOffsetError{}, err)
		return args, err
	}

	cons := InitConsumer(ch)
	msg, err := cons.GetMessage()
	if err != nil {
		return args, err
	}
	lastOffsetRabbit := msg.GetOffset()
	cons.ConsumerShutdown()

	err = r.recreateChan()
	if err != nil {
		return args, err
	}

	if lastOffsetDB > int(lastOffsetRabbit) {
		// если offset из БД больше, значит очередь в Rabbit была сброшена и нужно начать читать сообщения с начала очереди
		args = amqp.Table{"x-stream-offset": "first"}
		return args, nil
	}

	// если offset из Rabbit больше, значит очередь в Rabbit в порядке, и можно читать сообщения с offset+1 БД
	args = amqp.Table{"x-stream-offset": lastOffsetDB + 1}
	return args, nil
}

// метод для получения offset из БД.
func (r *Rabbit) getStreamOffset(ctx context.Context) (int, error) {
	log.Info("getStreamOffset")
	var offset int

	revCh := make(chan interface{})
	event := msgEvent{
		typeEvent: typeGetOffset,
		reverceCh: revCh,
	}

	r.outgoingCh <- event
	answer, err := r.getResponse(ctx, revCh, r.timeWaitBD)
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
func (r *Rabbit) getResponse(ctx context.Context, ch chan interface{}, timeWait int) (answerEvent, error) {
	var err error
	var answer answerEvent
	var ok bool

	ctxWait, _ := context.WithTimeout(context.Background(), time.Duration(timeWait)*time.Second)
	select {
	case <-ctx.Done():
		err = fmt.Errorf("waiting for a response has been interrupted")
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
