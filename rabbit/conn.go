package rabbit

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
)

// структура работает с каналом и коннектом RabbitMQ
type RabbitConn struct {
	numAttemps  int              // количество попыток повторного подключения
	timeWait    int              // время ожидания между попытками подключения в секундах
	url         string           // URL для подключения к RabbitMQ
	Connector   *amqp.Connection //
	Channel     *amqp.Channel    //
	IsReadyConn bool             // флаг активности подключения rabbitMQ
	IsReadyCh   bool             // флаг активности канала rabbitMQ
	mx          sync.RWMutex
}

// функция инициализирует структуру RabbitConn и делает попытки создать коннект и канал для RabbitMQ
// url: url для подключения к RabbitMQ;
// numAttemps: количество попыток подключения;
// timeWait: время ожидания между попытками в секундах
func InitRabbitConn(url string, numAttemps, timeWait int) (*RabbitConn, error) {
	rc := &RabbitConn{
		numAttemps: numAttemps,
		timeWait:   timeWait,
		url:        url,
		mx:         sync.RWMutex{},
	}

	err := rc.CreateConnChan()

	return rc, err
}

// Получение флага активности подключения rabbitMQ
func (r *RabbitConn) GetIsReadyConn() bool {
	defer r.mx.RUnlock()
	r.mx.RLock()
	return r.IsReadyConn
}

// Установка флага активности подключения rabbitMQ
func (r *RabbitConn) SetIsReadyConn(value bool) {
	defer r.mx.Unlock()
	r.mx.Lock()
	r.IsReadyConn = value
}

// Получение флага активности канала rabbitMQ
func (r *RabbitConn) GetIsReadyCh() bool {
	defer r.mx.RUnlock()
	r.mx.RLock()
	return r.IsReadyCh
}

// Установка флага активности канала rabbitMQ
func (r *RabbitConn) SetIsReadyCh(value bool) {
	defer r.mx.Unlock()
	r.mx.Lock()
	r.IsReadyCh = value
}

// метод для обновления статуса коннекта
func (r *RabbitConn) UpdataStatusConn() {
	var isClosed bool

	r.mx.Lock()
	if r.Connector != nil {
		isClosed = r.Connector.IsClosed()
		// log.Infof("RabbitConn %v", isClosed)
	}
	r.mx.Unlock()

	if isClosed {
		r.SetIsReadyConn(false)
	} else {
		r.SetIsReadyConn(true)
	}
}

// метод обновления статуса канала
func (r *RabbitConn) UpdataStatusCh() {
	var isClosed bool

	r.mx.Lock()
	if r.Channel != nil {
		isClosed = r.Channel.IsClosed()
		// log.Infof("RabbitChan %v", isClosed)
	}
	r.mx.Unlock()

	if isClosed {
		r.SetIsReadyCh(false)
	} else {
		r.SetIsReadyCh(true)
	}
}

// метод обновляет статусы коннекта и канала.
// возвращает true если все статусы true, иначе false
func (r *RabbitConn) CheckStatusReady() bool {
	r.UpdataStatusConn()
	r.UpdataStatusCh()

	return r.GetStatus()
}

// метод проверки статуса подключения к RabbitMQ без обновления статусов
// возвращает true если все статусы true, иначе false
func (r *RabbitConn) GetStatus() bool {
	return r.GetIsReadyConn() && r.GetIsReadyCh()
}

// метод создает подключение к RabbitMQ и создает канал
func (r *RabbitConn) CreateConnChan() error {
	var err error

	err = r.connectionAttempts(r.numAttemps, r.timeWait)
	if err != nil {
		log.Error("connection to RabbitMQ failed")
		return err
	}

	err = r.createChann()
	if err != nil {
		log.Error("the creation of the RabbitMQ channel failed")
	}

	return err
}

// метод производит попытки создания коннекта к rabbitMQ
func (r *RabbitConn) connectionAttempts(numberAttempts, timeWait int) error {
	var err error
	for i := 0; i < numberAttempts; i++ {
		if !r.GetIsReadyConn() {
			r.createConnect()
		} else {
			return err
		}
		time.Sleep(time.Duration(timeWait) * time.Second)
	}

	err = fmt.Errorf("the number of attempts to connect to RabbitMQ has ended")
	return err
}

// метод создания коннекта RabbitMQ
func (r *RabbitConn) createConnect() {
	conn, err := amqp.Dial(r.url)

	if err != nil {
		log.Infof("Connect Rabbit failed: %v\n", err)
		r.SetIsReadyConn(false)
		return
	}

	r.mx.Lock()
	r.Connector = conn
	r.mx.Unlock()

	r.SetIsReadyConn(true)
	log.Info("Connect Rabbit was created")

}

// метод создания канала
func (r *RabbitConn) createChann() error {
	chrb, err := r.Connector.Channel()

	if err != nil {
		log.Infof("Channel Rabbit failed: %v\n", err)
		r.SetIsReadyCh(false)
		return err
	}

	r.mx.Lock()
	r.Channel = chrb
	err = r.Channel.Qos(1, 0, false)
	r.mx.Unlock()

	if err != nil {
		r.SetIsReadyCh(false)
		log.Infof("Qos Rabbit failed: %v\n", err)
		return err
	}

	r.SetIsReadyCh(true)
	log.Info("Channel Rabbit is created")

	return err
}

// метод создает потребителя
func (r *RabbitConn) Consume(nameQueue, nameConsumer string, args amqp.Table) (<-chan amqp.Delivery, error) {
	var err error
	var ch <-chan amqp.Delivery

	if !r.CheckStatusReady() {
		err = fmt.Errorf("the connection to Rabbit is not ready to create a consumer")
		return ch, err
	}

	r.mx.Lock()
	if r.Channel != nil {
		ch, err = r.Channel.Consume(
			nameQueue,    // queue
			nameConsumer, // consumer
			false,        // auto-ack
			false,        // exclusive
			false,        // no-local
			false,        // no-wait
			args,         // args
		)
	} else {
		err = fmt.Errorf("channel RabbitMQ is not defined")
	}

	r.mx.Unlock()

	return ch, err
}

// метод закрытия канала и коннекта rabbitMQ
func (r *RabbitConn) Shutdown() {
	if r.GetIsReadyCh() {

		r.mx.Lock()
		r.Channel.Close()
		r.mx.Unlock()

		log.Warning("channel RabbitMQ is closed")
	} else {
		log.Warning("the RabbitMQ channel has already been closed")
	}

	if r.GetIsReadyConn() {

		r.mx.Lock()
		r.Connector.Close()
		r.mx.Unlock()

		log.Warning("connector RabbitMQ is close")
	} else {
		log.Warning("the RabbitMQ connector has already been closed")
	}
}
