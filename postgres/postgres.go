package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pgx "github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type Postgres struct {
	url                string
	recordingProcedure string
	funcGetOffset      string
	Offset             string
	waitingTime        int // время ожидания между попытками запроса
	IncomingCh         chan interface{}
	Conn               *pgx.Conn
	mx                 sync.RWMutex
	isReadyConn        bool   // флаг показывающий подключен ли сервис к БД
	cancel             func() // функция закрытия контекста
}

// Метод получает сообщения и запускает горутину генерации запроса к БД requestMaker.
//
//   - ctx: общий контекст для postgres;
func (p *Postgres) eventRecipient(ctx context.Context) {
	defer log.Warning("Postgres: eventRecipient has finished its work")
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-p.IncomingCh:
			log.Debug("Postgres: a message has been received")
			event, ok := msg.(msgEvent)
			if !ok {
				// прекратить работу, ошибка приведения типа
				err := typeConversionError{}
				p.PostgresShutdown(err)
				return
			}
			p.requestMaker(ctx, event, p.waitingTime)
		}
	}
}

// Метод производит запрос в БД, если запрос провалился из-за ошибки подключения, то он будет повторяться.
//
//   - event: интерфейс полученного сообщения;
//   - waitingTime: время ожидания между попытками (в миллисекундах)
func (p *Postgres) requestMaker(ctx context.Context, event msgEvent, waitingTime int) {
	defer log.Debug("requestMaker has finished its work")
	var err error
	answer := answerEvent{}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			switch event.GetTypeMsg() {
			case typeGetOffset:
				offset, err := p.GetOffset()
				if err == nil {
					answer.offset = offset
					event.GetReverceCh() <- answer
					return
				} else {
					if errors.Is(err, queryError{}) {
						time.Sleep(time.Duration(waitingTime) * time.Millisecond)
						if p.checkConn() {
							p.PostgresShutdown(err)
							return
						}
					} else {
						time.Sleep(time.Duration(waitingTime) * time.Millisecond)
					}
				}
			case typeInputMsg:
				err = p.RequestDb(event.GetMsg(), event.GetOffset())
				if err == nil {
					event.GetReverceCh() <- answer
					return
				} else {
					if errors.Is(err, queryError{}) {
						time.Sleep(time.Duration(waitingTime) * time.Millisecond)
						if p.checkConn() {
							p.PostgresShutdown(err)
							return
						}
					} else {
						time.Sleep(time.Duration(waitingTime) * time.Millisecond)
					}
				}
			}
		}
	}
}

// Метод производит вызов процедуры в БД (процедура передается из переменной окружения).
func (pg *Postgres) RequestDb(msg []byte, offset_msg int64) error {
	var err error

	log.Debug("RequestDb start work")

	if pg.getIsReadyConn() {
		pg.mx.Lock()
		_, err = pg.Conn.Exec(context.Background(), pg.recordingProcedure, msg, offset_msg)
		pg.mx.Unlock()
		if err != nil {
			err = fmt.Errorf("%w: %w", queryError{}, err)
			log.Error(err)
			return err
		} else {
			log.Infof("Postgres: the message is recorded in the database with offset %d", offset_msg)
			return err
		}
	}
	// если флаг isReadyConn == false
	err = fmt.Errorf("%w", connectDBError{})
	log.Errorf("the request was not executed: %v", err)
	return err
}

// Метод возврщает последний оффсет из БД
func (pg *Postgres) GetOffset() (int, error) {
	var err error
	var offset_msg int

	log.Debug("GetOffset start work")

	if pg.getIsReadyConn() {
		pg.mx.Lock()
		err = pg.Conn.QueryRow(context.Background(), pg.funcGetOffset).Scan(&offset_msg)
		pg.mx.Unlock()
		if err != nil {
			err = fmt.Errorf("%w: %w", queryError{}, err)
			log.Error(err)
		} else {
			log.Infof("the request GetOffset was successfully, offset : %d", offset_msg)
		}
		return offset_msg, err
	}
	err = fmt.Errorf("%w", connectDBError{})
	log.Errorf("the request was not executed: %v", err)
	return offset_msg, err
}

/*
процесс контроля за подключением к БД.

Параметры: количество попыток подключения, время ожидания между проверками состояния
*/
func (p *Postgres) processConnDB(ctx context.Context, numberAttempts, timeSleep int) {
	// var b

	defer log.Warning("processConnDB has finished its work")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if p.getIsReadyConn() {
				time.Sleep(time.Duration(timeSleep) * time.Second)
				p.checkConn()
			} else {
				if !p.connection(ctx, numberAttempts, timeSleep) {
					// все попытки подключения провалены
					// завершение работы
					err := endConnectAttemptsError{}
					p.PostgresShutdown(err)
					return
				}
			}
		}
	}
}

// цикл переподключения
func (pg *Postgres) connection(ctx context.Context, numberAttempts, timeSleep int) bool {
	for i := 0; i < numberAttempts; i++ {
		select {
		case <-ctx.Done():
			return false
		default:
			if !pg.getIsReadyConn() {
				pg.connPg()
			} else {
				return true
			}
			time.Sleep(time.Duration(timeSleep) * time.Second)
		}
	}
	return false
}

func (pg *Postgres) connPg() {
	var err error
	pg.Conn, err = pgx.Connect(context.Background(), pg.url)
	if err != nil {
		log.Errorf("Database connection error: %v\n", err)
		pg.setIsReadyConn(false)
	} else {
		pg.setIsReadyConn(true)
		log.Info("Connect DB is ready")
	}
}

// метод для проверки подключения к БД
func (pg *Postgres) checkConn() bool {
	var err error

	ctxCheck, _ := context.WithTimeout(context.Background(), 5*time.Second)

	pg.mx.Lock()
	if pg.Conn != nil {
		err = pg.Conn.Ping(ctxCheck)
	} else {
		err = fmt.Errorf("the connector is not defined")
	}
	pg.mx.Unlock()

	if err != nil {
		log.Errorf("Database connection error: %v\n", err)
		pg.setIsReadyConn(false)
		return false
	}
	pg.setIsReadyConn(true)
	return true
}

// потокобезопасно возвращает флаг isReadyConn, который сигнализирует о подключении к БД
func (pg *Postgres) getIsReadyConn() bool {
	defer pg.mx.RUnlock()
	pg.mx.RLock()
	res := pg.isReadyConn
	connNotnil := (pg.Conn != nil)
	return res && connNotnil
}

func (pg *Postgres) setIsReadyConn(value bool) {
	defer pg.mx.Unlock()
	pg.mx.Lock()
	pg.isReadyConn = value
}

// Закрытие подключения к БД
func (p *Postgres) CloseConn() {
	p.checkConn()
	if p.getIsReadyConn() {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		p.mx.Lock()
		err := p.Conn.Close(ctx)
		p.mx.Unlock()
		if err != nil {
			log.Error(err)
		} else {
			log.Info("the connection to the database is closed")
		}
		return
	}
	log.Warning("the connection to the database has already been closed")
}

// метод для прекращения работы Postgres
func (p *Postgres) PostgresShutdown(err error) {
	log.Errorf("Postgres shutdown due to: %v", err)
	p.cancel()
	time.Sleep(50 * time.Millisecond)
	p.CloseConn()
	log.Warning("postgres has finished its work")
}

// инициализирует Postgres{}, запускает чтение ENV и подключение к БД.
//
//   - waitingTime: время ожидания между попытками запроса к БД в миллисекундах;
//   - numberAttemptsBDrequest: количество попыток запроса к БД (5-20);
//   - numberAttemptsConnect: количество попыток переподключения к БД (10-30);
//   - waitingTimeConn: время ожидания между попытками переподключения к БД в секундах (1-10).
func InitPg(envs envs, // параметры для запуска
	incomingCh chan interface{},
	waitingTime,
	numberAttemptsBDrequest,
	numberAttemptsConnect,
	waitingTimeConn int) (*Postgres, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())

	pg := &Postgres{
		url:                envs.GetUrl(),
		recordingProcedure: envs.GetRecProcedure(),
		funcGetOffset:      envs.GetOffsetFunc(),
		mx:                 sync.RWMutex{},
		IncomingCh:         incomingCh,
		cancel:             cancel,
		waitingTime:        waitingTime,
	}

	go pg.processConnDB(ctx, numberAttemptsConnect, waitingTimeConn)
	time.Sleep(50 * time.Millisecond)
	go pg.eventRecipient(ctx)

	return pg, ctx
}
