package postgres

import (
	"context"
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
	IncomingCh         chan interface{}
	Conn               *pgx.Conn
	mx                 sync.RWMutex
	isReadyConn        bool   // флаг показывающий подключен ли сервис к БД
	cancel             func() // функция закрытия контекста
}

// Основной процесс, получает сообщения, запускает логику.
// Параметры: конекст, количество попыток при неудачном запросе
func (p *Postgres) mainProcess(ctx context.Context, numberAttempts int) {

	go func() {
		defer log.Warning("mainProcess has finished its work")
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-p.IncomingCh:
				event, ok := msg.(msgEvent)
				if !ok {
					// прекратить работу, ошибка приведения типа
					log.Error("type conversion error")
					p.PostgresShutdown()
				}
				answer, err := p.requestMaker(event, numberAttempts)
				if err != nil {
					// ошибка запроса, либо закончились попытки
					// прекратить работу
					log.Error(err)
					p.PostgresShutdown()
				}
				event.GetReverceCh() <- answer
			}
		}

	}()
}

// Метод производит запрос, если запрос был провален из-за проблемы подключения к БД то запрос повториться указанное количество раз.
// Если запрос провалился из-за ошибки запроса то вернется ошибка
func (p *Postgres) requestMaker(event msgEvent, numberAttempts int) (answerEvent, error) {
	var err error
	answer := answerEvent{}

	for i := 0; i < numberAttempts; i++ {
		switch event.GetTypeMsg() {
		case typeGetOffset:
			offset, err, isConn := p.GetOffset()
			if !isConn {
				time.Sleep(1 * time.Second)
				continue
			}

			answer.offset = offset
			return answer, err
		case typeInputMsg:
			err, isConn := p.RequestDb(event.GetMsg(), event.GetOffset())
			if !isConn {
				time.Sleep(1 * time.Second)
				continue
			}
			return answer, err
		}
	}

	err = fmt.Errorf("the number of attempts to send the request has been exceeded")
	return answer, err
}

// Метод производит вызов процедуры в БД (процедура передается из переменной окружения).
// Возвращает ошибку и флаг подключения к БД
func (pg *Postgres) RequestDb(msg []byte, offset_msg int64) (error, bool) {
	var err error
	if pg.getIsReadyConn() {
		pg.mx.Lock()
		_, err = pg.Conn.Exec(context.Background(), pg.recordingProcedure, msg, offset_msg)
		pg.mx.Unlock()
		if err != nil {
			log.Errorf("QueryRow failed: %v\n", err)
			return err, true
		} else {
			log.Info("the message is recorded in the database")
		}
	}
	return err, false
}

// Метод возврщает последний оффсет из БД, ошибку запроса, флаг подключения к БД
func (pg *Postgres) GetOffset() (int, error, bool) {
	var err error
	var offset_msg int
	if pg.getIsReadyConn() {
		pg.mx.Lock()
		err = pg.Conn.QueryRow(context.Background(), pg.funcGetOffset).Scan(&offset_msg)
		pg.mx.Unlock()
		if err != nil {
			log.Errorf("QueryRow failed: %v\n", err)
		} else {
			log.Infof("the request GetOffset was successfully, offset : %d", offset_msg)
		}
		return offset_msg, err, true
	}
	return offset_msg, err, false
}

// процесс контроля за подключением к БД.
// Параметры: количество попыток подключения, время ожидания между проверками состояния
func (p *Postgres) processConnDB(ctx context.Context, numberAttempts, timeSleep int) {

	go func() {
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
					if !p.connection(numberAttempts) {
						// все попытки подключения провалены
						// завершение работы
						p.PostgresShutdown()
						return
					}
				}
			}
		}
	}()
}

// цикл переподключения
func (pg *Postgres) connection(numberAttempts int) bool {
	for i := 0; i < numberAttempts; i++ {
		if !pg.getIsReadyConn() {
			pg.connPg()
		} else {
			return true
		}
		time.Sleep(1 * time.Second)
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
func (pg *Postgres) checkConn() {
	defer pg.mx.Unlock()
	ctxCheck, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err := pg.Conn.Ping(ctxCheck)
	pg.mx.Lock()
	if err != nil {
		log.Errorf("Database connection error: %v\n", err)
		pg.isReadyConn = false
		return
	}
	pg.isReadyConn = true
}

// потокобезопасно возвращает флаг isReadyConn, который сигнализирует о подключении к БД
func (pg *Postgres) getIsReadyConn() bool {
	defer pg.mx.RUnlock()
	pg.mx.RLock()
	res := pg.isReadyConn
	return res
}

func (pg *Postgres) setIsReadyConn(value bool) {
	defer pg.mx.Unlock()
	pg.mx.Lock()
	pg.isReadyConn = value
}

// Закрытие подключения к БД
func (p *Postgres) CloseConn() {
	defer p.mx.Unlock()
	p.checkConn()
	if p.getIsReadyConn() {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		p.mx.Lock()
		err := p.Conn.Close(ctx)
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
func (p *Postgres) PostgresShutdown() {
	p.cancel()
	p.CloseConn()
	log.Warning("postgres has finished its work")
}

// инициализирует Postgres{}, запускает чтение ENV и подключение к БД
func InitPg(envs envs, incomingCh chan interface{}) *Postgres {
	ctx, cancel := context.WithCancel(context.Background())

	pg := &Postgres{
		url:                envs.GetUrl(),
		recordingProcedure: envs.GetRecProcedure(),
		funcGetOffset:      envs.GetOffsetFunc(),
		mx:                 sync.RWMutex{},
		IncomingCh:         incomingCh,
		cancel:             cancel,
	}

	pg.processConnDB(ctx, 10, 3)
	time.Sleep(50 * time.Millisecond)
	pg.mainProcess(ctx, 10)

	return pg
}
