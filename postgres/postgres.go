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
	isReadyConn        bool // флаг показывающий подключен ли сервис к БД
}

// Основной процесс, получает сообщения, запускает логику.
// Параметры: конекст, количество попыток при неудачном запросе
func (p *Postgres) mainProcess(ctx context.Context, numberAttempts int) {
	defer log.Warning("mainProcess has finished its work")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-p.IncomingCh:
				event, ok := msg.(msgEvent)
				if !ok {
					// прекратить работу
					log.Error("type conversion error")
				}
				answer, err := p.requestMaker(event, numberAttempts)
				if err != nil {
					// ошибка запроса, либо закончились попытки
					// прекратить работу
					log.Error(err)
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

// цикл переподключения
func (pg *Postgres) connection(numberAttempts int) bool {
	for i := 0; i < numberAttempts; i++ {
		if !pg.isReadyConn {
			pg.connPg()
		} else {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

func (pg *Postgres) connPg() {
	var err error
	pg.Conn, err = pgx.Connect(context.Background(), pg.url)
	if err != nil {
		log.Errorf("Database connection error: %v\n", err)
		pg.isReadyConn = false
	} else {
		pg.isReadyConn = true
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

// инициализирует Postgres{}, запускает чтение ENV и подключение к БД
func InitPg(envs envs) *Postgres {
	pg := &Postgres{
		url:                envs.GetUrl(),
		recordingProcedure: envs.GetRecProcedure(),
		funcGetOffset:      envs.GetOffsetFunc(),
		mx:                 sync.RWMutex{},
	}

	pg.mainProcess(context.TODO(), 10)

	return pg
}
