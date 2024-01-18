package postgres

import (
	"context"
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
	isReadyConn        bool // флаг показывающий подключен ли сервис к БД
}

// основной процесс
func (p *Postgres) mainProcess(ctx context.Context) {
	p.connection(10)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-p.IncomingCh:
			event := msg.(msgEvent)
			switch event.GetTypeMsg() {
			case typeGetOffset:
				responce := answerEvent{
					offset: p.GetOffset(),
				}
				event.GetReverceCh() <- responce
			}
		}
	}

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

// func (pg *Postgres) sendingResponse(revCh chan interface{}, answer answerEvent) {
// 	revCh <- answer
// }

// цикл переподключения
func (pg *Postgres) connection(maxAttempts int) bool {
	for i := 0; i < maxAttempts; i++ {
		if !pg.isReadyConn {
			pg.connPg()
		} else {
			return true
		}
		time.Sleep(3 * time.Second)
	}
	return false
}

// Метод производит вызов процедуры в БД (процедура передается из переменной окружения)
func (pg *Postgres) RequestDb(msg []byte, offset_msg int64) error {
	log.Info(pg.recordingProcedure)
	// fmt.Sprintf(pg.RecordingProcedure)
	_, err := pg.Conn.Exec(context.Background(), pg.recordingProcedure, msg, offset_msg)
	if err != nil {
		log.Errorf("QueryRow failed: %v\n", err)
		return err
	}
	return nil
}

// Метод возврщает последний оффсет из БД
func (pg *Postgres) GetOffset() int {
	var offset_msg int
	if pg.isReadyConn {
		err := pg.Conn.QueryRow(context.Background(), pg.funcGetOffset).Scan(&offset_msg)
		if err != nil {
			log.Errorf("QueryRow failed: %v\n", err)
		} else {
			log.Infof("the request GetOffset was successfully, offset : %d", offset_msg)
		}
	}
	return offset_msg
}

// инициализирует Postgres{}, запускает чтение ENV и подключение к БД
func InitPg(envs envs) *Postgres {
	pg := &Postgres{
		url:                envs.GetUrl(),
		recordingProcedure: envs.GetRecProcedure(),
		funcGetOffset:      envs.GetOffsetFunc(),
	}

	pg.mainProcess(context.TODO())

	return pg
}
