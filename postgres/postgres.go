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
	Conn               *pgx.Conn
	isReadyConn        bool
}

func (pg *Postgres) connPg() {
	var err error
	pg.Conn, err = pgx.Connect(context.Background(), pg.url)
	if err != nil {
		log.Errorf("QueryRow failed: %v\n", err)
	} else {
		pg.isReadyConn = true
		log.Info("Connect DB is ready")
	}
}

func (pg *Postgres) connection() {
	for {
		if !pg.isReadyConn {
			pg.connPg()
		} else {
			return
		}
		time.Sleep(3 * time.Second)
	}

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
func InitPg(envs PgEnvs) *Postgres {
	pg := &Postgres{
		url:                envs.getUrl(),
		recordingProcedure: envs.RecordingProcedure,
		funcGetOffset:      envs.OffsetFunc,
	}

	pg.connection()

	return pg
}
