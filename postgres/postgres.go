package postgres

import (
	"context"
	"fmt"
	"time"

	ut "messdelive/utils"

	pgx "github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

type Postgres struct {
	Host               string
	Port               string
	User               string
	Password           string
	DataBaseName       string
	RecordingProcedure string
	Offset             string
	Conn               *pgx.Conn
	isReadyConn        bool
}

func (pg *Postgres) pgEnv() {
	pg.Host = ut.GetEnvStr("ASD_POSTGRES_HOST")
	pg.Port = ut.GetEnvStr("ASD_POSTGRES_PORT")
	pg.User = ut.GetEnvStr("SERVICE_PG_ILOGIC_USERNAME")
	pg.Password = ut.GetEnvStr("SERVICE_PG_ILOGIC_PASSWORD")
	pg.DataBaseName = ut.GetEnvStr("ASD_POSTGRES_DBNAME")
	pg.RecordingProcedure = ut.GetEnvStr("SERVICE_PG_PROCEDURE")
	pg.Offset = ut.GetEnvStr("SERVICE_PG_GETOFFSET")
	pg.isReadyConn = false
}

func (pg *Postgres) connPg() {
	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", pg.User, pg.Password, pg.Host, pg.Port, pg.DataBaseName)
	var err error
	pg.Conn, err = pgx.Connect(context.Background(), dbURL)
	if err != nil {
		log.Printf("QueryRow failed: %v\n", err)
	} else {
		pg.isReadyConn = true
		log.Printf("Connect DB is ready: %s\n", pg.DataBaseName)
	}
}

func (pg *Postgres) connPgloop() {
	for {
		if !pg.isReadyConn {
			pg.connPg()
		} else {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

}

func (pg *Postgres) RequestDb(msg []byte, offset_msg int64) error {
	log.Info(pg.RecordingProcedure)
	// fmt.Sprintf(pg.RecordingProcedure)
	_, err := pg.Conn.Exec(context.Background(), pg.RecordingProcedure, msg, offset_msg)
	if err != nil {
		log.Errorf("QueryRow failed: %v\n", err)
		return err
	}
	return nil
}

func (pg *Postgres) GetOffset() int {
	var offset_msg int
	if pg.isReadyConn {
		err := pg.Conn.QueryRow(context.Background(), pg.Offset).Scan(&offset_msg)
		if err != nil {
			log.Errorf("QueryRow failed: %v\n", err)
		} else {
			log.Infof("the request GetOffset was successfully, offset : %d", offset_msg)
		}
	}
	return offset_msg
}

func PgMainConnect() Postgres {
	confPg := Postgres{}
	confPg.pgEnv()

	confPg.connPgloop()

	time.Sleep(50 * time.Millisecond)
	return confPg
}
