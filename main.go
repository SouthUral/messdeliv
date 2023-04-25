package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	pgx "github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Rabbit struct {
	Host         string
	Port         string
	VHost        string
	User         string
	Password     string
	NameQueue    string
	Heartbeat    int
	RabbitConn   RabbitConn
	streamOffset int
	isReadyConn  bool
}

type Postgres struct {
	Host               string
	Port               string
	User               string
	Password           string
	DataBaseName       string
	RecordingProcedure string
	Conn               *pgx.Conn
	isReadyConn        bool
}

func (pg *Postgres) pgEnv() {
	pg.Host = getEnvStr("ASD_POSTGRES_HOST", "localhost")
	pg.Port = getEnvStr("ASD_POSTGRES_PORT", "5432")
	pg.User = getEnvStr("SERVICE_PG_ILOGIC_USERNAME", "")
	pg.Password = getEnvStr("SERVICE_PG_ILOGIC_PASSWORD", "")
	pg.DataBaseName = getEnvStr("ASD_POSTGRES_DBNAME", "postgres")
	pg.RecordingProcedure = getEnvStr("SERVICE_PG_PROCEDURE", "call device.check_section($1, $2)")
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
		log.Printf("Connect DB is ready")
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

func (pg *Postgres) requestDb(msg []byte, offset_msg int64) error {
	_, err := pg.Conn.Exec(context.Background(), pg.RecordingProcedure, msg, offset_msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		return err
	}
	return nil
}

func (pg *Postgres) getOffset() int {
	var offset_msg int
	if pg.isReadyConn {
		err := pg.Conn.QueryRow(context.Background(), "SELECT device.get_offset();").Scan(&offset_msg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		}
		return offset_msg
	}
	return 0
}

func (rabbit *Rabbit) rabbitEnv() {
	rabbit.Host = getEnvStr("ASD_RMQ_HOST", "localhost")
	rabbit.Port = getEnvStr("ASD_RMQ_PORT", "5672")
	rabbit.VHost = getEnvStr("ASD_RMQ_VHOST", "")
	rabbit.User = getEnvStr("SERVICE_RMQ_ENOTIFY_USERNAME", "")
	rabbit.Password = getEnvStr("SERVICE_RMQ_ENOTIFY_PASSWORD", "")
	rabbit.Heartbeat = getEnvInt("ASD_RMQ_HEARTBEAT", 1)
	rabbit.NameQueue = getEnvStr("SERVICE_RMQ_QUEUE", "")
}

func getEnvStr(key, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if value, exists := os.LookupEnv(key); exists {
		val, err := strconv.Atoi(value)
		if err != nil {
			log.Fatal(err)
		}
		return val
	}
	return defaultVal
}

func (rb *Rabbit) connRabbitloop() {
	for {
		if !rb.isReadyConn {
			rb.connRabbit()
		} else {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

}

func (rabbit *Rabbit) connRabbit() {
	loginParameters := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", rabbit.User, rabbit.Password, rabbit.Host, rabbit.Port, rabbit.VHost)
	var err error
	rabbit.RabbitConn.Connector, err = amqp.Dial(loginParameters)
	if err != nil {
		log.Printf("Connect Rabbit failed: %v\n", err)
		return
	}

	rabbit.RabbitConn.Channel, err = rabbit.RabbitConn.Connector.Channel()
	if err != nil {
		log.Printf("Channel Rabbit failed: %v\n", err)
		return
	}

	if err = rabbit.RabbitConn.Channel.Qos(1, 0, false); err != nil {
		log.Printf("Qos Rabbit failed: %v\n", err)
		return
	}

	rabbit.isReadyConn = true
}

func (rabbit *Rabbit) Consumer() (<-chan amqp.Delivery, error) {
	var args amqp.Table

	if rabbit.streamOffset > 0 {
		args = amqp.Table{"x-stream-offset": rabbit.streamOffset}
	} else {
		args = amqp.Table{"x-stream-offset": "last"}
	}

	return rabbit.RabbitConn.Channel.Consume(
		rabbit.NameQueue, // queue
		"test_service",   // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		args,             // args
	)
}

type RabbitConn struct {
	Connector *amqp.Connection
	Channel   *amqp.Channel
}

func pgMainConnect() Postgres {
	confPg := Postgres{}
	confPg.pgEnv()

	confPg.connPgloop()

	time.Sleep(50 * time.Millisecond)
	return confPg
}

func rabbitMainConnect(offset int) Rabbit {
	configRabbit := Rabbit{}

	configRabbit.streamOffset = offset
	if configRabbit.streamOffset > 0 {
		configRabbit.streamOffset += 1
	}

	configRabbit.rabbitEnv()
	configRabbit.connRabbitloop()

	return configRabbit
}

func worker() {
	confPg := pgMainConnect()
	configRabbit := rabbitMainConnect(confPg.getOffset())

	m, err := configRabbit.Consumer()
	if err != nil {
		log.Printf("Failed to register a consumer: %v\n", err)
		configRabbit.isReadyConn = false
		return
	}
	// failOnError(err, "Failed to register a consumer")

	for d := range m {
		offset := d.Headers["x-stream-offset"].(int64)
		log.Printf("Received a message %d", offset)
		err := confPg.requestDb(d.Body, offset)
		if err == nil {
			log.Printf("Данные записаны в БД")
			d.Ack(true)
		} else {
			return
		}

	}
	return
}

func main() {
	for {
		worker()
		time.Sleep(50 * time.Millisecond)
	}

}
