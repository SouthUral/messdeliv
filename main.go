package main

import (
	"os"
	"time"

	"github.com/joho/godotenv"

	pg "messdelive/postgres"
	rb "messdelive/rabbit"

	log "github.com/sirupsen/logrus"
)

func init() {
	// логи в формате JSON, по умолчанию формат ASCII
	log.SetFormatter(&log.JSONFormatter{})

	// логи идут на стандартный вывод, их можно перенаправить в файл
	log.SetOutput(os.Stdout)

	// установка уровня логирования
	log.SetLevel(log.InfoLevel)

	// loads values from .env into the system
	if err := godotenv.Load(".env"); err != nil {
		log.Print("No .env file found")
	}
}

func worker() {
	defer log.Warning("worker закончил работу")
	confPg := pg.PgMainConnect()
	configRabbit := rb.RabbitMainConnect(confPg.GetOffset())

	m, err := configRabbit.Consumer()
	if err != nil {
		log.Printf("Failed to register a consumer: %v\n", err)
		configRabbit.IsReadyConn = false
		return
	}

	for d := range m {
		offset := d.Headers["x-stream-offset"].(int64)
		log.Printf("Received a message %d", offset)
		err := confPg.RequestDb(d.Body, offset)
		if err == nil {
			log.Printf("Данные записаны в БД")
			d.Ack(true)
		} else {
			log.Error(err, "сообщение об ошибке")
			return
		}

	}
}

func main() {
	for {
		worker()
		time.Sleep(50 * time.Millisecond)
	}

}
