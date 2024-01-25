package main

import (
	"os"
	"time"

	"github.com/joho/godotenv"

	env "messdeliv/env"
	pg "messdeliv/postgres"
	rb "messdeliv/rabbit"

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

func main() {
	defer log.Info("messDeliv finished the job")
	pgEnvs, rbEnvs := env.LoadEnvs()
	rbMain := rb.InitRb(*rbEnvs)
	pgMain, ctxPg := pg.InitPg(
		*pgEnvs,
		rbMain.GetChan(),
		1000, // время ожидания между попытками запроса к БД (в миллисекундах)
		20,   // количество попыток запроса к БД
		30,   // количество попыток переподключения к БД
		5,    // время между попытками переподключения (в секундах)
	)
	ctxRb := rbMain.StartRb(
		20, // количество попыток реконнекта к RabbitMQ
		20, // количество попыток создать потребителя
		3,  // время ожидания между реконнектами к RabbitMQ (секунды)
		5,  // время ожидания между проверками состояния потребителя (секунды)
		1,  // ожидание между проверками сообщения из очереди RabbitMQ (миллисекунды)
		5,  // ожидание сообщения из очереди в случае возникновения ошибки (секунды)
		30, // время ожидания ответа от БД, рекомендуется поставить 5-30 секунд (секунды)
	)
	for {
		select {
		case <-ctxPg.Done():
			rbMain.RabbitShutdown(pg.PostgresShutdownError{})
			return
		case <-ctxRb.Done():
			pgMain.PostgresShutdown(rb.RabbitShutdownError{})
			return
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}
