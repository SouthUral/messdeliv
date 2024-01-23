package main

import (
	"os"
	"time"

	"github.com/joho/godotenv"

	env "messdelive/env"
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

func main() {
	defer log.Info("messDeliv finished the job")
	pgEnvs, rbEnvs := env.LoadEnvs()
	rbMain := rb.InitRb(*rbEnvs, 20)
	pgMain, ctxPg := pg.InitPg(*pgEnvs, rbMain.GetChan(), 500, 30, 30, 3)
	ctxRb := rbMain.StartRb(20, 3, 50)
	for {
		select {
		case <-ctxPg.Done():
			rbMain.RabbitShutdown()
			return
		case <-ctxRb.Done():
			pgMain.PostgresShutdown()
			return
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}
