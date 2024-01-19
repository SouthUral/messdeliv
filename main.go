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
	pgEnvs, rbEnvs := env.LoadEnvs()
	rabbit := rb.InitRb(*rbEnvs)
	pg.InitPg(*pgEnvs, rabbit.GetChan())
	rabbit.StartRb()
	time.Sleep(2 * time.Minute)
}
