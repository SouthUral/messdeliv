package main

import (
	"os"
	"time"

	_ "net/http/pprof"

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
	defer time.Sleep(15 * time.Second)
	pgEnvs, rbEnvs := env.LoadEnvs()
	rbMain := rb.InitRabbit(rbEnvs.GetUrl(), rbEnvs.GetNameQueue(), "test_2", 30)
	pgMain, ctxPg := pg.InitPg(
		*pgEnvs,
		rbMain.GetChan(),
		1000, // время ожидания между попытками запроса к БД (в миллисекундах)
		20,   // количество попыток запроса к БД
		30,   // количество попыток переподключения к БД
		3,    // время между попытками переподключения (в секундах)
	)
	ctxRb := rbMain.StartRb()
	for {
		select {
		case <-ctxPg.Done():
			log.Info("pg done")
			rbMain.RabbitShutdown(pg.PostgresShutdownError{})
			return
		case <-ctxRb.Done():
			log.Info("rabbit done")
			pgMain.PostgresShutdown(rb.RabbitShutdownError{})
			return
		default:
			// http.ListenAndServe(":9090", nil)
			time.Sleep(50 * time.Millisecond)
		}
	}
}
