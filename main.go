package main

import (
	"encoding/json"
	"os"

	"github.com/joho/godotenv"

	pg "messdelive/postgres"
	rb "messdelive/rabbit"
	ut "messdelive/utils"

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

// метод сопоставления и загрузки переменных из map[string]string в соответствующую структуру
func uploadingStruct[T *pg.PgEnvs | *rb.RbEnvs](envs map[string]string, envType T) error {
	res, err := json.Marshal(envs)
	if err != nil {
		return err
	}

	err = json.Unmarshal(res, envType)
	if err != nil {
		return err
	}

	return nil
}

// загрузка переменных окружения для rabbitMQ и PostrgreSQL
func loadEnvs() (*pg.PgEnvs, *rb.RbEnvs) {
	var err error

	envLoader := ut.InitEnvLoader()

	pgEnvs := &pg.PgEnvs{}
	rbEnvs := &rb.RbEnvs{}

	envPg := envLoader.Load(pgEnvs.GetEnvKeys())
	envRb := envLoader.Load(rbEnvs.GetEnvKeys())

	if !envLoader.CheckUnloadEnvs() {
		log.Fatal("not all environment variables are loaded")
	}

	err = uploadingStruct[*pg.PgEnvs](envPg, pgEnvs)
	if err != nil {
		log.Fatal("error loading data into the structure pgEnvs")
	}

	err = uploadingStruct[*rb.RbEnvs](envRb, rbEnvs)
	if err != nil {
		log.Fatal("error loading data into the structure RbEnvs")
	}

	return pgEnvs, rbEnvs
}

func worker() {
	defer log.Warning("worker закончил работу")
	pgEnvs, rbEnvs := loadEnvs()
	confPg := pg.InitPg(*pgEnvs)
	configRabbit := rb.InitRb(*rbEnvs)

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
	worker()
}
