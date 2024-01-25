package env

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

// метод сопоставления и загрузки переменных из map[string]string в соответствующую структуру
func uploadingStruct[T *PgEnvs | *RbEnvs](envs map[string]string, envType T) error {
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
func LoadEnvs() (*PgEnvs, *RbEnvs) {
	var err error

	envLoader := InitEnvLoader()

	pgEnvs := &PgEnvs{}
	rbEnvs := &RbEnvs{}

	envPg := envLoader.Load(pgEnvs.GetEnvKeys())
	envRb := envLoader.Load(rbEnvs.GetEnvKeys())

	if !envLoader.CheckUnloadEnvs() {
		log.Fatal("not all environment variables are loaded")
	}

	err = uploadingStruct[*PgEnvs](envPg, pgEnvs)
	if err != nil {
		log.Fatal("error loading data into the structure pgEnvs")
	}

	err = uploadingStruct[*RbEnvs](envRb, rbEnvs)
	if err != nil {
		log.Fatal("error loading data into the structure RbEnvs")
	}

	return pgEnvs, rbEnvs
}
