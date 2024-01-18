package env

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

type PgEnvs struct {
	Host               string `json:"host"`
	Port               string `json:"port"`
	User               string `json:"user"`
	Password           string `json:"password"`
	DataBaseName       string `json:"db_name"`
	RecordingProcedure string `json:"rec_procedure"`
	OffsetFunc         string `json:"offset_func"`
}

func (p PgEnvs) GetEnvKeys() map[string]string {
	keysEnv := map[string]string{
		"host":          "ASD_POSTGRES_HOST",
		"port":          "ASD_POSTGRES_PORT",
		"user":          "SERVICE_PG_ILOGIC_USERNAME",
		"password":      "SERVICE_PG_ILOGIC_PASSWORD",
		"db_name":       "ASD_POSTGRES_DBNAME",
		"rec_procedure": "SERVICE_PG_PROCEDURE",
		"offset_func":   "SERVICE_PG_GETOFFSET",
	}
	return keysEnv
}

func (p PgEnvs) GetRecProcedure() string {
	return p.RecordingProcedure
}

func (p PgEnvs) GetOffsetFunc() string {
	return p.OffsetFunc
}

func (p PgEnvs) GetUrl() string {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", p.User, p.Password, p.Host, p.Port, p.DataBaseName)
	return url
}

type RbEnvs struct {
	Host      string `json:"host"`
	Port      string `json:"port"`
	User      string `json:"user"`
	Password  string `json:"password"`
	VHost     string `json:"v_host"`
	Heartbeat string `json:"heartbeat"`
	NameQueue string `json:"name_queue"`
}

func (r RbEnvs) GetEnvKeys() map[string]string {
	keysEnv := map[string]string{
		"host":       "ASD_RMQ_HOST",
		"port":       "ASD_RMQ_PORT",
		"user":       "SERVICE_RMQ_ENOTIFY_USERNAME",
		"password":   "SERVICE_RMQ_ENOTIFY_PASSWORD",
		"v_host":     "ASD_RMQ_VHOST",
		"heartbeat":  "ASD_RMQ_HEARTBEAT",
		"name_queue": "SERVICE_RMQ_QUEUE",
	}
	return keysEnv
}

func (r RbEnvs) GetNameQueue() string {
	return r.NameQueue
}

func (r RbEnvs) GetUrl() string {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", r.User, r.Password, r.Host, r.Port, r.VHost)
	return url
}

type EnvLoader struct {
	unloadedVariables []string
}

func InitEnvLoader() *EnvLoader {
	res := &EnvLoader{
		unloadedVariables: make([]string, 0, 0),
	}
	return res
}

// метод загрузки переменных окружения
func (e *EnvLoader) Load(keys map[string]string) map[string]string {
	res := make(map[string]string, len(keys))

	for key, envKey := range keys {
		value, exists := os.LookupEnv(envKey)
		if exists {
			res[key] = value
		} else {
			e.unloadedVariables = append(e.unloadedVariables, envKey)
			log.Warningf("the %s variable is not loaded", envKey)
		}
	}
	return res
}

// метод проверки, были ли незагруженные переменные
func (e *EnvLoader) CheckUnloadEnvs() bool {
	if len(e.unloadedVariables) > 0 {
		return false
	}
	return true
}
