package utils

import (
	"os"

	log "github.com/sirupsen/logrus"
)

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
