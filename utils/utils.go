package utils

import (
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
)

func GetEnvStr(key string) string {
	value, exists := os.LookupEnv(key)
	if exists {
		log.Info(value)
		return value
	}
	log.Panic(fmt.Sprintf("Environment variable not found: %s", key))
	return ""
}

func GetEnvInt(key string) int {
	if value, exists := os.LookupEnv(key); exists {
		val, err := strconv.Atoi(value)
		if err != nil {
			log.Fatal(err)
		}
		return val
	}
	log.Panic(fmt.Sprintf("Environment variable not found: %s", key))
	return 0
}
