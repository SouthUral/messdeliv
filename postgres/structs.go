package postgres

import (
	"fmt"
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

func (p PgEnvs) getUrl() string {
	url := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", p.User, p.Password, p.Host, p.Port, p.DataBaseName)
	return url
}
