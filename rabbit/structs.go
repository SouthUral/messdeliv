package rabbit

import (
	"fmt"
)

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

func (r RbEnvs) getUrl() string {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/%s", r.User, r.Password, r.Host, r.Port, r.VHost)
	return url
}
