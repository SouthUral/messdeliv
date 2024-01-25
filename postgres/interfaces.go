package postgres

type envs interface {
	GetUrl() string
	GetRecProcedure() string
	GetOffsetFunc() string
}

type msgEvent interface {
	GetTypeMsg() string
	GetReverceCh() chan interface{}
	GetMsg() []byte
	GetOffset() int64
}
