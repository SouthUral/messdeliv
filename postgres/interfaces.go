package postgres

type envs interface {
	GetUrl() string
	GetRecProcedure() string
	GetOffsetFunc() string
}
