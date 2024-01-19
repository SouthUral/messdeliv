package rabbit

type envs interface {
	GetUrl() string
	GetNameQueue() string
}

type answerEvent interface {
	GetOffset() int
}
