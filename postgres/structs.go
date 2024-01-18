package postgres

// ответное сообщение
type answerEvent struct {
	offset int
}

func (a answerEvent) GetOffset() int {
	return a.offset
}
