package rabbit

// ошибка, генерирующаяся при отсутствующем событии от Consumer
type noEventError struct {
}

func (e noEventError) Error() string {
	return "the event was not received from the consumer"
}

// ошибка, если Consumer не активен
type consumerActiveError struct {
}

func (e consumerActiveError) Error() string {
	return "consumer is not active"
}

// ошибка, Consumer не определен
type consumerNotDedineError struct {
}

func (e consumerNotDedineError) Error() string {
	return "consumer has not been defined yet"
}

// Экспортируемая ошибка, Rabbit закончил работу
type RabbitShutdownError struct {
}

func (e RabbitShutdownError) Error() string {
	return "Rabbit finished the job"
}
