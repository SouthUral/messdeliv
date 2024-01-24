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

// Ошибка инициализации RabbitConn
type initRabbitConnError struct {
}

func (e initRabbitConnError) Error() string {
	return "initialization error RabbitConn"
}

// ошибка коннекта к Rabbit
type connRabbitError struct {
}

func (e connRabbitError) Error() string {
	return "connect Rabbit failed"
}

// закончились попытки подключения к RabbitMQ
type connectAttemptsError struct {
}

func (e connectAttemptsError) Error() string {
	return "the number of attempts to connect to RabbitMQ has ended"
}

// ошибка создания канала
type createChanRabbitError struct {
}

func (e createChanRabbitError) Error() string {
	return "error creating the RabbitMQ channel"
}
