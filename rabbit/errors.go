package rabbit

// ошибка создания потребителя
type consumCreateError struct {
}

func (e consumCreateError) Error() string {
	return "the consumer is not created"
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

// ошибки связанные с rabbitConn{}

// RabbitConn не определен
type rabbitConnNotDefineError struct {
}

func (e rabbitConnNotDefineError) Error() string {
	return "rabbitConn is not define"
}

// коннект к RabbitMQ не готов
type connRabbitNotReadyError struct {
}

func (e connRabbitNotReadyError) Error() string {
	return "connect RabbitMQ is not ready"
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

// канал RabbitMQ не определен
type chanRabbitNotDefineError struct {
}

func (e chanRabbitNotDefineError) Error() string {
	return "the RabbitMQ channel is not defined"
}

// канал RabbitMQ закрыт
type chanRabbitIsClosedError struct {
}

func (e chanRabbitIsClosedError) Error() string {
	return "the RabbitMQ channel is closed"
}

// ошибка создания канала
type createChanRabbitError struct {
}

func (e createChanRabbitError) Error() string {
	return "error creating the RabbitMQ channel"
}

// не удалось создать оффсет
type createOffsetError struct {
}

func (e createOffsetError) Error() string {
	return "error create offset"
}

// не получилось получить offset из БД
type gettingOffsetError struct {
}

func (e gettingOffsetError) Error() string {
	return "error getting offset from DB"
}
