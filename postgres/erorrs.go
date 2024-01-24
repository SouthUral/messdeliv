package postgres

// // функция создает ошибку типа queryError
// func createQueryError(err error) queryError {
// 	queryErr := queryError{
// 		msg: err.Error(),
// 	}
// 	return queryErr
// }

// ошибка запроса к БД
type queryError struct {
	msg string
}

func (e queryError) Error() string {
	return "queryRow failed :"
}

// ошибка подключения к БД
type connectDBError struct {
}

func (e connectDBError) Error() string {
	return "there is no connection to the database"
}

// ошибка приведения типов
type typeConversionError struct {
}

func (e typeConversionError) Error() string {
	return "type conversion error"
}

// ошибка, кончились попытки подключения к БД
type endConnectAttemptsError struct {
}

func (e endConnectAttemptsError) Error() string {
	return "attempts to connect to the database have ended"
}
