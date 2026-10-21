package queue

import "context"

// CommandTag — результат выполнения оператора: сколько строк затронуто.
// Интерфейс минимальный, чтобы его удовлетворял тег драйвера без
// импорта самого драйвера.
type CommandTag interface {
	// RowsAffected — число затронутых оператором строк.
	RowsAffected() int64
}

// Row — одна строка результата запроса.
type Row interface {
	// Scan разбирает строку в переданные адресаты.
	Scan(dest ...any) error
}

// Rows — курсор результата запроса.
type Rows interface {
	// Next переходит к следующей строке; false — строк больше нет.
	Next() bool
	// Scan разбирает текущую строку в переданные адресаты.
	Scan(dest ...any) error
	// Err — ошибка, возникшая при обходе курсора.
	Err() error
	// Close освобождает ресурсы курсора.
	Close()
}

// Tx — транзакция, в которой производитель кладёт задание, а обработчик
// ставит следующее (порт по D2).
//
// Порт структурный и объявлен здесь, а не в pkg/postgres: pkg/queue
// не имеет права импортировать драйвер. Набор методов — ровно
// тот, что нужен движку: выполнение, чтение, вложенная транзакция и
// завершение. Тип, чьи методы совпадают с этим набором, удовлетворяет
// порту на этапе компиляции; для pgx.Tx приведение `pgconn.CommandTag`
// к CommandTag делает тонкий адаптер в pkg/postgres — сам драйвер
// сюда не проникает.
type Tx interface {
	// Exec выполняет оператор, не возвращающий строки.
	Exec(ctx context.Context, sql string, args ...any) (CommandTag, error)
	// Query выполняет оператор, возвращающий строки.
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	// QueryRow выполняет оператор, возвращающий одну строку.
	QueryRow(ctx context.Context, sql string, args ...any) Row
	// Begin открывает вложенную транзакцию.
	Begin(ctx context.Context) (Tx, error)
	// Commit фиксирует транзакцию.
	Commit(ctx context.Context) error
	// Rollback откатывает транзакцию; после Commit ошибка не считается
	// аварийной.
	Rollback(ctx context.Context) error
}
