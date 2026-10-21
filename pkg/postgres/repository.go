// Хранилище очереди: конструктор, транзакции и общие хелперы записи.
// Здесь и только здесь живёт SQL; записи идут через pgx.Tx,
// чтения — через пул.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thefish/brooq/pkg/queue"
)

// Repository — хранилище очереди поверх пула соединений PostgreSQL.
// Методы не держат состояния: пул один на процесс, транзакцию задаёт
// вызывающий.
type Repository struct {
	pool *pgxpool.Pool
}

// New возвращает хранилище поверх готового пула соединений. Пул
// создаётся снаружи (pkg/db) — хранилище его не открывает и не закрывает.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Begin открывает транзакцию записи и оборачивает её в порт queue.Tx,
// чтобы её можно было передать и в методы хранилища, и в движок. Тип
// возврата — порт, а не *Tx: так хранилище удовлетворяет интерфейсу
// движка (pkg/brooq.Repo) и подменяется фейком в тестах.
func (r *Repository) Begin(ctx context.Context) (queue.Tx, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("открытие транзакции: %w", err)
	}
	return wrapTx(tx), nil
}

// execRows выполняет оператор записи и возвращает число затронутых
// строк: ноль — это «переход не состоялся» (например, задание уже
// терминальное), а не ошибка.
func execRows(ctx context.Context, tx queue.Tx, sql string, args ...any) (int64, error) {
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
