// Адаптер pgx.Tx к порту queue.Tx: драйвер не проникает в pkg/queue,
// поэтому приведение тега команды и строк делает этот файл.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/thefish/brooq/pkg/queue"
)

// Tx — транзакция pgx, приведённая к порту queue.Tx. pgx.Row и pgx.Rows
// удовлетворяют порту как есть; обёртка нужна только из-за типа тега
// команды (pgconn.CommandTag против queue.CommandTag).
type Tx struct {
	tx pgx.Tx
}

// Порядок проверки: адаптер обязан удовлетворять порту движка.
var _ queue.Tx = (*Tx)(nil)

// wrapTx оборачивает транзакцию драйвера в порт.
func wrapTx(tx pgx.Tx) *Tx {
	return &Tx{tx: tx}
}

// WrapTx оборачивает транзакцию драйвера в порт queue.Tx, чтобы
// потребитель мог поставить задание в собственной транзакции: его
// SQL-слой и очередь делят одну транзакцию. Возвращается конкретный
// тип: он же удовлетворяет queue.Tx, а через Pgx доступна обратная
// сторона.
func WrapTx(tx pgx.Tx) *Tx { return wrapTx(tx) }

// Exec выполняет оператор, не возвращающий строки.
func (t *Tx) Exec(ctx context.Context, sql string, args ...any) (queue.CommandTag, error) {
	tag, err := t.tx.Exec(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return tag, nil
}

// Query выполняет оператор, возвращающий строки.
func (t *Tx) Query(ctx context.Context, sql string, args ...any) (queue.Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

// QueryRow выполняет оператор, возвращающий одну строку.
func (t *Tx) QueryRow(ctx context.Context, sql string, args ...any) queue.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

// Begin открывает вложенную транзакцию (точку сохранения).
func (t *Tx) Begin(ctx context.Context) (queue.Tx, error) {
	nested, err := t.tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return wrapTx(nested), nil
}

// Commit фиксирует транзакцию.
func (t *Tx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

// Rollback откатывает транзакцию; после Commit ошибка не аварийна —
// вызывающий вправе отложить откат безусловно.
func (t *Tx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// Pgx возвращает транзакцию драйвера, в которую обёрнут порт. Это та же
// транзакция, а не копия: Commit или Rollback через неё закрывает её для
// всех, кто её делит. Владеет транзакцией тот, кто её открыл. Для
// нулевого или nil *Tx возвращается nil.
func (t *Tx) Pgx() pgx.Tx {
	if t == nil {
		return nil
	}
	return t.tx
}

// AsPgx достаёт транзакцию драйвера из порта queue.Tx. Нужен там, где
// порт пришёл из движка — из Queue.Begin или из Claim.OnSucceeded: на
// значении порта Pgx не вызвать без приведения типа. Второй результат —
// false, если tx создан не этим пакетом, равен nil или пуст.
func AsPgx(tx queue.Tx) (pgx.Tx, bool) {
	wrapped, ok := tx.(*Tx)
	if !ok {
		return nil, false
	}
	inner := wrapped.Pgx()
	if inner == nil {
		return nil, false
	}
	return inner, true
}
