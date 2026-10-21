// Тесты экспорта транзакции драйвера: обёртка pgx.Tx в порт queue.Tx
// (WrapTx), обратный доступ к драйверу (Pgx, AsPgx). Главное обещание
// обёртки — она не копирует транзакцию: задание, поставленное через неё,
// уходит в ту самую чужую транзакцию, поэтому видно ей до фиксации,
// появляется в пуле только после Commit, а откат не оставляет строки.
// Проверяется наблюдаемый результат, а не форма кода: подмена обёртки
// копией (собственная транзакция внутри WrapTx) роняет эти тесты.
package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/queue"
)

// foreignTx — минимальная заглушка «чужой» реализации queue.Tx: порт она
// удовлетворяет, но создана не этим пакетом, поэтому AsPgx обязан
// вернуть false. Тела методов не вызываются — обёртка чужих транзакций
// не разворачивает.
type foreignTx struct{}

var _ queue.Tx = (*foreignTx)(nil)

// Exec заглушки: чужая транзакция не исполняется.
func (*foreignTx) Exec(context.Context, string, ...any) (queue.CommandTag, error) {
	panic("не используется")
}

// Query заглушки: чужая транзакция не читается.
func (*foreignTx) Query(context.Context, string, ...any) (queue.Rows, error) {
	panic("не используется")
}

// QueryRow заглушки: чужая транзакция не читается.
func (*foreignTx) QueryRow(context.Context, string, ...any) queue.Row {
	panic("не используется")
}

// Begin заглушки: вложенная транзакция не открывается.
func (*foreignTx) Begin(context.Context) (queue.Tx, error) {
	panic("не используется")
}

// Commit заглушки: чужая транзакция не фиксируется.
func (*foreignTx) Commit(context.Context) error {
	panic("не используется")
}

// Rollback заглушки: чужая транзакция не откатывается.
func (*foreignTx) Rollback(context.Context) error {
	panic("не используется")
}

// TestWrapTxEnqueueInOwnTransaction — сценарий «чужая транзакция
// драйвера становится транзакцией очереди»: обёрнутая транзакция — та же
// самая (тождество, а не копия), задание пишется в неё, видно внутри неё
// до фиксации, появляется в пуле после Commit и исчезает при откате.
func TestWrapTxEnqueueInOwnTransaction(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	// reset убирает строки темы: `go test -count=N` повторяет тест в той
	// же схеме, и строки прошлого прогона исказили бы счёт.
	reset := func(t *testing.T, topic queue.Topic) {
		t.Helper()
		_, err := pool.Exec(ctx, `DELETE FROM job WHERE topic = $1`, string(topic))
		require.NoError(t, err)
	}

	// countJobs считает задания темы в таблице.
	countJobs := func(t *testing.T, topic queue.Topic) int {
		t.Helper()
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic)).Scan(&n))
		return n
	}

	// readJob читает строку задания через пул; ok == false — строки с
	// таким id нет (в том числе пока она не зафиксирована).
	readJob := func(t *testing.T, id string) (jobRow, bool) {
		t.Helper()

		var row jobRow
		err := pool.QueryRow(ctx,
			`SELECT state, attempts, last_error, available_at, updated_at, locked_by
			   FROM job WHERE id = $1`, id,
		).Scan(&row.state, &row.attempts, &row.lastError, &row.availableAt, &row.updatedAt, &row.lockedBy)
		if errors.Is(err, pgx.ErrNoRows) {
			return jobRow{}, false
		}
		require.NoError(t, err)
		return row, true
	}

	t.Run("задание уходит в чужую транзакцию", func(t *testing.T) {
		const topic = queue.Topic("t-wraptx-own")
		reset(t, topic)

		tx, err := pool.Begin(ctx)
		require.NoError(t, err, "транзакция драйвера")
		defer func() { _ = tx.Rollback(ctx) }()

		// Тождество: обёртка держит ту же транзакцию, а не её копию.
		wrapped := WrapTx(tx)
		require.True(t, wrapped.Pgx() == tx, "WrapTx не копирует транзакцию")

		inner, ok := AsPgx(wrapped)
		require.True(t, ok, "транзакция драйвера достаётся из обёртки")
		assert.True(t, inner == tx, "AsPgx вернул ту же транзакцию драйвера")

		id, created, err := repo.EnqueueTx(ctx, wrapped, queue.Job{
			Topic:          topic,
			Payload:        queue.Payload(`{"письмо":1}`),
			IdempotencyKey: "k-wraptx-own",
		})
		require.NoError(t, err, "постановка в чужой транзакции")
		assert.True(t, created, "строка создана этим вызовом")
		assert.NotEmpty(t, id, "id непустой при err == nil")

		// До фиксации строка видна только этой транзакции: читаем её
		// прямым драйвером, а не пулом. Так доказывается, что запись
		// действительно ушла в чужую транзакцию.
		var topicInTx string
		require.NoError(t, tx.QueryRow(ctx,
			`SELECT topic FROM job WHERE id = $1`, id).Scan(&topicInTx),
			"внутри чужой транзакции задание записано")
		assert.Equal(t, string(topic), topicInTx, "тема задания в чужой транзакции")

		_, visible := readJob(t, id)
		assert.False(t, visible, "до Commit строка снаружи не видна")

		require.NoError(t, tx.Commit(ctx), "фиксация чужой транзакции")

		row, ok := readJob(t, id)
		require.True(t, ok, "после Commit задание на месте")
		assert.Equal(t, string(queue.StatePending), row.state, "новое задание ждёт взятия")
		assert.Equal(t, 1, countJobs(t, topic), "в очереди одно задание")
	})

	t.Run("откат чужой транзакции не оставляет задания", func(t *testing.T) {
		const topic = queue.Topic("t-wraptx-rollback")
		reset(t, topic)

		tx, err := pool.Begin(ctx)
		require.NoError(t, err, "транзакция драйвера")
		defer func() { _ = tx.Rollback(ctx) }()

		id, created, err := repo.EnqueueTx(ctx, WrapTx(tx), queue.Job{
			Topic:          topic,
			Payload:        queue.Payload(`{"письмо":1}`),
			IdempotencyKey: "k-wraptx-rollback",
		})
		require.NoError(t, err, "постановка в чужой транзакции")
		assert.NotEmpty(t, id, "id известен до фиксации")
		assert.True(t, created)

		// Внутри транзакции задание видно: иначе проверять откат было бы
		// нечего.
		var inTx int
		require.NoError(t, tx.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic)).Scan(&inTx))
		require.Equal(t, 1, inTx, "внутри чужой транзакции задание записано")

		require.NoError(t, tx.Rollback(ctx), "откат чужой транзакции")

		_, ok := readJob(t, id)
		assert.False(t, ok, "задания с полученным id в очереди нет")
		assert.Equal(t, 0, countJobs(t, topic), "откат не оставил строк")
	})
}

// TestAsPgx — разворачивание порта обратно в транзакцию драйвера и
// безопасность Pgx на пустых значениях. Порт может прийти из движка
// (Queue.Begin, Claim.OnSucceeded) или быть чужим; в этих случаях второй
// результат false, а nil-получатель не паникует.
func TestAsPgx(t *testing.T) {
	// Таблица значений порта, из которых транзакцию драйвера не достать:
	// nil, типизированный nil, пустая обёртка, обёртка nil-транзакции и
	// чужая реализация порта.
	for _, tc := range []struct {
		name string
		tx   queue.Tx
	}{
		{"nil порт", nil},
		{"типизированный nil *Tx", (*Tx)(nil)},
		{"пустая обёртка", &Tx{}},
		{"обёртка nil-транзакции", WrapTx(nil)},
		{"чужой порт", &foreignTx{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner, ok := AsPgx(tc.tx)
			assert.False(t, ok, "транзакции драйвера нет")
			assert.Nil(t, inner, "при ok == false возвращается nil")
		})
	}

	// Pgx на пустых значениях возвращает nil, а не паникует: вызывающий
	// вправе спросить драйвер, не проверяя обёртку на nil.
	t.Run("Pgx на nil и пустой обёртке", func(t *testing.T) {
		var nilTx *Tx
		assert.Nil(t, nilTx.Pgx(), "Pgx nil-получателя — nil")
		assert.Nil(t, (&Tx{}).Pgx(), "Pgx пустой обёртки — nil")
	})
}
