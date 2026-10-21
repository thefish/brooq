// Приёмка возврата истёкших аренд (задача 3.4, D8): ReapExpired
// возвращает в pending только взятые задания с истёкшей арендой, не
// трогает счётчик попыток и идемпотентен — повторный вызов то же задание
// второй раз не возвращает. Терминальные задания с остатком аренды от
// прошлой попытки не воскрешаются: за это отвечает guard
// state = 'processing' в reapExpiredSQL.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/queue"
)

// leaseRow — снимок строки для приёмки сборщика: к полям jobRow добавлен
// locked_until, потому что возврат аренды обязан обнулить и его.
type leaseRow struct {
	state       string
	attempts    int
	availableAt time.Time
	updatedAt   time.Time
	lockedBy    *string
	lockedUntil *time.Time
}

// TestReapExpired — контракт возврата истёкших аренд на подготовленных
// строках: какие строки возвращаются, что с ними происходит и чего
// сборщик не делает.
func TestReapExpired(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const (
		// worker — воркер, держащий аренду в подготовленных строках.
		worker = "w-reap"
		// clockWindow — допуск на разницу между временем Go и базы.
		clockWindow = 5 * time.Second
	)

	// Таблица чистится целиком: сборщик не фильтрует по теме, поэтому
	// строки других тестов пакета исказили бы и результат возврата, и
	// проверку «пустая таблица». `go test -count=N` повторяет тест в той
	// же схеме, так что чистка нужна и по этой причине.
	_, err := pool.Exec(ctx, `DELETE FROM job`)
	require.NoError(t, err)

	// putJob кладёт задание и отдаёт его id. lockedIn == 0 — аренды нет
	// (locked_by и locked_until в NULL); отрицательное значение — аренда
	// истекла, положительное — живая. available_at в прошлом, чтобы
	// единственным отличием кейсов была аренда.
	putJob := func(t *testing.T, topic string, state queue.State, attempts int, lockedIn time.Duration) string {
		t.Helper()

		var (
			lockedBy *string
			until    *time.Time
		)
		if lockedIn != 0 {
			owner := worker
			lockedBy = &owner
			u := time.Now().Add(lockedIn)
			until = &u
		}

		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, attempts,
			                  available_at, locked_by, locked_until)
			 VALUES (gen_random_uuid(), $1, '{}'::jsonb, $2, $3,
			         now() - interval '1 hour', $4, $5)
			 RETURNING id::text`,
			topic, string(state), attempts, lockedBy, until,
		).Scan(&id))
		return id
	}

	// reap — один тик сборщика: транзакция, возврат, фиксация (то же, что
	// Queue.reapOnce, но без горутины и таймера).
	reap := func(t *testing.T) []queue.ReapedJob {
		t.Helper()

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция сборщика")
		defer func() { _ = tx.Rollback(ctx) }()

		reaped, err := repo.ReapExpired(ctx, tx)
		require.NoError(t, err, "возврат истёкших аренд — не ошибка")
		require.NoError(t, tx.Commit(ctx))
		return reaped
	}

	// readJob читает строку целиком.
	readJob := func(t *testing.T, id string) leaseRow {
		t.Helper()

		var row leaseRow
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, attempts, available_at, updated_at, locked_by, locked_until
			   FROM job WHERE id = $1`, id,
		).Scan(&row.state, &row.attempts, &row.availableAt, &row.updatedAt,
			&row.lockedBy, &row.lockedUntil))
		return row
	}

	// reapedIDs — идентификаторы возвращённых заданий: проверка не
	// зависит от порядка RETURNING.
	reapedIDs := func(reaped []queue.ReapedJob) []string {
		ids := make([]string, 0, len(reaped))
		for _, job := range reaped {
			ids = append(ids, job.ID)
		}
		return ids
	}

	tests := []struct {
		name string
		// topic — тема задания кейса.
		topic string
		// state — состояние задания до возврата.
		state queue.State
		// attempts — счётчик попыток до возврата.
		attempts int
		// lockedIn — сдвиг locked_until от «сейчас» (0 — аренды нет).
		lockedIn time.Duration
		// wantReaped — возвращается ли задание в pending.
		wantReaped bool
	}{
		{
			name:       "истёкшая аренда возвращает задание в pending",
			topic:      "t-reap-expired",
			state:      queue.StateProcessing,
			attempts:   2,
			lockedIn:   -time.Minute,
			wantReaped: true,
		},
		{
			name:     "живая аренда не возвращается",
			topic:    "t-reap-alive",
			state:    queue.StateProcessing,
			attempts: 1,
			lockedIn: time.Hour,
		},
		{
			name:     "processing без срока аренды не возвращается",
			topic:    "t-reap-null-lease",
			state:    queue.StateProcessing,
			attempts: 1,
		},
		{
			name:     "done с истёкшей арендой не воскрешается",
			topic:    "t-reap-done",
			state:    queue.StateDone,
			attempts: 3,
			lockedIn: -time.Minute,
		},
		{
			name:     "dead с истёкшей арендой не воскрешается",
			topic:    "t-reap-dead",
			state:    queue.StateDead,
			attempts: 3,
			lockedIn: -time.Minute,
		},
		{
			name:     "canceled с истёкшей арендой не воскрешается",
			topic:    "t-reap-canceled",
			state:    queue.StateCanceled,
			attempts: 1,
			lockedIn: -time.Minute,
		},
		{
			// failed — не терминальное состояние, но и не processing:
			// guard сборщика обязан оставить его в покое, иначе возврат
			// перебивал бы назначенный повтор.
			name:     "failed с истёкшей арендой не возвращается",
			topic:    "t-reap-failed",
			state:    queue.StateFailed,
			attempts: 2,
			lockedIn: -time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := putJob(t, tc.topic, tc.state, tc.attempts, tc.lockedIn)
			before := readJob(t, id)

			reaped := reap(t)

			if !tc.wantReaped {
				assert.NotContains(t, reapedIDs(reaped), id, "задание не попадает в возврат")
				after := readJob(t, id)
				assert.Equal(t, before.state, after.state, "состояние не меняется")
				assert.Equal(t, before.attempts, after.attempts, "счётчик попыток не меняется")
				assert.True(t, before.updatedAt.Equal(after.updatedAt),
					"updated_at не меняется: строку никто не трогал")
				assert.Equal(t, before.lockedBy, after.lockedBy, "владелец аренды не меняется")
				assert.Equal(t, before.lockedUntil, after.lockedUntil, "срок аренды не меняется")
				return
			}

			require.Contains(t, reapedIDs(reaped), id, "задание с истёкшей арендой возвращено")
			for _, job := range reaped {
				if job.ID == id {
					assert.Equal(t, queue.Topic(tc.topic), job.Topic,
						"тема возвращённого задания — для журнала вызывающего")
				}
			}

			after := readJob(t, id)
			assert.Equal(t, string(queue.StatePending), after.state, "задание вернулось в pending")
			assert.Nil(t, after.lockedBy, "владелец аренды обнулён")
			assert.Nil(t, after.lockedUntil, "срок аренды обнулён")
			assert.Equal(t, tc.attempts, after.attempts,
				"счётчик попыток не меняется: истекла аренда, а не попытка")
			assert.False(t, after.availableAt.After(time.Now().Add(clockWindow)),
				"available_at не в будущем: сама аренда и есть backoff (D8), получено %s", after.availableAt)
			assert.False(t, before.updatedAt.Equal(after.updatedAt), "строка обновлена возвратом")
		})
	}

	t.Run("два последовательных вызова возвращают задание ровно один раз", func(t *testing.T) {
		id := putJob(t, "t-reap-twice", queue.StateProcessing, 1, -time.Minute)

		first := reap(t)
		require.Contains(t, reapedIDs(first), id, "первый вызов возвращает задание")
		afterFirst := readJob(t, id)
		require.Equal(t, string(queue.StatePending), afterFirst.state)

		second := reap(t)
		assert.NotContains(t, reapedIDs(second), id, "второй вызов то же задание не возвращает")
		assert.Empty(t, second, "возвращать больше нечего")

		afterSecond := readJob(t, id)
		assert.Equal(t, string(queue.StatePending), afterSecond.state, "состояние осталось pending")
		assert.True(t, afterFirst.updatedAt.Equal(afterSecond.updatedAt),
			"второй вызов строку не трогает: updated_at %s против %s",
			afterFirst.updatedAt, afterSecond.updatedAt)
		assert.True(t, afterFirst.availableAt.Equal(afterSecond.availableAt),
			"available_at не сдвигается повторно")
		assert.Equal(t, afterFirst.attempts, afterSecond.attempts, "счётчик попыток не меняется")
	})

	t.Run("возврат не меняет счётчик попыток", func(t *testing.T) {
		id := putJob(t, "t-reap-attempts", queue.StateProcessing, 2, -time.Minute)

		require.Contains(t, reapedIDs(reap(t)), id)

		after := readJob(t, id)
		assert.Equal(t, 2, after.attempts,
			"было 2 — осталось 2: аренда истекла у упавшего воркера, попытка не исчерпана (инвариант 7)")
	})

	t.Run("две истёкшие аренды возвращаются обе", func(t *testing.T) {
		first := putJob(t, "t-reap-many-a", queue.StateProcessing, 1, -2*time.Minute)
		second := putJob(t, "t-reap-many-b", queue.StateProcessing, 1, -time.Minute)

		ids := reapedIDs(reap(t))
		assert.Contains(t, ids, first, "первое задание возвращено")
		assert.Contains(t, ids, second, "второе задание возвращено")
	})

	t.Run("пустая таблица — пустой результат без ошибки", func(t *testing.T) {
		_, err := pool.Exec(ctx, `DELETE FROM job`)
		require.NoError(t, err)

		// reap внутри требует отсутствия ошибки: пустая таблица — не сбой.
		assert.Empty(t, reap(t), "возвращать нечего")
	})
}
