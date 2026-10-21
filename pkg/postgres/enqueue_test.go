// Интеграционные тесты дельты 02-enqueue-returns-id: постановка
// возвращает идентификатор задания и признак created
// (job-lifecycle/spec.md, «Постановка возвращает идентификатор задания»).
// Проверяется наблюдаемый результат вызова, а не форма SQL: сценарии
// описаны в спеке, поэтому мутации адаптера (пустой id при успехе,
// created == true на повторе, второй строке при занятом ключе) роняют
// именно эти тесты.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/queue"
)

// TestEnqueueReturnsID — сценарии дельты постановки: первая постановка,
// повтор по ключу, постановка без ключа, ключ занятый терминальным
// заданием, откат транзакции производителя.
func TestEnqueueReturnsID(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	// reset убирает строки темы: `go test -count=N` повторяет тест в той же
	// схеме, и строки прошлого прогона исказили бы счёт.
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

	// readJob читает строку задания; ok == false — строки с таким id нет.
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

	t.Run("первая постановка возвращает идентификатор", func(t *testing.T) {
		const topic = queue.Topic("t-enqueue-id-first")
		reset(t, topic)

		id, created, err := repo.Enqueue(ctx, queue.Job{
			Topic:   topic,
			Payload: queue.Payload(`{"письмо":1}`),
		})
		require.NoError(t, err, "первая постановка")
		assert.NotEmpty(t, id, "id непустой при err == nil")
		assert.True(t, created, "строка создана этим вызовом")

		row, ok := readJob(t, id)
		require.True(t, ok, "задание с полученным id присутствует в очереди")
		assert.Equal(t, string(queue.StatePending), row.state, "новое задание ждёт взятия")
		assert.Equal(t, 1, countJobs(t, topic), "в очереди одно задание")
	})

	t.Run("повтор с тем же ключом возвращает тот же идентификатор", func(t *testing.T) {
		const topic = queue.Topic("t-enqueue-id-repeat")
		reset(t, topic)

		job := queue.Job{
			Topic:          topic,
			Payload:        queue.Payload(`{"письмо":1}`),
			IdempotencyKey: "k-repeat",
		}

		first, created, err := repo.Enqueue(ctx, job)
		require.NoError(t, err, "первая постановка")
		require.NotEmpty(t, first)
		require.True(t, created)

		second, created, err := repo.Enqueue(ctx, job)
		require.NoError(t, err, "повтор с тем же ключом — не ошибка")
		assert.Equal(t, first, second, "повтор возвращает id первой постановки")
		assert.False(t, created, "второго создания строки не было")
		assert.Equal(t, 1, countJobs(t, topic), "в очереди остаётся одно задание")
	})

	t.Run("постановка без ключа даёт разные идентификаторы", func(t *testing.T) {
		const topic = queue.Topic("t-enqueue-id-nokey")
		reset(t, topic)

		job := queue.Job{Topic: topic, Payload: queue.Payload(`{"письмо":1}`)}

		first, created, err := repo.Enqueue(ctx, job)
		require.NoError(t, err, "первая постановка без ключа")
		require.NotEmpty(t, first)
		require.True(t, created)

		second, created, err := repo.Enqueue(ctx, job)
		require.NoError(t, err, "вторая постановка без ключа")
		assert.NotEmpty(t, second)
		assert.True(t, created, "задание без ключа не дедуплицирует")
		assert.NotEqual(t, first, second, "идентификаторы заданий без ключа разные")
		assert.Equal(t, 2, countJobs(t, topic), "в очереди два задания")

		_, ok := readJob(t, first)
		assert.True(t, ok, "первое задание в очереди")
		_, ok = readJob(t, second)
		assert.True(t, ok, "второе задание в очереди")
	})

	// Терминальное задание держит ключ: повтор возвращает его id, но
	// created == false — производитель не должен писать мету дважды.
	for _, state := range []queue.State{queue.StateDone, queue.StateDead, queue.StateCanceled} {
		t.Run(fmt.Sprintf("ключ занят терминальным заданием %s", state), func(t *testing.T) {
			topic := queue.Topic("t-enqueue-id-terminal-" + string(state))
			reset(t, topic)

			job := queue.Job{
				Topic:          topic,
				Payload:        queue.Payload(`{"письмо":1}`),
				IdempotencyKey: "k-terminal",
			}

			terminalID, created, err := repo.Enqueue(ctx, job)
			require.NoError(t, err, "первая постановка")
			require.NotEmpty(t, terminalID)
			require.True(t, created)

			_, err = pool.Exec(ctx, `UPDATE job SET state = $1 WHERE id = $2`, string(state), terminalID)
			require.NoError(t, err, "перевод задания в терминальное состояние")

			id, created, err := repo.Enqueue(ctx, job)
			require.NoError(t, err, "повтор по ключу терминального задания — не ошибка")
			assert.Equal(t, terminalID, id, "id терминального задания")
			assert.False(t, created, "второе задание не создаётся")
			assert.Equal(t, 1, countJobs(t, topic), "в очереди одна строка")

			row, ok := readJob(t, terminalID)
			require.True(t, ok, "терминальное задание на месте")
			assert.Equal(t, string(state), row.state, "терминальное состояние не перезаписано")
		})
	}

	t.Run("откат транзакции производителя делает идентификатор недействительным", func(t *testing.T) {
		const topic = queue.Topic("t-enqueue-id-rollback")
		reset(t, topic)

		job := queue.Job{
			Topic:          topic,
			Payload:        queue.Payload(`{"письмо":1}`),
			IdempotencyKey: "k-rollback",
		}

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция производителя")

		id, created, err := repo.EnqueueTx(ctx, tx, job)
		require.NoError(t, err, "постановка в транзакции производителя")
		assert.NotEmpty(t, id, "id известен до фиксации")
		assert.True(t, created)

		// Внутри транзакции задание видно: иначе проверять откат было бы
		// нечего.
		var inTx int
		require.NoError(t, tx.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic)).Scan(&inTx))
		require.Equal(t, 1, inTx, "внутри транзакции задание записано")

		require.NoError(t, tx.Rollback(ctx), "откат транзакции производителя")

		_, ok := readJob(t, id)
		assert.False(t, ok, "задания с полученным id в очереди нет")
		assert.Equal(t, 0, countJobs(t, topic), "откат не оставил строк")
	})

	// Ошибка постановки не должна выдавать идентификатор: иначе
	// производитель записал бы мету по несуществующему заданию.
	t.Run("ошибка постановки не возвращает идентификатор", func(t *testing.T) {
		const topic = queue.Topic("t-enqueue-id-error")
		reset(t, topic)

		id, created, err := repo.Enqueue(ctx, queue.Job{
			Topic:   topic,
			Payload: queue.Payload(`не json`),
		})
		require.Error(t, err, "невалидный payload — ошибка схемы")
		assert.Empty(t, id, "при ошибке id пустой")
		assert.False(t, created, "при ошибке created == false")
		assert.Equal(t, 0, countJobs(t, topic), "строка не создана")
	})
}

// TestEnqueueConcurrentSameKey — идемпотентность под конкуренцией: две
// горутины ставят одно и то же задание (тема + ключ) через пул. Обе
// проходят путь конфликта по индексу job_idempotency_idx, поэтому
// проверяется только инвариант — ошибок нет, ровно одна строка, один и
// тот же непустой id, ровно один created == true. Победитель гонки не
// проверяется (design.md, Risks): он не наблюдаем для производителя.
func TestEnqueueConcurrentSameKey(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	// Тема уникальна для прогона: строки не удаляются между горутинами, а
	// `-count=N` не должен видеть строки прошлого прогона.
	topic := queue.Topic(fmt.Sprintf("t-enqueue-concurrent-%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		_, err := pool.Exec(ctx, `DELETE FROM job WHERE topic = $1`, string(topic))
		require.NoError(t, err)
	})

	job := queue.Job{
		Topic:          topic,
		Payload:        queue.Payload(`{"письмо":1}`),
		IdempotencyKey: fmt.Sprintf("k-concurrent-%d", time.Now().UnixNano()),
	}

	type result struct {
		id      string
		created bool
		err     error
	}

	const goroutines = 2
	results := make(chan result, goroutines)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // горутины стартуют одновременно
			id, created, err := repo.Enqueue(ctx, job)
			results <- result{id: id, created: created, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	var got []result
	for r := range results {
		got = append(got, r)
	}
	require.Len(t, got, goroutines, "оба вызова вернули результат")

	createdCount := 0
	for i, r := range got {
		require.NoError(t, r.err, "конкурентная постановка %d не вернула ошибку", i+1)
		assert.NotEmpty(t, r.id, "id непустой у вызова %d", i+1)
		if r.created {
			createdCount++
		}
	}
	assert.Equal(t, 1, createdCount, "ровно один вызов создал строку")
	assert.Equal(t, got[0].id, got[1].id, "оба вызова вернули один и тот же id")

	var rows int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic)).Scan(&rows))
	assert.Equal(t, 1, rows, "в очереди одна строка")
}
