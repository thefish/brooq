// Тесты публичного фасада движка (D9): Enqueue, EnqueueTx с Begin и
// Cancel работают с настоящим хранилищем pkg/postgres на изолированной
// схеме. Воркеры здесь не запускаются: проверяется фасад над хранилищем,
// а исполнение задания проверяют e2e-тесты пакета.
package brooq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

// facadeWorker — идентификатор воркера фасада: попадает в locked_by
// только у тех строк, которые тест готовит сам.
const facadeWorker = "w-facade"

// newFacadeQueue собирает движок поверх настоящего хранилища, не запуская
// воркеров: методы фасада работают с хранилищем напрямую.
func newFacadeQueue(repo *postgres.Repository) *Queue {
	return New(repo, discardLogger(), Config{
		Workers:      1,
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		WorkerID:     facadeWorker,
	})
}

// TestQueueEnqueue — постановка через фасад: задание оказывается в
// таблице в pending, тело не искажается, попытка не тратится, аренды нет.
func TestQueueEnqueue(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	q := newFacadeQueue(postgres.New(pool))

	t.Run("задание из Enqueue лежит в таблице", func(t *testing.T) {
		topic := e2eTopic(t, "facade-enqueue")
		payload := queue.Payload(`{"order":"7"}`)

		_, _, err := q.Enqueue(ctx, queue.Job{Topic: topic, Payload: payload})
		require.NoError(t, err)

		var (
			state      string
			gotTopic   string
			gotPayload []byte
			attempts   int
			lockedBy   *string
		)
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, topic, payload, attempts, locked_by
			   FROM job WHERE topic = $1`, string(topic),
		).Scan(&state, &gotTopic, &gotPayload, &attempts, &lockedBy))

		assert.Equal(t, string(queue.StatePending), state, "новое задание ждёт взятия")
		assert.Equal(t, string(topic), gotTopic, "тема задания сохранена")
		assert.JSONEq(t, string(payload), string(gotPayload), "тело задания не искажено")
		assert.Equal(t, 0, attempts, "постановка не тратит попытку")
		assert.Nil(t, lockedBy, "постановка не закрепляет аренду")
	})

	t.Run("повтор с тем же ключом не создаёт второе задание", func(t *testing.T) {
		topic := e2eTopic(t, "facade-enqueue-key")
		job := queue.Job{Topic: topic, Payload: queue.Payload(`{"n":1}`), IdempotencyKey: "k-facade"}

		_, _, err := q.Enqueue(ctx, job)
		require.NoError(t, err)
		_, _, err = q.Enqueue(ctx, job)
		require.NoError(t, err, "повтор постановки — не ошибка")

		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic)).Scan(&n))
		assert.Equal(t, 1, n, "идемпотентность работает и через фасад")
	})
}

// TestQueueEnqueueTx — постановка в транзакции вызывающего: фиксация
// оставляет задание в очереди, откат не оставляет ничего.
func TestQueueEnqueueTx(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	q := newFacadeQueue(postgres.New(pool))

	tests := []struct {
		name string
		// commit — фиксировать ли транзакцию производителя.
		commit bool
		// wantRows — сколько заданий темы обязано остаться после неё.
		wantRows int
	}{
		{
			name:     "фиксация — задание в таблице",
			commit:   true,
			wantRows: 1,
		},
		{
			name:     "откат — задания нет",
			commit:   false,
			wantRows: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topic := e2eTopic(t, "facade-tx")

			tx, err := q.Begin(ctx)
			require.NoError(t, err, "транзакция производителя")
			_, _, err = q.EnqueueTx(ctx, tx, queue.Job{
				Topic:   topic,
				Payload: queue.Payload(`{"n":1}`),
			})
			require.NoError(t, err, "постановка в транзакции производителя")

			// Внутри транзакции задание видно: иначе «после отката пусто»
			// доказывало бы и то, что EnqueueTx не пишет ничего вовсе.
			var inTx int
			require.NoError(t, tx.QueryRow(ctx,
				`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic)).Scan(&inTx))
			require.Equal(t, 1, inTx, "внутри транзакции задание записано")

			if tc.commit {
				require.NoError(t, tx.Commit(ctx), "фиксация транзакции производителя")
			} else {
				require.NoError(t, tx.Rollback(ctx), "откат транзакции производителя")
			}

			var n int
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic)).Scan(&n))
			assert.Equal(t, tc.wantRows, n, "число заданий темы после транзакции")
		})
	}
}

// TestQueueCancel — отмена через фасад (D9, D14): незавершённое задание
// (в том числе в полёте) становится canceled, терминальное — даёт ошибку
// и не меняется.
func TestQueueCancel(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	q := newFacadeQueue(postgres.New(pool))

	// putJob кладёт задание в заданном состоянии и отдаёт его id.
	// lockedIn != 0 — аренда: взятое задание всегда с арендой (инвариант 1).
	putJob := func(t *testing.T, topic string, state queue.State, lockedIn time.Duration) string {
		t.Helper()

		var (
			lockedBy *string
			until    *time.Time
		)
		if lockedIn != 0 {
			owner := facadeWorker
			lockedBy = &owner
			u := time.Now().Add(lockedIn)
			until = &u
		}

		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, available_at, locked_by, locked_until)
			 VALUES (gen_random_uuid(), $1, '{}'::jsonb, $2, now(), $3, $4)
			 RETURNING id::text`,
			topic, string(state), lockedBy, until,
		).Scan(&id))
		return id
	}

	// readState читает состояние и updated_at: отказ отмены обязан
	// оставить строку нетронутой.
	readState := func(t *testing.T, id string) (string, time.Time) {
		t.Helper()

		var (
			state     string
			updatedAt time.Time
		)
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, updated_at FROM job WHERE id = $1`, id).Scan(&state, &updatedAt))
		return state, updatedAt
	}

	tests := []struct {
		name string
		// state — состояние задания до отмены.
		state queue.State
		// lockedIn — сдвиг locked_until от «сейчас» (0 — аренды нет).
		lockedIn time.Duration
		// wantState — состояние после отмены.
		wantState queue.State
		// wantErr — отмена обязана вернуть ошибку.
		wantErr bool
		// wantErrText — подстрока текста ошибки.
		wantErrText string
	}{
		{
			name:      "отмена pending — canceled без ошибки",
			state:     queue.StatePending,
			wantState: queue.StateCanceled,
		},
		{
			name:      "отмена processing — canceled",
			state:     queue.StateProcessing,
			lockedIn:  time.Hour,
			wantState: queue.StateCanceled,
		},
		{
			name:      "отмена failed — canceled",
			state:     queue.StateFailed,
			wantState: queue.StateCanceled,
		},
		{
			name:        "отмена done — ошибка, строка не изменилась",
			state:       queue.StateDone,
			wantState:   queue.StateDone,
			wantErr:     true,
			wantErrText: "терминальное",
		},
		{
			name:        "отмена dead — ошибка",
			state:       queue.StateDead,
			wantState:   queue.StateDead,
			wantErr:     true,
			wantErrText: "терминальное",
		},
		{
			name:        "отмена canceled — ошибка",
			state:       queue.StateCanceled,
			wantState:   queue.StateCanceled,
			wantErr:     true,
			wantErrText: "терминальное",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := putJob(t, "t-facade-cancel", tc.state, tc.lockedIn)
			beforeState, beforeUpdated := readState(t, id)
			require.Equal(t, string(tc.state), beforeState, "подготовленное состояние")

			err := q.Cancel(ctx, id)

			afterState, afterUpdated := readState(t, id)
			assert.Equal(t, string(tc.wantState), afterState, "состояние после отмены")

			if !tc.wantErr {
				require.NoError(t, err, "отмена незавершённого задания — не ошибка")
				assert.False(t, beforeUpdated.Equal(afterUpdated), "строка обновлена отменой")
				return
			}

			require.Error(t, err, "отмена терминального задания — ошибка, а не тихий успех")
			assert.Contains(t, err.Error(), tc.wantErrText, "текст ошибки объясняет причину")
			assert.Contains(t, err.Error(), id, "ошибка называет задание")
			assert.True(t, beforeUpdated.Equal(afterUpdated),
				"строка не тронута: updated_at %s против %s", beforeUpdated, afterUpdated)
		})
	}

	t.Run("неизвестный id — ошибка", func(t *testing.T) {
		const missing = "00000000-0000-0000-0000-0000000000ff"

		err := q.Cancel(ctx, missing)
		require.Error(t, err, "отмена несуществующего задания — ошибка")
		assert.Contains(t, err.Error(), "терминальное",
			"обновлять нечего — ошибка, а не тихий успех")
	})
}
