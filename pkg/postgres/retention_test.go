// Приёмка уборки терминальных строк (retention.go, миграция 00003):
// DeleteTerminal удаляет пачкой только терминальные задания (done, dead,
// canceled), пролежавшие терминальными дольше ttl, — начиная с самых
// старых по updated_at — и не трогает ни активные строки, ни выключенную
// уборку (ttl <= 0, batch < 1). Тест репозиторный: строки кладутся в
// таблицу напрямую, поэтому мутации deleteTerminalSQL (снятый фильтр по
// состоянию или по updated_at, забытый ORDER BY updated_at, LIMIT не по
// batch) видны здесь, а не только в e2e.
package postgres

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/queue"
)

// TestDeleteTerminal — контракт уборки терминальных строк на
// подготовленных строках: что удаляется, что остаётся, чем ограничена
// пачка и что уборка идемпотентна.
func TestDeleteTerminal(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const (
		// ttl — срок хранения терминальных строк во всех кейсах, кроме
		// тех, где он и есть предмет проверки.
		ttl = time.Hour
		// bigBatch — заведомо большая пачка: в одиночных кейсах
		// ограничение пачки не участвует.
		bigBatch = 100
		// edge — отступ от границы TTL в кейсах «чуть старше/чуть
		// моложе»: он заведомо больше задержки между вставкой и уборкой.
		edge = 30 * time.Second
	)

	// reset чистит таблицу целиком: уборка не фильтрует по теме, поэтому
	// строки других тестов пакета исказили бы и результат, и проверку
	// «пустая таблица». `go test -count=N` повторяет тест в той же схеме,
	// так что чистка нужна и по этой причине.
	reset := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, `DELETE FROM job`)
		require.NoError(t, err)
	}

	// putJob кладёт задание и отдаёт его id. age — насколько updated_at
	// (момент перехода в терминальное состояние) в прошлом; created_at и
	// available_at уводятся в то же прошлое, чтобы единственным отличием
	// кейсов были состояние и возраст.
	putJob := func(t *testing.T, topic string, state queue.State, age time.Duration) string {
		t.Helper()

		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, available_at, created_at, updated_at)
			 VALUES (gen_random_uuid(), $1, '{}'::jsonb, $2,
			         now() - interval '1 day', now() - $3::interval, now() - $3::interval)
			 RETURNING id::text`,
			topic, string(state), age,
		).Scan(&id))
		return id
	}

	// countJobs считает строки таблицы.
	countJobs := func(t *testing.T) int {
		t.Helper()

		var n int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM job`).Scan(&n))
		return n
	}

	// jobExists сообщает, лежит ли строка в таблице.
	jobExists := func(t *testing.T, id string) bool {
		t.Helper()

		var exists bool
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM job WHERE id = $1)`, id).Scan(&exists))
		return exists
	}

	// sweep — один вызов уборки в собственной транзакции с фиксацией:
	// удаление видно снаружи только после Commit (уборка — запись).
	sweep := func(t *testing.T, ttl time.Duration, batch int) []queue.DeletedJob {
		t.Helper()

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция уборки")
		defer func() { _ = tx.Rollback(ctx) }()

		deleted, err := repo.DeleteTerminal(ctx, tx, ttl, batch)
		require.NoError(t, err, "уборка — не ошибка")
		require.NoError(t, tx.Commit(ctx))
		return deleted
	}

	// sortedIDs — идентификаторы удалённых заданий: порядок RETURNING не
	// гарантирован, поэтому сравниваются множества, а не
	// последовательности.
	sortedIDs := func(jobs []queue.DeletedJob) []string {
		ids := make([]string, 0, len(jobs))
		for _, job := range jobs {
			ids = append(ids, job.ID)
		}
		sort.Strings(ids)
		return ids
	}

	// Одиночные кейсы: одна строка в таблице, один вызов уборки.
	tests := []struct {
		name string
		// topic — тема задания кейса.
		topic string
		// state — состояние задания в таблице.
		state queue.State
		// age — насколько updated_at в прошлом.
		age time.Duration
		// ttl — срок хранения, с которым зовётся уборка.
		ttl time.Duration
		// wantDeleted — удаляется ли строка.
		wantDeleted bool
	}{
		{
			name:        "done старше TTL удаляется",
			topic:       "t-ret-done-old",
			state:       queue.StateDone,
			age:         2 * time.Hour,
			ttl:         ttl,
			wantDeleted: true,
		},
		{
			name:        "dead старше TTL удаляется",
			topic:       "t-ret-dead-old",
			state:       queue.StateDead,
			age:         2 * time.Hour,
			ttl:         ttl,
			wantDeleted: true,
		},
		{
			name:        "canceled старше TTL удаляется",
			topic:       "t-ret-canceled-old",
			state:       queue.StateCanceled,
			age:         2 * time.Hour,
			ttl:         ttl,
			wantDeleted: true,
		},
		{
			name:  "done моложе TTL остаётся",
			topic: "t-ret-done-fresh",
			state: queue.StateDone,
			age:   10 * time.Minute,
			ttl:   ttl,
		},
		{
			// Граница TTL: условие уборки — updated_at < now() - ttl,
			// поэтому «чуть моложе» остаётся, «чуть старше» удаляется.
			// Строгое равенство не проверяется: момент now() в базе
			// движется, и такой кейс был бы нестабильным.
			name:  "done чуть моложе границы TTL остаётся",
			topic: "t-ret-done-edge-young",
			state: queue.StateDone,
			age:   ttl - edge,
			ttl:   ttl,
		},
		{
			name:        "done чуть старше границы TTL удаляется",
			topic:       "t-ret-done-edge-old",
			state:       queue.StateDone,
			age:         ttl + edge,
			ttl:         ttl,
			wantDeleted: true,
		},
		{
			// Активные строки не терминальны: возраст для них не повод
			// для удаления, иначе уборка уносила бы живую работу.
			name:  "pending не удаляется даже при очень старом updated_at",
			topic: "t-ret-pending-ancient",
			state: queue.StatePending,
			age:   10_000 * time.Hour,
			ttl:   ttl,
		},
		{
			name:  "processing не удаляется даже при очень старом updated_at",
			topic: "t-ret-processing-ancient",
			state: queue.StateProcessing,
			age:   10_000 * time.Hour,
			ttl:   ttl,
		},
		{
			name:  "failed не удаляется даже при очень старом updated_at",
			topic: "t-ret-failed-ancient",
			state: queue.StateFailed,
			age:   10_000 * time.Hour,
			ttl:   ttl,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			id := putJob(t, tc.topic, tc.state, tc.age)

			deleted := sweep(t, tc.ttl, bigBatch)

			if !tc.wantDeleted {
				assert.Empty(t, deleted, "строка не подлежит уборке — удалять нечего")
				assert.True(t, jobExists(t, id), "строка осталась в таблице")
				assert.Equal(t, 1, countJobs(t), "уборка не тронула ни одной строки")
				return
			}

			require.Len(t, deleted, 1, "удалена ровно одна строка")
			assert.Equal(t, id, deleted[0].ID, "удалено ожидаемое задание")
			assert.Equal(t, queue.Topic(tc.topic), deleted[0].Topic,
				"тема удалённого задания — для журнала и метрики вызывающего")
			assert.False(t, jobExists(t, id), "строки в таблице больше нет")
			assert.Equal(t, 0, countJobs(t), "таблица пуста")
		})
	}

	t.Run("ttl <= 0 — уборка выключена, ничего не удалено", func(t *testing.T) {
		for _, ttl := range []time.Duration{0, -time.Hour} {
			reset(t)
			id := putJob(t, "t-ret-ttl-off", queue.StateDone, 10_000*time.Hour)

			assert.Empty(t, sweep(t, ttl, bigBatch),
				"ttl %s — защита от случайного DELETE всей терминальной части", ttl)
			assert.True(t, jobExists(t, id), "строка на месте при ttl %s", ttl)
			assert.Equal(t, 1, countJobs(t), "ничего не удалено при ttl %s", ttl)
		}
	})

	t.Run("batch < 1 — пустой результат, ничего не удалено", func(t *testing.T) {
		for _, batch := range []int{0, -1} {
			reset(t)
			id := putJob(t, "t-ret-batch-off", queue.StateDone, 2*time.Hour)

			assert.Empty(t, sweep(t, ttl, batch), "batch %d — «нечего убирать»", batch)
			assert.True(t, jobExists(t, id), "строка на месте при batch %d", batch)
			assert.Equal(t, 1, countJobs(t), "ничего не удалено при batch %d", batch)
		}
	})

	t.Run("пачка ограничена batch", func(t *testing.T) {
		reset(t)
		// Пять подходящих строк: терминальные и старше TTL.
		for i := 0; i < 5; i++ {
			putJob(t, "t-ret-batch", queue.StateDone, 2*time.Hour+time.Duration(i)*time.Minute)
		}

		deleted := sweep(t, ttl, 2)
		assert.Len(t, deleted, 2, "одна пачка — не больше batch строк")
		assert.Equal(t, 3, countJobs(t), "остальные строки ждут следующей пачки")
	})

	t.Run("порядок: сначала удаляется самое старое", func(t *testing.T) {
		reset(t)

		// Разные updated_at: две самые старые строки обязаны уехать в
		// первой пачке. «Свежее» задание получает меньший id — так видно,
		// что первичен updated_at, а не id.
		oldest := putJob(t, "t-ret-order-oldest", queue.StateDone, 5*time.Hour)
		middle := putJob(t, "t-ret-order-middle", queue.StateDead, 4*time.Hour)
		young := putJob(t, "t-ret-order-young", queue.StateCanceled, 3*time.Hour)
		newest := putJob(t, "t-ret-order-newest", queue.StateDone, 2*time.Hour)

		deleted := sweep(t, ttl, 2)
		require.Len(t, deleted, 2, "пачка из двух строк")

		want := []string{oldest, middle}
		sort.Strings(want)
		assert.Equal(t, want, sortedIDs(deleted),
			"в пачку попали две самые старые строки, а не любые две")

		assert.False(t, jobExists(t, oldest), "самое старое удалено")
		assert.False(t, jobExists(t, middle), "второе по возрасту удалено")
		assert.True(t, jobExists(t, young), "более свежая строка ждёт следующей пачки")
		assert.True(t, jobExists(t, newest), "самая свежая строка ждёт следующей пачки")
		assert.Equal(t, 2, countJobs(t))
	})

	t.Run("в смешанной таблице убираются только терминальные старше TTL", func(t *testing.T) {
		reset(t)

		doneOld := putJob(t, "t-ret-mixed-done", queue.StateDone, 3*time.Hour)
		deadOld := putJob(t, "t-ret-mixed-dead", queue.StateDead, 3*time.Hour)
		canceledOld := putJob(t, "t-ret-mixed-canceled", queue.StateCanceled, 3*time.Hour)
		pendingOld := putJob(t, "t-ret-mixed-pending", queue.StatePending, 3*time.Hour)
		failedOld := putJob(t, "t-ret-mixed-failed", queue.StateFailed, 3*time.Hour)
		doneFresh := putJob(t, "t-ret-mixed-done-fresh", queue.StateDone, time.Minute)

		deleted := sweep(t, ttl, bigBatch)

		want := []string{doneOld, deadOld, canceledOld}
		sort.Strings(want)
		assert.Equal(t, want, sortedIDs(deleted),
			"удалены все три терминальных состояния старше TTL — и только они")
		for _, id := range []string{pendingOld, failedOld, doneFresh} {
			assert.True(t, jobExists(t, id), "строка %s осталась в таблице", id)
		}
		assert.Equal(t, 3, countJobs(t))
	})

	t.Run("повторный вызов с теми же параметрами — пустой результат", func(t *testing.T) {
		reset(t)
		first := putJob(t, "t-ret-repeat-a", queue.StateDone, 3*time.Hour)
		second := putJob(t, "t-ret-repeat-b", queue.StateDead, 2*time.Hour)

		deleted := sweep(t, ttl, bigBatch)
		want := []string{first, second}
		sort.Strings(want)
		require.Equal(t, want, sortedIDs(deleted), "первый вызов убирает обе строки")

		assert.Empty(t, sweep(t, ttl, bigBatch),
			"повторный вызов с теми же параметрами убирать нечего")
		assert.Equal(t, 0, countJobs(t), "таблица осталась пустой")
	})

	t.Run("пустая таблица — пустой результат без ошибки", func(t *testing.T) {
		reset(t)

		// sweep внутри требует отсутствия ошибки: пустая таблица — не сбой.
		assert.Empty(t, sweep(t, ttl, bigBatch), "убирать нечего")
		assert.Equal(t, 0, countJobs(t))
	})
}
