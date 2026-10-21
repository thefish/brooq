// Приёмка выборки статистики для метрик (задача 3.5, D11): число заданий
// по паре «тема + состояние» (CountByState) и возраст старейшего готового
// задания по теме (OldestReadyAge). Оба чтения идут через пул,
// поэтому тест репозиторный: строки кладутся в таблицу напрямую, чтение
// зовётся через Repository. Правка countByStateSQL/oldestReadyAgeSQL,
// ломающая договор (потерянная группировка, состояние вне перечня готовых,
// снятый фильтр available_at <= now(), забытая сортировка), видна здесь.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/queue"
)

// TestCountByState — CountByState считает строки по каждой паре «тема +
// состояние»: пары разных тем и состояний не смешиваются, каждая пара
// приходит один раз со своим числом, вывод отсортирован (сборщик метрик
// получает детерминированный ряд), пустая таблица — пустой результат без
// ошибки.
func TestCountByState(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	// insertJob кладёт задание заданной темы и состояния и отдаёт его id.
	// Строка в processing получает аренду: без неё она нарушала бы
	// инвариант job-lifecycle «задание в обработке всегда имеет аренду».
	insertJob := func(t *testing.T, topic string, state queue.State) string {
		t.Helper()

		var (
			lockedBy *string
			until    *time.Time
		)
		if state == queue.StateProcessing {
			owner := "w-stats"
			lockedBy = &owner
			u := time.Now().Add(time.Hour)
			until = &u
		}

		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, locked_by, locked_until)
			 VALUES (gen_random_uuid(), $1, '{}'::jsonb, $2, $3, $4)
			 RETURNING id::text`,
			topic, string(state), lockedBy, until,
		).Scan(&id))
		return id
	}

	// resetTable очищает таблицу: CountByState читает её целиком, поэтому
	// ожидаемые числа можно сравнивать только на известном содержимом.
	// Чистка нужна и потому, что `go test -count=N` повторяет тест в той же
	// схеме.
	resetTable := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, `DELETE FROM job`)
		require.NoError(t, err)
	}

	// preparedJob — подготовленная строка кейса: тема, состояние и число
	// копий.
	type preparedJob struct {
		topic string
		state queue.State
		count int
	}

	tests := []struct {
		name string
		// jobs — подготовленные строки таблицы.
		jobs []preparedJob
		// want — ожидаемый результат целиком, в порядке countByStateSQL
		// (тема, затем состояние).
		want []queue.TopicStateCount
	}{
		{
			name: "пары разных тем и состояний не смешиваются",
			jobs: []preparedJob{
				{"t-count-a", queue.StatePending, 3},
				{"t-count-a", queue.StateFailed, 2},
				{"t-count-a", queue.StateDone, 1},
				{"t-count-b", queue.StatePending, 1},
				{"t-count-b", queue.StateProcessing, 2},
				{"t-count-c", queue.StateDead, 4},
			},
			want: []queue.TopicStateCount{
				{Topic: "t-count-a", State: queue.StateDone, Count: 1},
				{Topic: "t-count-a", State: queue.StateFailed, Count: 2},
				{Topic: "t-count-a", State: queue.StatePending, Count: 3},
				{Topic: "t-count-b", State: queue.StatePending, Count: 1},
				{Topic: "t-count-b", State: queue.StateProcessing, Count: 2},
				{Topic: "t-count-c", State: queue.StateDead, Count: 4},
			},
		},
		{
			// canceled — состояние из миграции 00002: отменённые задания
			// обязаны попадать в метрику наравне с остальными, иначе
			// сборщик показывал бы очередь меньше, чем она есть.
			name: "отменённое задание считается наравне с остальными",
			jobs: []preparedJob{
				{"t-count-canceled", queue.StateCanceled, 2},
				{"t-count-canceled", queue.StatePending, 1},
			},
			want: []queue.TopicStateCount{
				{Topic: "t-count-canceled", State: queue.StateCanceled, Count: 2},
				{Topic: "t-count-canceled", State: queue.StatePending, Count: 1},
			},
		},
		{
			// Отсутствие данных — не ошибка: сборщик метрик не должен
			// падать на пустой очереди. Пустой срез, а не nil: nil в
			// assert.Equal ниже означал бы «результат не собран».
			name: "пустая таблица — пустой результат без ошибки",
			jobs: nil,
			want: []queue.TopicStateCount{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetTable(t)

			for _, job := range tc.jobs {
				for i := 0; i < job.count; i++ {
					insertJob(t, job.topic, job.state)
				}
			}

			got, err := repo.CountByState(ctx)
			require.NoError(t, err, "выборка числа заданий по состояниям — не ошибка")
			require.NotNil(t, got, "результат — срез, а не nil")
			assert.Equal(t, tc.want, got, "пары «тема + состояние» и их числа")
		})
	}
}

// TestOldestReadyAge — OldestReadyAge отдаёт по теме возраст старейшего
// готового задания: готовые — это pending и failed с наступившим
// available_at (ровно те строки, что возьмёт Claim, D4). Задание с
// available_at в будущем не учитывается и не сдвигает возраст, тема без
// готовых заданий в результат не попадает, пустая таблица — пустой
// результат без ошибки.
func TestOldestReadyAge(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const (
		// clockWindow — допуск на разницу между моментом подготовки
		// строки и моментом чтения: возраст считается от now() базы.
		clockWindow = 5 * time.Second
		// oldest — возраст самого давнего готового задания темы
		// t-age-ready.
		oldest = 2 * time.Hour
		// failedAge — возраст готового задания в failed.
		failedAge = 30 * time.Minute
		// mixedAge — возраст готового задания темы, где есть и будущее
		// задание.
		mixedAge = 10 * time.Minute
	)

	// insertJob кладёт задание и отдаёт его id; availableIn — сдвиг
	// available_at от «сейчас» (отрицательный — в прошлом). Строка в
	// processing получает аренду, чтобы не нарушать инвариант «задание в
	// обработке всегда имеет аренду».
	insertJob := func(t *testing.T, topic string, state queue.State, availableIn time.Duration) string {
		t.Helper()

		var (
			lockedBy *string
			until    *time.Time
		)
		if state == queue.StateProcessing {
			owner := "w-age"
			lockedBy = &owner
			u := time.Now().Add(time.Hour)
			until = &u
		}

		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, available_at, locked_by, locked_until)
			 VALUES (gen_random_uuid(), $1, '{}'::jsonb, $2, $3, $4, $5)
			 RETURNING id::text`,
			topic, string(state), time.Now().Add(availableIn), lockedBy, until,
		).Scan(&id))
		return id
	}

	// resetTable очищает таблицу: OldestReadyAge читает её целиком, поэтому
	// набор тем результата известен только на известном содержимом. Чистка
	// нужна и потому, что `go test -count=N` повторяет тест в той же схеме.
	resetTable := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, `DELETE FROM job`)
		require.NoError(t, err)
	}

	// agesByTopic переводит результат в карту «тема -> возраст»: проверки
	// ниже не зависят от порядка строк в oldestReadyAgeSQL.
	agesByTopic := func(ages []queue.TopicAge) map[queue.Topic]time.Duration {
		byTopic := make(map[queue.Topic]time.Duration, len(ages))
		for _, age := range ages {
			byTopic[age.Topic] = age.Age
		}
		return byTopic
	}

	t.Run("подготовленные данные", func(t *testing.T) {
		resetTable(t)

		// Готовые задания: возраст темы задаёт самое давнее из них.
		insertJob(t, "t-age-ready", queue.StatePending, -oldest)
		insertJob(t, "t-age-ready", queue.StatePending, -5*time.Minute)

		// failed — готовое состояние: задание с назначенным повтором
		// берётся взятием, значит оно тоже ждёт.
		insertJob(t, "t-age-failed", queue.StateFailed, -failedAge)

		// Смешанная тема: будущее задание не готово и не сдвигает возраст
		// (иначе min(available_at) дал бы отрицательное значение).
		insertJob(t, "t-age-mixed", queue.StatePending, -mixedAge)
		insertJob(t, "t-age-mixed", queue.StatePending, time.Hour)

		// Темы без готовых заданий: всё в будущем, терминальные состояния
		// и задание в работе.
		insertJob(t, "t-age-future", queue.StatePending, time.Hour)
		insertJob(t, "t-age-terminal", queue.StateDone, -3*time.Hour)
		insertJob(t, "t-age-terminal", queue.StateDead, -3*time.Hour)
		insertJob(t, "t-age-terminal", queue.StateCanceled, -3*time.Hour)
		insertJob(t, "t-age-processing", queue.StateProcessing, -5*time.Hour)

		ages, err := repo.OldestReadyAge(ctx)
		require.NoError(t, err, "выборка возраста старейшего готового задания — не ошибка")

		tests := []struct {
			name string
			// topic — тема кейса.
			topic queue.Topic
			// wantPresent — попадает ли тема в результат.
			wantPresent bool
			// wantAge — ожидаемый возраст старейшего готового задания
			// (проверяется только при wantPresent).
			wantAge time.Duration
		}{
			{
				name:        "возраст задаёт самое давнее готовое задание темы",
				topic:       "t-age-ready",
				wantPresent: true,
				wantAge:     oldest,
			},
			{
				name:        "failed — готовое состояние, тема учитывается",
				topic:       "t-age-failed",
				wantPresent: true,
				wantAge:     failedAge,
			},
			{
				name:        "будущее задание не сдвигает возраст темы",
				topic:       "t-age-mixed",
				wantPresent: true,
				wantAge:     mixedAge,
			},
			{
				name:  "тема без готовых заданий: всё в будущем",
				topic: "t-age-future",
			},
			{
				name:  "тема без готовых заданий: терминальные состояния",
				topic: "t-age-terminal",
			},
			{
				name:  "тема без готовых заданий: задание в обработке",
				topic: "t-age-processing",
			},
		}

		byTopic := agesByTopic(ages)
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				age, ok := byTopic[tc.topic]
				if !tc.wantPresent {
					assert.False(t, ok, "тема %s не попадает в результат, получено %s", tc.topic, age)
					return
				}

				require.True(t, ok, "тема %s есть в результате", tc.topic)
				assert.Greater(t, age, time.Duration(0), "возраст готового задания положителен")
				assert.InDelta(t, float64(tc.wantAge), float64(age), float64(clockWindow),
					"возраст старейшего готового задания темы %s", tc.topic)
			})
		}

		// Ровно три темы с готовыми заданиями: лишняя тема означала бы, что
		// фильтр готовности пропускает неготовое.
		assert.Len(t, ages, 3, "в результате только темы с готовыми заданиями")
	})

	t.Run("пустая таблица — пустой результат без ошибки", func(t *testing.T) {
		resetTable(t)

		ages, err := repo.OldestReadyAge(ctx)
		require.NoError(t, err, "пустая таблица — не ошибка")
		require.NotNil(t, ages, "результат — срез, а не nil")
		assert.Empty(t, ages, "готовых заданий нет — результат пуст")
	})
}
