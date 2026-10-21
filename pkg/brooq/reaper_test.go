// Приёмка сборщика истёкших аренд на уровне движка (задача 4.5, D8):
// pkg/postgres проверяет SQL возврата, здесь — что движок действительно
// запускает сборщик, что возврат доходит до исполнения и что возврат
// безопасен при параллельных исполнителях. Мутация «reapOnce возвращает
// nil, nil» (мёртвый сборщик) обязана ронять первый тест этого файла —
// иначе сборщик не покрыт ничем.
//
// База — та же, что у e2e_test.go: TestMain пакета поднимает
// изолированную схему и накатывает goose. Без TEST_DATABASE_URL тесты
// пропускаются через newTestPool.
package brooq

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

const (
	// reaperTestPeriod — период сборщика в тестах: задан явно и коротко.
	// Без явного значения движок посчитал бы max(5s, Lease/4) по D8 и
	// тест шёл бы секундами.
	reaperTestPeriod = 50 * time.Millisecond
	// reaperClaimLease — аренда, которую тест выдаёт сам: короткая, чтобы
	// следующее истечение наступило в пределах теста, а не через 30 с.
	reaperClaimLease = 100 * time.Millisecond
)

// newReaperEngine собирает движок с явным периодом сборщика: настройки
// темы те же, что у newEngine из e2e_test.go.
func newReaperEngine(t *testing.T, repo *postgres.Repository, workerID string,
	topic queue.Topic, h queue.Handler) *Queue {
	t.Helper()

	q := New(repo, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Workers:      1,
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		ReaperPeriod: reaperTestPeriod,
		WorkerID:     workerID,
	})
	require.NoError(t, q.RegisterKind(topic, h, queue.KindOptions{
		MaxAttempts:         3,
		AttemptTimeout:      e2eTimeout,
		MaxExecutionTimeout: e2eTimeout,
		BackoffBase:         100 * time.Millisecond,
		BackoffMax:          time.Second,
	}), "регистрация темы %s", topic)
	return q
}

// newReaperOnlyEngine собирает движок без зарегистрированных тем: Start
// поднимает только горутину сборщика, поэтому взятие заданий тест делает
// сам и ничто не перебивает его собственный сценарий.
func newReaperOnlyEngine(repo *postgres.Repository) *Queue {
	return New(repo, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{
		Workers:      1,
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		ReaperPeriod: reaperTestPeriod,
		WorkerID:     "reaper-only",
	})
}

// insertLeaseRow кладёт строку задания с готовой арендой: leaseLeft < 0 —
// аренда истекла, > 0 — живая. available_at в прошлом, чтобы единственным
// отличием кейсов была аренда.
func insertLeaseRow(t *testing.T, pool *pgxpool.Pool, topic queue.Topic,
	state queue.State, attempts int, lockedBy string, leaseLeft time.Duration) {
	t.Helper()

	_, err := pool.Exec(context.Background(), `
		INSERT INTO job (id, topic, payload, state, attempts,
		                 available_at, locked_by, locked_until)
		VALUES (gen_random_uuid(), $1, '{}'::jsonb, $2, $3,
		        now() - interval '1 hour', $4,
		        now() + make_interval(secs => $5))`,
		string(topic), string(state), attempts, lockedBy, leaseLeft.Seconds())
	require.NoError(t, err, "подготовка строки задания темы %s", topic)
}

// jobRow — снимок строки задания для проверок сборщика.
type jobRow struct {
	state    string
	attempts int
	lockedBy *string
}

// readJobRow читает состояние, счётчик попыток и владельца аренды.
func readJobRow(t *testing.T, pool *pgxpool.Pool, topic queue.Topic) jobRow {
	t.Helper()

	var row jobRow
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT state, attempts, locked_by FROM job WHERE topic = $1`, string(topic),
	).Scan(&row.state, &row.attempts, &row.lockedBy))
	return row
}

// claimOne изображает воркера, который взял задание и умер: взятие идёт
// через настоящее хранилище, аренда короткая, исход не пишется.
func claimOne(t *testing.T, repo *postgres.Repository, topic queue.Topic, worker string) {
	t.Helper()

	ctx := context.Background()
	tx, err := repo.Begin(ctx)
	require.NoError(t, err, "транзакция взятия")
	defer func() { _ = tx.Rollback(ctx) }()

	claims, err := repo.Claim(ctx, tx, topic, worker, reaperClaimLease, 1)
	require.NoError(t, err, "взятие задания темы %s", topic)
	require.Len(t, claims, 1, "задание темы %s взято", topic)
	require.NoError(t, tx.Commit(ctx))
}

// TestReaperReturnsExpiredLeaseWhileRunning — главный тест задачи 4.5:
// при работающем движке задание с истёкшей арендой возвращается в оборот и
// исполняется. Признак того, что вернул именно сборщик, — рост счётчика
// попыток: строку никто не трогал, значит взятие состоялось после возврата.
//
// Мутация «reapOnce возвращает nil, nil» обязана ронять этот тест: без
// сборщика строка навсегда остаётся в processing.
func TestReaperReturnsExpiredLeaseWhileRunning(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.New(pool)
	topic := e2eTopic(t, "reaper-expired")

	// Строка умершего воркера: взято, попыток 2, аренда истекла 5 минут назад.
	const deadWorker = "мёртвый-воркер"
	insertLeaseRow(t, pool, topic, queue.StateProcessing, 2, deadWorker, -5*time.Minute)

	handler := &countingHandler{}
	engine := newReaperEngine(t, repo, "reaper-expired-1", topic, handler)
	defer func() { _ = engine.Close() }()
	engine.Start()

	waitJobState(t, pool, topic, queue.StateDone)
	engine.Stop()

	row := readJobRow(t, pool, topic)
	assert.Equal(t, string(queue.StateDone), row.state,
		"сборщик вернул задание в оборот, воркер взял и записал исход")
	assert.Equal(t, 3, row.attempts,
		"попытка списана повторным взятием: 2 → 3, то есть вернул именно сборщик")
	assert.Equal(t, 1, handler.count(), "задание исполнено ровно один раз")
	require.NotNil(t, row.lockedBy, "задание в done помнит, кто его взял")
	assert.Equal(t, "reaper-expired-1", *row.lockedBy,
		"взял воркер движка, а не мёртвый воркер")
}

// TestReaperDoesNotTouchLiveLease — сценарий «Живая аренда не освобождает
// задание»: пока срок аренды не прошёл, сборщик её не трогает, даже если
// воркер молчит.
func TestReaperDoesNotTouchLiveLease(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.New(pool)
	topic := e2eTopic(t, "reaper-live")

	// Живой воркер молчит, но аренда ещё не истекла.
	const liveWorker = "живой-воркер"
	insertLeaseRow(t, pool, topic, queue.StateProcessing, 1, liveWorker, time.Hour)

	handler := &countingHandler{}
	engine := newReaperEngine(t, repo, "reaper-live-1", topic, handler)
	defer func() { _ = engine.Close() }()
	engine.Start()

	// Сборщик не фильтрует по теме (D8), поэтому возврат контрольной
	// строки в pending — доказательство состоявшегося тика. Так тест ждёт
	// тики сборщика, а не спит «на всякий случай».
	for tick := 1; tick <= 3; tick++ {
		control := e2eTopic(t, "reaper-live-control")
		insertLeaseRow(t, pool, control, queue.StateProcessing, 1, liveWorker, -time.Minute)
		waitJobState(t, pool, control, queue.StatePending)
	}

	row := readJobRow(t, pool, topic)
	assert.Equal(t, string(queue.StateProcessing), row.state,
		"живая аренда держит задание в processing после нескольких тиков сборщика")
	require.NotNil(t, row.lockedBy, "владелец аренды сохранён")
	assert.Equal(t, liveWorker, *row.lockedBy, "владелец аренды не сменился")
	assert.Equal(t, 1, row.attempts, "попытки не тронуты")
	assert.Equal(t, 0, handler.count(), "обработчик не вызывался: задание не возвращалось")
}

// TestReaperIdempotentUnderTwoEngines — сценарий «Два процесса возвращают
// одновременно»: два независимых движка со своими сборщиками на одну БД.
// Задание возвращается ровно один раз, поэтому исполняется ровно один раз,
// а счётчик попыток растёт ровно на единицу.
func TestReaperIdempotentUnderTwoEngines(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.New(pool)
	topic := e2eTopic(t, "reaper-two")

	insertLeaseRow(t, pool, topic, queue.StateProcessing, 2, "мёртвый-воркер", -5*time.Minute)

	first, second := &countingHandler{}, &countingHandler{}
	q1 := newReaperEngine(t, repo, "reaper-two-1", topic, first)
	q2 := newReaperEngine(t, repo, "reaper-two-2", topic, second)
	// Остановка идемпотентна: Stop уже вызван, Close лишь добивает горутины.
	defer func() {
		_ = q1.Close()
		_ = q2.Close()
	}()

	// Оба движка стартуют на уже истёкшую аренду: возврат идёт конкурентно.
	q1.Start()
	q2.Start()

	waitJobState(t, pool, topic, queue.StateDone)
	q1.Stop()
	q2.Stop()

	total := first.count() + second.count()
	assert.Equal(t, 1, total,
		"два сборщика вернули задание ровно один раз: исполнение ровно одно (вызовы: первый=%d, второй=%d)",
		first.count(), second.count())

	row := readJobRow(t, pool, topic)
	assert.Equal(t, string(queue.StateDone), row.state)
	assert.Equal(t, 3, row.attempts,
		"попытка списана ровно один раз: 2 → 3, второго взятия не было")
}

// TestReaperRepeatedExpiryDoesNotKillJob — сценарий «Многократное истечение
// аренды не убивает задание» (claim/spec.md): возврат по аренде не
// расходует попытку и не приближает задание к dead, сколько бы раз аренда
// ни истекала.
func TestReaperRepeatedExpiryDoesNotKillJob(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.New(pool)

	tests := []struct {
		name string
		// expiries — сколько раз аренда истекает, прежде чем тест смотрит
		// на итог.
		expiries int
	}{
		{name: "одно истечение аренды", expiries: 1},
		{name: "два истечения подряд", expiries: 2},
		{name: "три истечения подряд", expiries: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			topic := e2eTopic(t, "reaper-repeat")
			insertLeaseRow(t, pool, topic, queue.StateProcessing, 1, "мёртвый-воркер", -time.Minute)

			engine := newReaperOnlyEngine(repo)
			defer func() { _ = engine.Close() }()
			engine.Start()

			for cycle := 1; cycle <= tc.expiries; cycle++ {
				// Сборщик вернул задание в оборот: попытки те же, что были.
				waitJobState(t, pool, topic, queue.StatePending)
				returned := readJobRow(t, pool, topic)
				assert.Equal(t, cycle, returned.attempts,
					"возврат по аренде не меняет счётчик попыток (цикл %d)", cycle)
				assert.Nil(t, returned.lockedBy, "аренда снята при возврате (цикл %d)", cycle)

				// Воркер взял задание и умер: аренда снова истечёт.
				claimOne(t, repo, topic, fmt.Sprintf("мёртвый-воркер-%d", cycle))
			}

			// Последнее истечение: задание обязано остаться в очереди.
			waitJobState(t, pool, topic, queue.StatePending)
			row := readJobRow(t, pool, topic)
			assert.Equal(t, string(queue.StatePending), row.state,
				"задание осталось в очереди и доступно для взятия")
			assert.NotEqual(t, string(queue.StateDead), row.state,
				"истечение аренды не приближает задание к dead (инвариант 7)")
			assert.Equal(t, 1+tc.expiries, row.attempts,
				"счётчик попыток накопительный и не сбрасывается в 0: 1 + %d истечений", tc.expiries)
		})
	}
}
