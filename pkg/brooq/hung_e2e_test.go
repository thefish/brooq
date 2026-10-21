// Приёмка задачи 4.7 на настоящей PostgreSQL (D15): обработчик,
// игнорирующий контекст попытки, — зависшая горутина, которую в Go нельзя
// убить снаружи. На реальном исполнении задания проверяются три следствия
// предохранителей D15:
//
//  1. watchdog через грацию после отмены контекста фиксирует зависание:
//     brooq_hung_workers_total{topic} растёт;
//  2. аренда перестаёт продлеваться: heartbeat живёт на контексте попытки,
//     поэтому после отмены locked_until замирает и больше не сдвигается
//     вперёд;
//  3. истёкшую аренду возвращает Reaper: задание снова в pending, его берёт
//     второй движок, счётчик попыток при возврате не уменьшился.
//
// Метрика берётся из фейкового порта (fakeMetrics, см. heartbeat_test.go),
// состояние — из БД: метрика доказывает, что сработал движок, строка — что
// задание действительно вернулось в оборот.
//
// Без TEST_DATABASE_URL тест пропускается (newTestPool → Skip).
package brooq

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

// hungHandler — обработчик, который контекст попытки ИГНОРИРУЕТ: он спит
// work и не смотрит на ctx. Так выглядит зависшая горутина (D15 п.3):
// отмена контекста до неё доходит, но она её не замечает.
type hungHandler struct {
	// work — сколько спит обработчик; заведомо дольше Lease.
	work time.Duration
	// started отдаёт взятое задание: по нему тест узнаёт, что попытка
	// началась, и не спит «на всякий случай».
	started chan queue.Claim
	// done закрывается, когда Handle вернулся.
	done chan struct{}

	once sync.Once
}

// newHungHandler собирает обработчик, игнорирующий отмену.
func newHungHandler(work time.Duration) *hungHandler {
	return &hungHandler{
		work:    work,
		started: make(chan queue.Claim, 1),
		done:    make(chan struct{}),
	}
}

// Handle спит work, не глядя на ctx: ровно так зависший обработчик
// переживает отмену контекста попытки.
func (h *hungHandler) Handle(_ context.Context, c queue.Claim) error {
	select {
	case h.started <- c:
	default:
	}

	time.Sleep(h.work)
	h.once.Do(func() { close(h.done) })
	return nil
}

// TestWatchdogHungWorkerStopsRenewalAndReaped — сценарий задачи 4.7
// целиком: обработчик игнорирует ctx → растёт brooq_hung_workers_total,
// аренда перестаёт продлеваться, Reaper возвращает задание в оборот после
// Lease.
//
// Зависание опасно именно из-за heartbeat (D13, D15): живой обработчик
// продлевает аренду, поэтому без предохранителей зависшая горутина держала
// бы задание вечно. Контекст попытки гасит жёсткий потолок темы
// (MaxExecutionTimeout, D15 п.2) — AttemptTimeout гасит только контекст
// вызова обработчика, а он отмену игнорирует. После потолка тикер
// продления гаснет, аренда истекает, и Reaper возвращает задание.
func TestWatchdogHungWorkerStopsRenewalAndReaped(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "hung-reaper")

	const (
		// lease — срок аренды: продление идёт каждые Lease/3 = 100ms, а
		// после отмены аренда истекает за Lease.
		lease = 300 * time.Millisecond
		// attemptTimeout — когда отменяется контекст вызова обработчика.
		attemptTimeout = 50 * time.Millisecond
		// ceiling — жёсткий потолок исполнения темы (D15 п.2): он гасит
		// контекст попытки, даже если обработчик жив, и тем самым
		// останавливает продление аренды. Он заведомо больше периода
		// продления (Lease/3), чтобы heartbeat успел сдвинуть аренду хотя
		// бы раз: иначе «перестала продлеваться» нечего доказывать.
		ceiling = 400 * time.Millisecond
		// grace — грация watchdog после отмены: не вышел — зависание.
		grace = 50 * time.Millisecond
		// reaperPeriod — период сборщика истёкших аренд: возврат
		// происходит заметно чаще, чем истекает аренда.
		reaperPeriod = 50 * time.Millisecond
		// work — сколько спит обработчик: шесть сроков аренды. Он переживает
		// и отмену контекста, и истечение аренды, и возврат задания: иначе
		// исход записал бы сам зависший воркер, а не Reaper.
		work = 6 * lease
	)

	require.NoError(t, repo.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"hung":true}`),
	}), "постановка задания, которое зависнет")

	m := newFakeMetrics()
	hung := newHungHandler(work)
	// Движок собирается здесь, а не через newEngineWith: тесту нужны свои
	// AttemptTimeout, MaxExecutionTimeout и WatchdogGrace — предохранители
	// D15 в масштабе долей секунды.
	engine := New(repo, discardLogger(), Config{
		Workers:       1,
		PollInterval:  e2ePollInterval,
		Lease:         lease,
		ReaperPeriod:  reaperPeriod,
		WatchdogGrace: grace,
		Metrics:       m,
		WorkerID:      "hung-1",
	})
	require.NoError(t, engine.RegisterKind(topic, hung, queue.KindOptions{
		MaxAttempts:         3,
		AttemptTimeout:      attemptTimeout,
		MaxExecutionTimeout: ceiling,
		BackoffBase:         100 * time.Millisecond,
		BackoffMax:          time.Second,
	}), "регистрация темы %s", topic)
	defer func() { _ = engine.Close() }()
	engine.Start()

	select {
	case claim := <-hung.started:
		require.NotEmpty(t, claim.ID, "взятое задание приходит с идентификатором")
	case <-time.After(e2eTimeout):
		t.Fatal("обработчик не взял задание за отведённое время")
	}

	// (2) и (3): пока задание в processing, следим за сроком аренды — с
	// первого же замера после взятия, чтобы увидеть и исходное значение, и
	// продления heartbeat-а. Живой heartbeat сдвигает locked_until вперёд
	// каждые Lease/3; после отмены контекста попытки продление гаснет, и
	// значение замирает. Ждём, пока задание выйдет из processing, и требуем,
	// чтобы после последнего сдвига аренда простояла неизменной не меньше
	// Lease/2: это и есть «аренда перестала продлеваться».
	var (
		state      string
		attempts   int
		lastUntil  time.Time
		lastChange time.Time
		renewals   int
		frozenFor  time.Duration
	)
	deadline := time.Now().Add(e2eTimeout)
	for {
		var until *time.Time
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, attempts, locked_until FROM job WHERE topic = $1`, string(topic),
		).Scan(&state, &attempts, &until), "чтение строки задания темы %s", topic)

		if state != string(queue.StateProcessing) {
			frozenFor = time.Since(lastChange)
			break
		}
		require.NotNil(t, until, "задание в processing всегда с арендой")
		if !until.Equal(lastUntil) {
			if !lastUntil.IsZero() {
				renewals++
			}
			lastUntil = *until
			lastChange = time.Now()
		}
		if time.Now().After(deadline) {
			t.Fatalf("задание темы %s не вышло из processing за %s (state=%q, renewals=%d)",
				topic, e2eTimeout, state, renewals)
		}
		time.Sleep(e2ePollInterval)
	}

	assert.GreaterOrEqual(t, renewals, 1,
		"до отмены heartbeat продлевал аренду: иначе «перестала продлеваться» ничего не значит")
	assert.GreaterOrEqual(t, frozenFor, lease/2,
		"после отмены аренда перестала продлеваться: locked_until простоял неизменным %s (Lease=%s)",
		frozenFor, lease)

	// (1) Watchdog: контекст отменён, грация прошла, горутина не вышла —
	// счётчик зависаний вырос. Счётчик монотонный, а зависание
	// зафиксировано ещё до возврата аренды (грация — доли Lease), поэтому
	// проверка после цикла ничего не теряет.
	waitCounter(t, m, testMetricHungWorkersTotal, topicLabels(topic), 1)

	// (3) Reaper вернул задание в оборот: state = pending, попытки те же,
	// аренда снята. Воркер первого движка всё ещё занят зависшей
	// горутиной, поэтому вернуть строку в pending мог только сборщик.
	assert.Equal(t, string(queue.StatePending), state,
		"Reaper вернул задание в оборот после истечения аренды")
	assert.Equal(t, 1, attempts, "возврат по аренде не меняет счётчик попыток (инвариант 7)")
	waitCounter(t, m, testMetricLeaseExpiredTotal, topicLabels(topic), 1)

	row := readJobRow(t, pool, topic)
	assert.Nil(t, row.lockedBy, "возврат в оборот снимает аренду")
	assert.Equal(t, 1, row.attempts, "попытки при возврате не уменьшились")

	// Задание берётся снова: второй движок с обычным обработчиком
	// забирает вернувшуюся строку и записывает исход. Попыток стало
	// больше — возврат их не обнулил.
	second := &countingHandler{}
	reclaimer := newEngineWith(t, repo, topic, second, Config{
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		WorkerID:     "hung-reclaimer-1",
	})
	defer func() { _ = reclaimer.Close() }()
	reclaimer.Start()

	waitJobState(t, pool, topic, queue.StateDone)
	assert.Equal(t, 1, second.count(), "задание взято заново после возврата в оборот")

	done := readJobRow(t, pool, topic)
	assert.Equal(t, string(queue.StateDone), done.state)
	assert.Equal(t, 2, done.attempts,
		"второе взятие списало попытку: 1 → 2, при возврате счётчик не уменьшился")
}
