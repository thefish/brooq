// Интеграционные тесты метрик движка (задача 5.2, D11) на настоящей
// PostgreSQL: счётчики и gauge'и публикует сам движок, поэтому проверяются
// они на реальном исполнении заданий, а не на фейковом хранилище.
//
// Порт метрик подменён fakeMetrics (см. heartbeat_test.go): он
// потокобезопасен, а ключ складывается из имени и лейблов, поэтому порядок
// обхода map в движке на результат не влияет. Имена метрик и лейблы в
// тестах написаны дословно: контракт наблюдаемости задан ими (D11), а не
// константами пакета — так тест ловит и переименование метрики.
//
// Без TEST_DATABASE_URL тесты пропускаются (newTestPool → Skip), как и
// остальные e2e пакета.
package brooq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

// Имена метрик движка из D11 — дословно, как их видит реестр.
const (
	testMetricJobs                 = "brooq_jobs"
	testMetricReadyOldestAge       = "brooq_ready_oldest_age_seconds"
	testMetricClaimTotal           = "brooq_claim_total"
	testMetricSettleTotal          = "brooq_settle_total"
	testMetricAttemptDurationCount = "brooq_attempt_duration_seconds_count"
	testMetricAttemptDurationSum   = "brooq_attempt_duration_seconds_sum"
	testMetricLeaseLostTotal       = "brooq_lease_lost_total"
	testMetricLeaseExpiredTotal    = "brooq_lease_expired_total"
	testMetricHungWorkersTotal     = "brooq_hung_workers_total"
	testMetricWorkerBusy           = "brooq_worker_busy"
)

// Лейблы метрик движка из D11 — дословно.
const (
	testLabelTopic   = "topic"
	testLabelState   = "state"
	testLabelOutcome = "outcome"
)

// topicLabels — лейблы метрики одной темы.
func topicLabels(topic queue.Topic) map[string]string {
	return map[string]string{testLabelTopic: string(topic)}
}

// outcomeLabels — лейблы метрики «тема + исход».
func outcomeLabels(topic queue.Topic, outcome string) map[string]string {
	return map[string]string{testLabelTopic: string(topic), testLabelOutcome: outcome}
}

// stateLabels — лейблы метрики «тема + состояние».
func stateLabels(topic queue.Topic, state queue.State) map[string]string {
	return map[string]string{testLabelTopic: string(topic), testLabelState: string(state)}
}

// gaugeValue читает gauge фейкового порта: fakeMetrics держит gauge'и в
// отдельной map, а метод чтения у неё есть только для счётчиков.
// Значение дробное: возраст старейшего задания и занятость пула
// публикуются секундами с долями.
func gaugeValue(m *fakeMetrics, name string, labels map[string]string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gauges[metricKey(name, labels)]
}

// waitCounter ждёт, пока счётчик name с лейблами labels не станет равен
// want: значение публикует горутина движка, поэтому «сразу после» — это
// опрос с дедлайном, а не чтение. want целый: число взятий/исходов/попыток.
func waitCounter(t *testing.T, m *fakeMetrics, name string, labels map[string]string, want uint64) {
	t.Helper()

	deadline := time.Now().Add(e2eTimeout)
	for {
		got := m.counter(name, labels)
		if got == float64(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("счётчик %s равен %g, ожидалось %d за %s",
				metricKey(name, labels), got, want, e2eTimeout)
		}
		time.Sleep(e2ePollInterval)
	}
}

// waitCounterPositive ждёт, пока счётчик name с лейблами labels не станет
// строго больше нуля, и возвращает значение. Нужен дробному счётчику:
// точная сумма длительностей попыток зависит от времени исполнения,
// поэтому проверяется только то, что доля секунды сохранилась, а не
// превратилась в ноль.
func waitCounterPositive(t *testing.T, m *fakeMetrics, name string, labels map[string]string) float64 {
	t.Helper()

	deadline := time.Now().Add(e2eTimeout)
	for {
		got := m.counter(name, labels)
		if got > 0 {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("счётчик %s равен %g, ожидалось больше нуля за %s",
				metricKey(name, labels), got, e2eTimeout)
		}
		time.Sleep(e2ePollInterval)
	}
}

// waitGauge ждёт, пока gauge name с лейблами labels не удовлетворит
// условию ok: источник gauge — тикер статистики (StatsPeriod), поэтому
// значение появляется не мгновенно. Значение дробное, поэтому условие —
// функция от float64.
func waitGauge(t *testing.T, m *fakeMetrics, name string, labels map[string]string,
	ok func(float64) bool, want string) float64 {
	t.Helper()

	deadline := time.Now().Add(e2eTimeout)
	for {
		got := gaugeValue(m, name, labels)
		if ok(got) {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("gauge %s равен %g, ожидалось %s за %s",
				metricKey(name, labels), got, want, e2eTimeout)
		}
		time.Sleep(e2ePollInterval)
	}
}

// waitJobCount ждёт, пока в состоянии state у темы станет ровно want
// заданий: заданий в тесте несколько, поэтому waitJobState (одна строка)
// здесь не подходит.
func waitJobCount(t *testing.T, pool *pgxpool.Pool, topic queue.Topic, state queue.State, want int) {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(e2eTimeout)
	var got int
	for {
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1 AND state = $2`,
			string(topic), string(state),
		).Scan(&got); err != nil {
			t.Fatalf("чтение числа заданий темы %s в состоянии %q: %v", topic, state, err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("заданий темы %s в состоянии %q стало %d, ожидалось %d за %s",
				topic, state, got, want, e2eTimeout)
		}
		time.Sleep(e2ePollInterval)
	}
}

// TestMetricsEngineCounters — после исполнения N заданий счётчик взятий,
// счётчик записанных исходов и число исполненных попыток равны N, а
// занятость пула вернулась к нулю: gauge brooq_worker_busy не остаётся на
// единице после последнего задания.
func TestMetricsEngineCounters(t *testing.T) {
	const jobs = 3

	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "metrics-counters")

	for i := range jobs {
		require.NoError(t, repo.Enqueue(ctx, queue.Job{
			Topic:   topic,
			Payload: queue.Payload(fmt.Sprintf(`{"n":%d}`, i+1)),
		}), "постановка задания %d", i+1)
	}

	m := newFakeMetrics()
	handler := &countingHandler{}
	// StatsPeriod короче дефолтных 30 с: gauge brooq_jobs проверяется за
	// доли секунды, а не за полминуты.
	engine := newEngineWith(t, repo, topic, handler, Config{
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		StatsPeriod:  50 * time.Millisecond,
		Metrics:      m,
		WorkerID:     "metrics-counters-1",
	})
	defer func() { _ = engine.Close() }()
	engine.Start()

	waitJobCount(t, pool, topic, queue.StateDone, jobs)

	// Счётчики публикуются после записи исхода (D11): когда все N заданий
	// в done, значения обязаны быть равны N.
	waitCounter(t, m, testMetricClaimTotal, outcomeLabels(topic, "claimed"), jobs)
	waitCounter(t, m, testMetricSettleTotal, outcomeLabels(topic, "done"), jobs)
	waitCounter(t, m, testMetricAttemptDurationCount, topicLabels(topic), jobs)

	// Сумма длительностей попыток — дробная (AddFloat): у быстрых заданий
	// она была бы нулём, и средняя длительность sum/count, на которой
	// держится SLO, оказалась бы невычислимой. Точное значение зависит от
	// времени исполнения, поэтому проверяется «больше нуля».
	sum := waitCounterPositive(t, m, testMetricAttemptDurationSum, topicLabels(topic))
	assert.Positive(t, sum, "сумма длительностей попыток больше нуля: доли секунды не потеряны")

	// Занятость пула — gauge, а не счётчик: после последнего задания она
	// обязана вернуться к нулю, иначе наблюдатель видит вечно занятый слот.
	waitGauge(t, m, testMetricWorkerBusy, topicLabels(topic),
		func(v float64) bool { return v == 0 }, "0")

	// Число заданий по состояниям берётся из тикера статистики: done = N.
	waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StateDone),
		func(v float64) bool { return v == jobs }, fmt.Sprintf("%d", jobs))

	// Опустевшие состояния обнуляются, а не остаются на прошлом значении:
	// gauge обязан отражать текущее состояние очереди (D11). Ни ждущих, ни
	// исполняемых заданий у темы не осталось — обе серии обязаны быть нулём
	// (что обнуляется именно опубликованное значение, доказывает
	// TestMetricsGaugeDrainsToZero).
	waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StatePending),
		func(v float64) bool { return v == 0 }, "0")
	waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StateProcessing),
		func(v float64) bool { return v == 0 }, "0")

	// Исходы не смешиваются: успех пишется только в done, а не в retry,
	// dead или canceled.
	assert.Equal(t, jobs, handler.count(), "каждое задание исполнено ровно один раз")
	assert.Equal(t, float64(0), m.counter(testMetricSettleTotal, outcomeLabels(topic, "retry")),
		"успех не попадает в лейбл retry")
	assert.Equal(t, float64(0), m.counter(testMetricSettleTotal, outcomeLabels(topic, "canceled")),
		"успех не попадает в лейбл canceled")
}

// TestMetricsReadyOldestAge — возраст старейшего готового задания темы
// виден в brooq_ready_oldest_age_seconds, пока задание ждёт взятия.
// Воркера у темы нет вовсе: движок собран без регистрации темы, поэтому
// задание гарантированно остаётся готовым, а не уходит в done.
func TestMetricsReadyOldestAge(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "metrics-age")

	// available_at в прошлом: возраст считается от available_at, а не от
	// постановки, поэтому «старейшее готовое» сразу старше секунды —
	// значение заведомо отличимо от нуля и от «поля не публиковалось».
	require.NoError(t, repo.Enqueue(ctx, queue.Job{
		Topic:       topic,
		Payload:     queue.Payload(`{"waiting":true}`),
		AvailableAt: time.Now().Add(-2 * time.Second),
	}), "постановка ждущего задания")

	m := newFakeMetrics()
	// Темы нет в kinds: воркеров у неё нет, задание ждёт в pending. Тикер
	// статистики и сборщик аренд живут на движке, а не на теме, поэтому
	// Start без регистраций — рабочий сценарий «движок без воркеров».
	engine := New(repo, discardLogger(), Config{
		StatsPeriod: 50 * time.Millisecond,
		Metrics:     m,
	})
	engine.Start()
	defer func() { _ = engine.Close() }()

	age := waitGauge(t, m, testMetricReadyOldestAge, topicLabels(topic),
		func(v float64) bool { return v > 0 }, "> 0")
	assert.GreaterOrEqual(t, age, 1.0, "возраст ждущего задания измеряется секундами и уже больше нуля")
	assert.Less(t, age, 60.0, "возраст старейшего готового задания — секунды, а не часы")

	// Тик статистики публикует и число заданий: ждущее задание видно как
	// pending.
	waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StatePending),
		func(v float64) bool { return v == 1 }, "1")

	// Тема без готовых заданий в gauge не попадает: наблюдатель видит 0, а
	// не устаревшее значение прошлой темы.
	other := e2eTopic(t, "metrics-age-empty")
	assert.Equal(t, float64(0), gaugeValue(m, testMetricReadyOldestAge, topicLabels(other)),
		"у темы без готовых заданий возраста нет")
}

// TestMetricsCancelInFlight — отмена взятого задания растит
// brooq_lease_lost_total и НЕ растит brooq_settle_total{canceled}: строку
// перевёл отменяющий, а воркер, потерявший аренду, в неё не пишет (D14).
func TestMetricsCancelInFlight(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "metrics-cancel")

	m := newFakeMetrics()
	// Обработчик держит попытку, пока не отменят контекст: задание в полёте.
	handler := newBlockingHandler(0)
	engine := newEngineWith(t, repo, topic, handler, Config{
		PollInterval: e2ePollInterval,
		Lease:        300 * time.Millisecond,
		Metrics:      m,
		WorkerID:     "metrics-cancel-1",
	})
	defer func() { _ = engine.Close() }()

	require.NoError(t, repo.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"long":true}`),
	}), "постановка долгого задания")
	engine.Start()

	claim := <-handler.started

	require.NoError(t, engine.Cancel(ctx, claim.ID), "отмена взятого задания")
	<-handler.done

	// Аренда потеряна ровно один раз: продление не прошло guard по
	// состоянию (задание уже canceled).
	waitCounter(t, m, testMetricLeaseLostTotal, topicLabels(topic), 1)

	// Воркер исход не писал: строка осталась той, что записал отменяющий.
	assert.Equal(t, float64(0), m.counter(testMetricSettleTotal, outcomeLabels(topic, "canceled")),
		"воркер не перезаписывает решение отменяющего (D14)")
	assert.Equal(t, float64(0), m.counter(testMetricSettleTotal, outcomeLabels(topic, "done")),
		"отменённое задание не считается исполненным")
	assert.Equal(t, float64(1), m.counter(testMetricClaimTotal, outcomeLabels(topic, "claimed")),
		"задание взято ровно один раз")
}

// newStatsOnlyEngine собирает движок без зарегистрированных тем: Start
// поднимает только горутину статистики, поэтому gauge публикуются, а задания
// никто не берёт. Нужен, чтобы увидеть ОПУБЛИКОВАННОЕ значение серии до
// того, как его обнулит опустевшая тема: иначе «стало 0» неотличимо от
// «никогда не публиковалось».
func newStatsOnlyEngine(repo *postgres.Repository, m *fakeMetrics) *Queue {
	return New(repo, discardLogger(), Config{
		StatsPeriod: 50 * time.Millisecond,
		Metrics:     m,
		WorkerID:    "metrics-stats-only",
	})
}

// TestMetricsGaugeDrainsToZero — gauge отражает ТЕКУЩЕЕ состояние: серия,
// опубликованная в прошлый тик и отсутствующая в этом, публикуется нулём.
// Без зачистки опустевшая тема навсегда оставляет ненулевой
// brooq_ready_oldest_age_seconds (вечный ложный алерт «очередь встала») и
// фантомный brooq_jobs{state="processing"}.
func TestMetricsGaugeDrainsToZero(t *testing.T) {
	const jobs = 3

	// pending и processing обнуляются после исполнения: обе серии успевают
	// быть опубликованными ненулевыми, пока задания ждут и исполняются.
	t.Run("исполненные задания обнуляют pending и processing", func(t *testing.T) {
		pool := newTestPool(t)
		ctx := context.Background()
		repo := postgres.New(pool)
		topic := e2eTopic(t, "metrics-zero-jobs")

		for i := range jobs {
			require.NoError(t, repo.Enqueue(ctx, queue.Job{
				Topic:   topic,
				Payload: queue.Payload(fmt.Sprintf(`{"n":%d}`, i+1)),
			}), "постановка задания %d", i+1)
		}

		m := newFakeMetrics()
		// Сначала только статистика: ждущие задания обязаны попасть в gauge
		// как pending = N — иначе обнулять было бы нечего.
		stats := newStatsOnlyEngine(repo, m)
		defer func() { _ = stats.Close() }()
		stats.Start()

		waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StatePending),
			func(v float64) bool { return v == jobs }, fmt.Sprintf("%d", jobs))

		// Обработчик держит попытку заметно дольше периода статистики:
		// задание обязано попасть в gauge как processing.
		handler := newBlockingHandler(150 * time.Millisecond)
		worker := newEngineWith(t, repo, topic, handler, Config{
			PollInterval: e2ePollInterval,
			Lease:        30 * time.Second,
			WorkerID:     "metrics-zero-jobs-1",
		})
		defer func() { _ = worker.Close() }()
		worker.Start()

		waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StateProcessing),
			func(v float64) bool { return v == 1 }, "1")

		waitJobCount(t, pool, topic, queue.StateDone, jobs)

		// Серии опустели — обе обязаны стать нулём, а done — вырасти до N.
		waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StatePending),
			func(v float64) bool { return v == 0 }, "0")
		waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StateProcessing),
			func(v float64) bool { return v == 0 }, "0")
		waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StateDone),
			func(v float64) bool { return v == jobs }, fmt.Sprintf("%d", jobs))
	})

	// Возраст старейшего готового задания обнуляется, как только готовых
	// заданий не осталось: иначе «очередь встала» горит вечно.
	t.Run("снятая готовность обнуляет возраст старейшего задания", func(t *testing.T) {
		pool := newTestPool(t)
		ctx := context.Background()
		repo := postgres.New(pool)
		topic := e2eTopic(t, "metrics-zero-age")

		// available_at в прошлом: возраст заведомо отличим от нуля.
		require.NoError(t, repo.Enqueue(ctx, queue.Job{
			Topic:       topic,
			Payload:     queue.Payload(`{"waiting":true}`),
			AvailableAt: time.Now().Add(-2 * time.Second),
		}), "постановка ждущего задания")

		m := newFakeMetrics()
		stats := newStatsOnlyEngine(repo, m)
		defer func() { _ = stats.Close() }()
		stats.Start()

		age := waitGauge(t, m, testMetricReadyOldestAge, topicLabels(topic),
			func(v float64) bool { return v > 0 }, "> 0")
		assert.GreaterOrEqual(t, age, 1.0, "возраст ждущего задания больше нуля секунд")

		// Готовность снимается обработкой: задание уходит в done, готовых
		// заданий у темы не остаётся.
		worker := newEngine(t, repo, "metrics-zero-age-1", topic, &countingHandler{})
		defer func() { _ = worker.Close() }()
		worker.Start()

		waitJobCount(t, pool, topic, queue.StateDone, 1)

		waitGauge(t, m, testMetricReadyOldestAge, topicLabels(topic),
			func(v float64) bool { return v == 0 }, "0")
		waitGauge(t, m, testMetricJobs, stateLabels(topic, queue.StatePending),
			func(v float64) bool { return v == 0 }, "0")
	})
}

// TestMetricsLeaseExpired — brooq_lease_expired_total: строка с истёкшей
// арендой возвращается сборщиком, и счётчик растёт на каждое возвращённое
// задание. Это единственная из метрик D11, у которой не было ассерта.
func TestMetricsLeaseExpired(t *testing.T) {
	pool := newTestPool(t)
	repo := postgres.New(pool)
	topic := e2eTopic(t, "metrics-lease-expired")

	// Строка умершего воркера: взята (попыток 2), аренда истекла минуту
	// назад. Истечение аренды — признак упавшего процесса, а не записанного
	// сбоя, поэтому попытки при возврате не меняются.
	insertLeaseRow(t, pool, topic, queue.StateProcessing, 2, "мёртвый-воркер", -time.Minute)

	m := newFakeMetrics()
	// Тем не регистрируем: Start поднимает только сборщик, поэтому вернуть
	// строку в оборот может только он — взятия в этом тесте нет.
	engine := New(repo, discardLogger(), Config{
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		ReaperPeriod: reaperTestPeriod,
		Metrics:      m,
		WorkerID:     "metrics-lease-expired-1",
	})
	defer func() { _ = engine.Close() }()
	engine.Start()

	got := waitCounterPositive(t, m, testMetricLeaseExpiredTotal, topicLabels(topic))
	assert.Equal(t, float64(1), got, "сборщик вернул одно задание — счётчик вырос ровно на единицу")

	waitJobState(t, pool, topic, queue.StatePending)
	row := readJobRow(t, pool, topic)
	assert.Equal(t, string(queue.StatePending), row.state, "возврат по аренде вернул задание в оборот")
	assert.Equal(t, 2, row.attempts, "возврат по аренде не меняет счётчик попыток")
	assert.Nil(t, row.lockedBy, "возврат по аренде снимает владельца")
}
