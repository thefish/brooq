// Тесты продления аренды (задача 4.6, D13/D14) на фейковом хранилище и
// фейковом порте метрик: база здесь не нужна — проверяется только
// поведение движка вокруг продления. Настоящую аренду проверяют
// e2e-тесты пакета.
package brooq

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// Порт метрик обязан быть тем же, что видит движок.
var _ Metrics = (*fakeMetrics)(nil)

// fakeMetrics — потокобезопасный счётчик вызовов порта Metrics: ключ
// складывается из имени метрики и отсортированных лейблов, поэтому
// порядок обхода map в движке на результат не влияет.
//
// Значения хранятся как float64: порт публикует дробные секунды (сумму
// длительностей попыток, возраст старейшего задания), и целочисленная
// map потеряла бы доли секунды — ровно тот дефект, который тесты обязаны
// ловить.
type fakeMetrics struct {
	mu       sync.Mutex
	counters map[string]float64
	gauges   map[string]float64
}

// newFakeMetrics возвращает пустой счётчик.
func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		counters: make(map[string]float64),
		gauges:   make(map[string]float64),
	}
}

// metricKey собирает устойчивый ключ метрики: имя и лейблы в
// отсортированном порядке.
func metricKey(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteString("|")
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
	}
	return b.String()
}

// Inc увеличивает счётчик на 1.
func (m *fakeMetrics) Inc(name string, labels map[string]string) {
	m.Add(name, labels, 1)
}

// Add увеличивает счётчик на целую delta.
func (m *fakeMetrics) Add(name string, labels map[string]string, delta uint64) {
	m.AddFloat(name, labels, float64(delta))
}

// AddFloat увеличивает счётчик на дробную delta: доли секунды
// сохраняются, а не отбрасываются.
func (m *fakeMetrics) AddFloat(name string, labels map[string]string, delta float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[metricKey(name, labels)] += delta
}

// Set устанавливает gauge в абсолютное дробное значение.
func (m *fakeMetrics) Set(name string, value float64, labels map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[metricKey(name, labels)] = value
}

// counter возвращает значение счётчика с заданными лейблами. Значение
// дробное: целые счётчики сравниваются с float64(want).
func (m *fakeMetrics) counter(name string, labels map[string]string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[metricKey(name, labels)]
}

// newUnitEngine собирает движок на фейковом хранилище без запуска
// горутин: юнит-тестам нужны только execute, watch и settle.
func newUnitEngine(repo *fakeRepo, cfg Config) *Queue {
	cfg.Workers = 1
	if cfg.WorkerID == "" {
		cfg.WorkerID = "test-worker"
	}
	return New(repo, discardLogger(), cfg)
}

// leaseCause — ячейка, в которую обработчик кладёт причину отмены:
// так видно, что heartbeat погасил попытку именно ErrLeaseLost, а не
// просто истёк по таймауту. Обработчик исполняется в горутине движка,
// поэтому доступ к полю — под мьютексом.
type leaseCause struct {
	mu    sync.Mutex
	cause error
}

// set запоминает причину отмены (nil — сброс перед подтестом).
func (c *leaseCause) set(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cause = err
}

// get возвращает запомненную причину.
func (c *leaseCause) get() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cause
}

// leaseLostHandler — обработчик, который ждёт отмены и записывает её
// причину в cause.
func leaseLostHandler(cause *leaseCause, guard time.Duration) func(context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			cause.set(context.Cause(ctx))
			return ctx.Err()
		case <-time.After(guard):
			return errors.New("аренда не объявлена потерянной за отведённое время")
		}
	}
}

// TestHeartbeatLeaseRenewal — таблица «ситуация → продление аренды»:
// живой обработчик продлевает аренду, ошибка запроса не гасит задачу
// (сбой базы — не потеря аренды, D14), а ноль изменённых строк гасит
// попытку причиной ErrLeaseLost и растит brooq_lease_lost_total.
func TestHeartbeatLeaseRenewal(t *testing.T) {
	const (
		// lease — срок аренды: период продления Lease/3 = 50ms.
		lease = 150 * time.Millisecond
		// work — сколько работает обработчик: заметно дольше периода
		// продления, поэтому продлений заведомо несколько. Это предмет
		// проверки, а не пауза «на всякий случай».
		work = 200 * time.Millisecond
		// guard — потолок ожидания отмены: тест не должен висеть вечно.
		guard = 5 * time.Second
		// topic — тема задания (лейбл метрики).
		topic = queue.Topic("тема")
	)

	// cause — ячейка с причиной отмены, увиденной обработчиком
	// (кейс потери аренды).
	cause := &leaseCause{}

	tests := []struct {
		name string
		// zeroRenewRows — продление возвращает 0 строк (аренда не наша).
		zeroRenewRows bool
		// renewErr — ошибка запроса продления (сбой базы).
		renewErr error
		// handle — тело обработчика.
		handle func(ctx context.Context) error
		// wantKind — ожидаемый исход попытки.
		wantKind settleKind
		// wantRenewsAtLeast — сколько продлений обязано случиться.
		wantRenewsAtLeast int
		// wantState — состояние строки после записи исхода.
		wantState string
		// wantLeaseLost — значение brooq_lease_lost_total{topic}.
		wantLeaseLost uint64
		// wantCause — причина отмены, которую обязан увидеть обработчик
		// (nil — не проверяется).
		wantCause error
	}{
		{
			name: "живой обработчик — аренда продлевается",
			handle: func(context.Context) error {
				time.Sleep(work)
				return nil
			},
			wantKind:          settleDone,
			wantRenewsAtLeast: 1,
			wantState:         string(queue.StateDone),
		},
		{
			name:     "ошибка продления — задача не гаснет, продление продолжается",
			renewErr: errors.New("сбой базы при продлении"),
			handle: func(context.Context) error {
				time.Sleep(work)
				return nil
			},
			wantKind:          settleDone,
			wantRenewsAtLeast: 2,
			wantState:         string(queue.StateDone),
		},
		{
			name:              "ноль строк — аренда потеряна, обработчик получает ErrLeaseLost",
			zeroRenewRows:     true,
			handle:            leaseLostHandler(cause, guard),
			wantKind:          settleNone,
			wantRenewsAtLeast: 1,
			wantState:         string(queue.StateProcessing),
			wantLeaseLost:     1,
			wantCause:         queue.ErrLeaseLost,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cause.set(nil)
			repo := newFakeRepo()
			if tc.zeroRenewRows {
				repo.renewRows = 0
			}
			repo.renewErr = tc.renewErr
			fm := newFakeMetrics()
			q := newUnitEngine(repo, Config{Lease: lease, Metrics: fm})

			k := kind{
				handler: handlerFunc(func(ctx context.Context, _ queue.Claim) error {
					return tc.handle(ctx)
				}),
				opts: queue.KindOptions{MaxAttempts: 3},
			}
			c := queue.Claim{
				ID:      "job-1",
				Topic:   topic,
				Attempt: 1,
				Lease:   queue.Lease{Worker: "test-worker", Until: time.Now().Add(lease)},
			}

			o := q.execute(context.Background(), c, k)
			if o.kind != tc.wantKind {
				t.Fatalf("вид исхода = %d, ожидался %d", o.kind, tc.wantKind)
			}

			if got := repo.renewCalls(); got < tc.wantRenewsAtLeast {
				t.Fatalf("продлений аренды %d, ожидалось не меньше %d", got, tc.wantRenewsAtLeast)
			}

			if tc.wantCause != nil && !errors.Is(cause.get(), tc.wantCause) {
				t.Fatalf("обработчик увидел причину отмены %v, ожидалась %v", cause.get(), tc.wantCause)
			}

			q.settle(context.Background(), c, o)

			if got := repo.snapshot().state; got != tc.wantState {
				t.Fatalf("состояние строки = %q, ожидалось %q", got, tc.wantState)
			}
			if got := fm.counter(metricLeaseLostTotal, map[string]string{labelTopic: string(topic)}); got != float64(tc.wantLeaseLost) {
				t.Fatalf("brooq_lease_lost_total{topic} = %g, ожидалось %d", got, tc.wantLeaseLost)
			}
		})
	}
}

// TestHeartbeatStopsAfterCancel — отмена контекста попытки гасит тикер
// немедленно (D15 п.1): после неё счётчик продлений не растёт, даже
// если подождать несколько периодов продления.
func TestHeartbeatStopsAfterCancel(t *testing.T) {
	const (
		// lease — срок аренды: период продления 50ms.
		lease = 150 * time.Millisecond
		// work — сколько обработчик работает до отмены: хватает на два
		// продления.
		work = 120 * time.Millisecond
		// quiet — сколько ждём после отмены: пять периодов продления.
		quiet = 250 * time.Millisecond
		// guard — потолок ожидания отмены внутри обработчика.
		guard = 5 * time.Second
		// topic — тема задания.
		topic = queue.Topic("тема")
	)

	repo := newFakeRepo()
	q := newUnitEngine(repo, Config{Lease: lease})

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	k := kind{
		handler: handlerFunc(func(ctx context.Context, _ queue.Claim) error {
			select {
			case <-time.After(work):
			case <-ctx.Done():
				return ctx.Err()
			}
			// Отмена контекста попытки снаружи — дальше продление
			// обязано прекратиться.
			cancel()
			select {
			case <-ctx.Done():
			case <-time.After(guard):
				return errors.New("отмена не дошла до обработчика за отведённое время")
			}
			return ctx.Err()
		}),
		opts: queue.KindOptions{MaxAttempts: 3},
	}
	c := queue.Claim{
		ID:      "job-1",
		Topic:   topic,
		Attempt: 1,
		Lease:   queue.Lease{Worker: "test-worker", Until: time.Now().Add(lease)},
	}

	o := q.execute(baseCtx, c, k)
	if o.kind != settleCanceled {
		t.Fatalf("вид исхода = %d, ожидался %d (отмена без явной причины)", o.kind, settleCanceled)
	}

	afterCancel := repo.renewCalls()
	if afterCancel < 1 {
		t.Fatalf("до отмены не было ни одного продления: %d", afterCancel)
	}

	// execute дождался выхода горутины продления, поэтому счётчик
	// обязан замереть.
	time.Sleep(quiet)
	if got := repo.renewCalls(); got != afterCancel {
		t.Fatalf("после отмены продлений %d, до неё было %d: тикер пережил попытку", got, afterCancel)
	}
}
