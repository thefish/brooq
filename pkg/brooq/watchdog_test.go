// Тесты watchdog зависших обработчиков (задача 4.7, D15 п.3) на фейковом
// хранилище: грация отсчитывается после отмены контекста попытки, и по
// её истечении растёт brooq_hung_workers_total{topic}. Горутину в Go
// убить нельзя, поэтому watchdog ничего не прерывает — он только
// фиксирует факт.
package brooq

import (
	"context"
	"testing"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// TestWatchdogHungWorkers — таблица «обработчик → метрика зависания»:
// обработчик, игнорирующий отмену, через грацию попадает в счётчик, а
// обработчик, уважающий контекст, — нет.
func TestWatchdogHungWorkers(t *testing.T) {
	const (
		// attemptTimeout — когда обработчик получает отмену контекста.
		attemptTimeout = 20 * time.Millisecond
		// grace — грация watchdog (Config.WatchdogGrace): тест не должен
		// ждать секунды.
		grace = 50 * time.Millisecond
		// work — сколько работает обработчик, игнорирующий отмену:
		// заведомо дольше attemptTimeout + grace.
		work = 300 * time.Millisecond
		// quiet — сколько ждём после попытки, чтобы убедиться, что метрика
		// не выросла: четыре грации.
		quiet = 4 * grace
		// check — потолок ожидания метрики зависания.
		check = 2 * time.Second
		// topic — тема задания (лейбл метрики).
		topic = queue.Topic("тема")
	)

	tests := []struct {
		name string
		// handle — тело обработчика.
		handle func(ctx context.Context) error
		// wantHung — ожидаемое значение brooq_hung_workers_total{topic}.
		wantHung uint64
	}{
		{
			name: "обработчик игнорирует отмену — зависание зафиксировано",
			handle: func(context.Context) error {
				// Отмену не замечаем намеренно: так выглядит зависший
				// обработчик.
				time.Sleep(work)
				return nil
			},
			wantHung: 1,
		},
		{
			name:     "обработчик уважает отмену — зависания нет",
			handle:   blockUntilDone,
			wantHung: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			fm := newFakeMetrics()
			q := newUnitEngine(repo, Config{
				Lease:         time.Minute,
				WatchdogGrace: grace,
				Metrics:       fm,
			})

			k := kind{
				handler: handlerFunc(func(ctx context.Context, _ queue.Claim) error {
					return tc.handle(ctx)
				}),
				opts: queue.KindOptions{MaxAttempts: 3, AttemptTimeout: attemptTimeout},
			}
			c := queue.Claim{
				ID:      "job-1",
				Topic:   topic,
				Attempt: 1,
				Lease:   queue.Lease{Worker: "test-worker", Until: time.Now().Add(time.Minute)},
			}

			q.execute(context.Background(), c, k)

			labels := map[string]string{labelTopic: string(topic)}
			if tc.wantHung == 0 {
				// Грация уже прошла за время работы обработчика; выжидаем
				// ещё, чтобы поймать запоздавший счётчик.
				time.Sleep(quiet)
				if got := fm.counter(metricHungWorkersTotal, labels); got != 0 {
					t.Fatalf("brooq_hung_workers_total{topic} = %g, ожидалось 0: обработчик вышел по отмене", got)
				}
				return
			}

			waitFor(t, "watchdog зафиксировал зависшего обработчика", check, func() bool {
				return fm.counter(metricHungWorkersTotal, labels) == float64(tc.wantHung)
			})
		})
	}
}
