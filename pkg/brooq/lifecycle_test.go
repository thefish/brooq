// Тесты жизненного цикла движка (D7, D8) на фейковом хранилище: пустая
// очередь, идемпотентность Stop/Close, отсутствие утечки горутин и
// отсутствие наложения итераций у медленного обработчика. База не нужна —
// проверяется поведение самого движка, а не SQL.
package brooq

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// goroutineTolerance — допуск на горутины, которые тестовый процесс
// держит помимо движка (планировщик, рантайм, пул соседних тестов). Строгий
// ноль здесь дал бы флак, а не сигнал.
const goroutineTolerance = 2

// TestLifecycleEmptyQueue — пустая очередь не ошибка: воркер крутит
// поллинг, ничего не пишет, не паникует (worker.go, claim: «Пустая очередь
// — не ошибка: просто следующий тик»).
func TestLifecycleEmptyQueue(t *testing.T) {
	tests := []struct {
		name    string
		workers int
	}{
		{"один воркер", 1},
		{"три воркера", 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			// nextClaim == nil: очередь пуста навсегда.
			q := New(repo, discardLogger(), Config{
				Workers:      tc.workers,
				PollInterval: time.Millisecond,
				WorkerID:     "test-worker",
			})
			// Тема зарегистрирована, но заданий нет: без регистрации Start
			// вообще не поднял бы воркеров.
			if err := q.RegisterKind("тема", handlerFunc(func(context.Context, queue.Claim) error {
				t.Fatalf("обработчик не должен вызываться на пустой очереди")
				return nil
			}), queue.KindOptions{MaxAttempts: 1}); err != nil {
				t.Fatalf("регистрация темы: %v", err)
			}
			q.Start()

			// Несколько итераций поллинга: каждый слот обязан спросить
			// задания хотя бы раз.
			waitFor(t, "слоты воркеров опросили пустую очередь", 2*time.Second, func() bool {
				return repo.snapshot().claimCalls >= tc.workers
			})

			if err := q.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got := repo.snapshot()
			if got.writes != 0 {
				t.Fatalf("пустая очередь не пишет исход: зафиксировано %d записей", got.writes)
			}
			if got.state != string(queue.StateProcessing) {
				t.Fatalf("состояние строки изменилось: %q", got.state)
			}
		})
	}
}

// TestLifecycleStopIdempotent — Stop идемпотентен, а после Close живых
// горутин не остаётся: повторные вызовы не паникуют и не оставляют
// работающих воркеров.
func TestLifecycleStopIdempotent(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, q *Queue)
	}{
		{
			name: "повторный Stop",
			run: func(_ *testing.T, q *Queue) {
				q.Start()
				q.Stop()
				q.Stop()
			},
		},
		{
			name: "Stop после Close",
			run: func(t *testing.T, q *Queue) {
				q.Start()
				mustClose(t, q)
				q.Stop()
			},
		},
		{
			name: "Close дважды",
			run: func(t *testing.T, q *Queue) {
				q.Start()
				mustClose(t, q)
				mustClose(t, q)
			},
		},
		{
			name: "Stop без Start",
			run: func(_ *testing.T, q *Queue) {
				q.Stop()
			},
		},
		{
			name: "Start после Close ничего не запускает",
			run: func(t *testing.T, q *Queue) {
				mustClose(t, q)
				q.Start()
				q.Stop()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := runtime.NumGoroutine()

			repo := newFakeRepo()
			q := New(repo, discardLogger(), Config{
				Workers:      2,
				PollInterval: time.Millisecond,
				WorkerID:     "test-worker",
			})
			// Паника внутри run — провал теста: Stop и Close обязаны быть
			// идемпотентными.
			tc.run(t, q)

			assertNoGoroutineLeak(t, base)
			if got := repo.snapshot().writes; got != 0 {
				t.Fatalf("пустая очередь не пишет исход: зафиксировано %d записей", got)
			}
		})
	}
}

// TestLifecycleSlowHandlerDoesNotOverlap — медленный обработчик не даёт
// наложиться итерациям: каждая горутина-слот исполняет одну попытку за раз
// (worker.go, runWorker), поэтому одновременных попыток не больше числа
// воркеров.
func TestLifecycleSlowHandlerDoesNotOverlap(t *testing.T) {
	tests := []struct {
		name    string
		workers int
	}{
		{"один воркер — строго одна попытка за раз", 1},
		{"три воркера — не больше трёх одновременных попыток", 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			var seq atomic.Int64
			// Задания не кончаются: проверяется сериализация итераций, а не
			// скорость выборки.
			repo.nextClaim = func(int) []queue.Claim {
				n := seq.Add(1)
				return []queue.Claim{{ID: fmt.Sprintf("job-%d", n), Topic: "тема", Attempt: 1}}
			}

			var (
				mu            sync.Mutex
				current       int
				maxConcurrent int
				done          int
			)
			handler := handlerFunc(func(context.Context, queue.Claim) error {
				mu.Lock()
				current++
				if current > maxConcurrent {
					maxConcurrent = current
				}
				mu.Unlock()

				defer func() {
					mu.Lock()
					current--
					done++
					mu.Unlock()
				}()

				// Медленный обработчик: без сериализации итерации наехали бы
				// друг на друга.
				time.Sleep(15 * time.Millisecond)
				return nil
			})

			q := New(repo, discardLogger(), Config{
				Workers:      tc.workers,
				PollInterval: time.Millisecond,
				WorkerID:     "test-worker",
			})
			if err := q.RegisterKind("тема", handler, queue.KindOptions{
				MaxAttempts: 3,
				BackoffBase: time.Millisecond,
				BackoffMax:  time.Millisecond,
			}); err != nil {
				t.Fatalf("регистрация темы: %v", err)
			}
			q.Start()

			waitFor(t, "обработчик исполнил несколько попыток", 5*time.Second, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return done >= tc.workers*3
			})
			if err := q.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			mu.Lock()
			defer mu.Unlock()
			if maxConcurrent < 1 {
				t.Fatalf("обработчик ни разу не исполнился")
			}
			if maxConcurrent > tc.workers {
				t.Fatalf("одновременных попыток %d при %d воркерах: итерации наложились",
					maxConcurrent, tc.workers)
			}
			if tc.workers == 1 && maxConcurrent != 1 {
				t.Fatalf("при одном воркере одновременных попыток %d, ожидалась ровно 1", maxConcurrent)
			}
		})
	}
}

// mustClose закрывает движок и валит тест, если Close вернул ошибку.
func mustClose(t *testing.T, q *Queue) {
	t.Helper()
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// assertNoGoroutineLeak ждёт, пока число горутин вернётся к исходному:
// после Close работающих воркеров не остаётся. Повторные замеры и допуск
// нужны потому, что умирающие горутины соседних тестов пакета исчезают не
// мгновенно, а планировщик не обязан отчитываться сразу.
func assertNoGoroutineLeak(t *testing.T, base int) {
	t.Helper()

	const timeout = 2 * time.Second
	deadline := time.Now().Add(timeout)
	best := runtime.NumGoroutine()
	for {
		if n := runtime.NumGoroutine(); n < best {
			best = n
		}
		if best <= base+goroutineTolerance {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("после Close остались горутины: %d против %d до запуска (допуск %d)",
				best, base, goroutineTolerance)
		}
		runtime.Gosched()
		time.Sleep(5 * time.Millisecond)
	}
}
