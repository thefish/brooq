// Тесты записи исхода попытки (D5, D14, D15) на фейковом хранилище: база
// здесь не нужна — проверяется только решение движка «какой исход и как
// записать». Настоящее хранилище проверяют e2e-тесты пакета.
package brooq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// Порядок проверки: фейк обязан удовлетворять порту движка — иначе тесты
// проверяли бы не тот контракт, с которым работает продакшен.
var _ Repo = (*fakeRepo)(nil)

// discardLogger глушит вывод движка: он не предмет проверки.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// handlerFunc — обработчик из функции: табличным кейсам нужен свой.
type handlerFunc func(ctx context.Context, c queue.Claim) error

// Handle реализует queue.Handler.
func (f handlerFunc) Handle(ctx context.Context, c queue.Claim) error { return f(ctx, c) }

// waitFor ждёт выполнения условия: опрос с коротким шагом вместо «сна на
// всякий случай», при провале — внятное сообщение.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s (за %s)", what, timeout)
		}
		time.Sleep(time.Millisecond)
	}
}

// fakeOp — одна запись исхода внутри фейковой транзакции.
type fakeOp struct {
	kind        string
	id          string
	cause       string
	availableAt time.Time
	// worker — владелец аренды, переданный движком: guard владения в SQL
	// требует locked_by = worker, поэтому движок обязан передавать именно
	// c.Lease.Worker, а не что-то иное.
	worker string
}

// fakeRepo — фейковое хранилище порта Repo. Моделирует ровно то, что
// важно движку: транзакцию записи, её фиксацию и итоговое состояние
// строки. Записи применяются к состоянию только в Commit — поэтому
// откат виден как неизменённое состояние, а не как «ничего не было».
type fakeRepo struct {
	mu sync.Mutex

	// state, cause, availableAt, attempts — то, что «лежит в строке».
	state       string
	cause       string
	availableAt time.Time
	attempts    int

	// writes — число зафиксированных записей исхода.
	writes int
	// begins — сколько раз движок открывал транзакцию.
	begins int
	// lastTx — последняя открытая транзакция (для проверки OnSucceeded).
	lastTx *fakeTx
	// zeroRows — подставить ответ «0 строк» на любую запись исхода.
	zeroRows bool
	// nextClaim — поставщик заданий для цикла поллинга; nil — очередь пуста.
	nextClaim func(call int) []queue.Claim
	// claimCalls — сколько раз движок просил задания.
	claimCalls int
	// renews — сколько раз heartbeat продлевал аренду.
	renews int
	// renewRows — сколько строк возвращает продление аренды (0 —
	// «аренда не моя», D14).
	renewRows int64
	// renewErr — ошибка запроса продления (сбой базы, а не потеря
	// аренды).
	renewErr error
	// enqueueTxCalls — сколько раз движок ставил следующее задание в
	// транзакции закрытия текущего (OnSucceeded).
	enqueueTxCalls int
	// enqueueTxErr — ошибка постановки следующего задания.
	enqueueTxErr error
	// enqueueTxJobs — задания, переданные в EnqueueTx, в порядке вызовов.
	enqueueTxJobs []queue.Job
	// enqueueTxBeforeCommit — была ли транзакция ещё не зафиксирована в
	// момент вызова EnqueueTx: так видно порядок «постановка до Commit».
	enqueueTxBeforeCommit []bool
	// enqueued — задания, видимые в очереди: попадают сюда только при
	// фиксации транзакции.
	enqueued []queue.Job
}

// newFakeRepo возвращает фейковое хранилище с одним взятым заданием:
// строка в processing, попытка уже списана (D5).
func newFakeRepo() *fakeRepo {
	return &fakeRepo{state: string(queue.StateProcessing), attempts: 1, renewRows: 1}
}

// renewCalls — сколько раз движок продлевал аренду.
func (r *fakeRepo) renewCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.renews
}

// fakeSnapshot — снимок состояния фейка под мьютексом.
type fakeSnapshot struct {
	state       string
	cause       string
	availableAt time.Time
	attempts    int
	writes      int
	begins      int
	claimCalls  int
}

// snapshot возвращает согласованный снимок состояния фейка.
func (r *fakeRepo) snapshot() fakeSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fakeSnapshot{
		state:       r.state,
		cause:       r.cause,
		availableAt: r.availableAt,
		attempts:    r.attempts,
		writes:      r.writes,
		begins:      r.begins,
		claimCalls:  r.claimCalls,
	}
}

// txUsed возвращает последнюю открытую транзакцию.
func (r *fakeRepo) txUsed() *fakeTx {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastTx
}

// lastOps возвращает копию записей последней транзакции: по ним видно,
// что именно движок передал в порт (какой исход, с каким воркером).
func (r *fakeRepo) lastOps() []fakeOp {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastTx == nil {
		return nil
	}
	return append([]fakeOp(nil), r.lastTx.ops...)
}

// Claim берёт задания из поставщика nextClaim (limit фейк не дробит:
// тестам движка достаточно одного задания на итерацию).
func (r *fakeRepo) Claim(_ context.Context, _ queue.Tx, _ queue.Topic, _ string,
	_ time.Duration, _ int) ([]queue.Claim, error) {

	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimCalls++
	if r.nextClaim == nil {
		return nil, nil
	}
	return r.nextClaim(r.claimCalls), nil
}

// record складывает запись исхода в транзакцию вызывающего: к строке она
// применится только в Commit.
func (r *fakeRepo) record(tx queue.Tx, op fakeOp) (int64, error) {
	ft, ok := tx.(*fakeTx)
	if !ok {
		return 0, fmt.Errorf("фейк: ожидалась своя транзакция, получено %T", tx)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.zeroRows {
		// Guard не пропустил: ни строки, ни записи в транзакции.
		return 0, nil
	}
	ft.ops = append(ft.ops, op)
	return 1, nil
}

// MarkDone записывает успех.
func (r *fakeRepo) MarkDone(_ context.Context, tx queue.Tx, id, worker string) (int64, error) {
	return r.record(tx, fakeOp{kind: "done", id: id, worker: worker})
}

// MarkFailed записывает сбой с моментом следующей попытки.
func (r *fakeRepo) MarkFailed(_ context.Context, tx queue.Tx, id, worker, cause string, availableAt time.Time) (int64, error) {
	return r.record(tx, fakeOp{kind: "failed", id: id, worker: worker, cause: cause, availableAt: availableAt})
}

// MarkFailedPreservingAttempt записывает сбой без расхода попытки.
func (r *fakeRepo) MarkFailedPreservingAttempt(_ context.Context, tx queue.Tx, id, worker, cause string, availableAt time.Time) (int64, error) {
	return r.record(tx, fakeOp{kind: "failedPreserving", id: id, worker: worker, cause: cause, availableAt: availableAt})
}

// MarkDead записывает терминальный сбой.
func (r *fakeRepo) MarkDead(_ context.Context, tx queue.Tx, id, worker, cause string) (int64, error) {
	return r.record(tx, fakeOp{kind: "dead", id: id, worker: worker, cause: cause})
}

// MarkCanceled записывает отмену.
func (r *fakeRepo) MarkCanceled(_ context.Context, tx queue.Tx, id string) (int64, error) {
	return r.record(tx, fakeOp{kind: "canceled", id: id})
}

// RenewLease продлевает аренду в фейке: число строк задаётся renewRows,
// ошибка запроса — renewErr (D13). Аренда не моделируется — важно лишь,
// что guard прошёл (1) или не прошёл (0).
func (r *fakeRepo) RenewLease(_ context.Context, _ queue.Tx, _, _ string, _ time.Duration) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.renews++
	if r.renewErr != nil {
		return 0, r.renewErr
	}
	return r.renewRows, nil
}

// Begin открывает фейковую транзакцию.
func (r *fakeRepo) Begin(_ context.Context) (queue.Tx, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.begins++
	tx := &fakeTx{repo: r}
	r.lastTx = tx
	return tx, nil
}

// Enqueue в фейке ничего не делает: постановку проверяют e2e-тесты.
func (r *fakeRepo) Enqueue(_ context.Context, _ queue.Job) error { return nil }

// ReapExpired в фейке ничего не возвращает: возврат истёкших аренд
// проверяет сценарий сборщика (задача 4.5).
func (r *fakeRepo) ReapExpired(_ context.Context, _ queue.Tx) ([]queue.ReapedJob, error) {
	return nil, nil
}

// DeleteTerminal в фейке ничего не удаляет: уборку терминальных строк
// проверяет свой сценарий.
func (r *fakeRepo) DeleteTerminal(_ context.Context, _ queue.Tx, _ time.Duration, _ int) ([]queue.DeletedJob, error) {
	return nil, nil
}

// EnqueueTx ставит задание в транзакции вызывающего: задание становится
// видно в очереди только при её фиксации, а ошибка постановки откатывает
// всю транзакцию (D9).
func (r *fakeRepo) EnqueueTx(_ context.Context, tx queue.Tx, job queue.Job) error {
	ft, ok := tx.(*fakeTx)
	if !ok {
		return fmt.Errorf("фейк: ожидалась своя транзакция, получено %T", tx)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueueTxCalls++
	r.enqueueTxJobs = append(r.enqueueTxJobs, job)
	r.enqueueTxBeforeCommit = append(r.enqueueTxBeforeCommit, !ft.committed)
	if r.enqueueTxErr != nil {
		return r.enqueueTxErr
	}
	ft.enqueued = append(ft.enqueued, job)
	return nil
}

// enqueueTxSnapshot — снимок постановок следующего задания: число
// вызовов, переданные задания и признак «транзакция ещё не была
// зафиксирована» для каждого вызова.
func (r *fakeRepo) enqueueTxSnapshot() (int, []queue.Job, []bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.enqueueTxCalls,
		append([]queue.Job(nil), r.enqueueTxJobs...),
		append([]bool(nil), r.enqueueTxBeforeCommit...)
}

// enqueuedJobs — задания, видимые в очереди после зафиксированных
// транзакций.
func (r *fakeRepo) enqueuedJobs() []queue.Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]queue.Job(nil), r.enqueued...)
}

// CountByState в фейке пуст: чтения для метрик проверяет интеграционный
// тест хранилища (задача 3.5).
func (r *fakeRepo) CountByState(_ context.Context) ([]queue.TopicStateCount, error) {
	return nil, nil
}

// OldestReadyAge в фейке пуст: чтения для метрик проверяет
// интеграционный тест хранилища (задача 3.5).
func (r *fakeRepo) OldestReadyAge(_ context.Context) ([]queue.TopicAge, error) {
	return nil, nil
}

// applyLocked переносит записи зафиксированной транзакции в состояние
// строки.
func (r *fakeRepo) applyLocked(ops []fakeOp) {
	for _, op := range ops {
		r.writes++
		switch op.kind {
		case "done":
			r.state = string(queue.StateDone)
			r.cause = ""
			r.availableAt = time.Time{}
		case "failed":
			r.state = string(queue.StateFailed)
			r.cause = op.cause
			r.availableAt = op.availableAt
		case "failedPreserving":
			// Попытка возвращается: взятие её уже списало (D5).
			r.attempts--
			r.state = string(queue.StateFailed)
			r.cause = op.cause
			r.availableAt = op.availableAt
		case "dead":
			r.state = string(queue.StateDead)
			r.cause = op.cause
		case "canceled":
			r.state = string(queue.StateCanceled)
		}
	}
}

// fakeTx — транзакция фейкового хранилища: записи копятся до Commit.
type fakeTx struct {
	repo      *fakeRepo
	ops       []fakeOp
	enqueued  []queue.Job
	committed bool
}

// Exec фейк не поддерживает: движок пишет исход методами порта, а не SQL.
func (t *fakeTx) Exec(_ context.Context, _ string, _ ...any) (queue.CommandTag, error) {
	return nil, errors.New("фейк: Exec при записи исхода не используется")
}

// Query фейк не поддерживает.
func (t *fakeTx) Query(_ context.Context, _ string, _ ...any) (queue.Rows, error) {
	return nil, errors.New("фейк: Query при записи исхода не используется")
}

// QueryRow фейк не поддерживает.
func (t *fakeTx) QueryRow(_ context.Context, _ string, _ ...any) queue.Row {
	return nil
}

// Begin открывает вложенную фейковую транзакцию.
func (t *fakeTx) Begin(_ context.Context) (queue.Tx, error) {
	return &fakeTx{repo: t.repo}, nil
}

// Commit фиксирует записи: только здесь они попадают в строку.
func (t *fakeTx) Commit(_ context.Context) error {
	t.repo.mu.Lock()
	defer t.repo.mu.Unlock()
	if t.committed {
		return nil
	}
	t.committed = true
	t.repo.applyLocked(t.ops)
	// Постановка следующего задания фиксируется вместе с закрытием
	// текущего — только теперь оно видно в очереди.
	t.repo.enqueued = append(t.repo.enqueued, t.enqueued...)
	return nil
}

// Rollback откатывает транзакцию: записи в строку не попадают.
func (t *fakeTx) Rollback(_ context.Context) error { return nil }

// blockUntilDone — корректный обработчик, уважающий контекст: ждёт отмены
// и возвращает её причину.
func blockUntilDone(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// canceledWith возвращает базовый контекст попытки, уже прерванный с
// заданной причиной.
func canceledWith(cause error) func(t *testing.T) context.Context {
	return func(t *testing.T) context.Context {
		t.Helper()
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(nil) })
		cancel(cause)
		return ctx
	}
}

// TestSettleOutcomeMapping — таблица «ситуация → исход»: успех, обычный
// сбой, исчерпание попыток, таймауты (D15), сигналы обработчика (D14),
// потеря аренды (D14) и паника. Проверяется и вид исхода, и то, что
// реально оказалось записано в строку.
func TestSettleOutcomeMapping(t *testing.T) {
	const (
		// maxAttempts — потолок попыток темы во всех кейсах.
		maxAttempts = 3
		// backoffMax — потолок отсрочки: граница для ErrMustDelay.
		backoffMax = 5 * time.Second
		// backoffBase — базовая отсрочка темы.
		backoffBase = 100 * time.Millisecond
		// window — допуск на неточность таймера между «до» и записью.
		window = 500 * time.Millisecond
	)

	tests := []struct {
		name string
		// attempt — номер состоявшейся попытки.
		attempt int
		// attemptTimeout и maxExecution — таймауты темы (0 — не задан).
		attemptTimeout time.Duration
		maxExecution   time.Duration
		// baseCtx — базовый контекст попытки; nil — живой.
		baseCtx func(t *testing.T) context.Context
		// handle — тело обработчика.
		handle func(ctx context.Context) error
		// wantKind — ожидаемый вид исхода.
		wantKind settleKind
		// wantState — ожидаемое состояние строки после фиксации.
		wantState string
		// wantDelay — ожидаемая отсрочка (0 — не проверяется точно).
		wantDelay time.Duration
		// wantCause — подстрока last_error ("" — не проверяется;
		// canceled его не пишет: markCanceledSQL трогает только состояние).
		wantCause string
	}{
		{
			name:      "успех — done",
			attempt:   1,
			handle:    func(context.Context) error { return nil },
			wantKind:  settleDone,
			wantState: string(queue.StateDone),
		},
		{
			name:      "обычная ошибка — failed с отсрочкой",
			attempt:   1,
			handle:    func(context.Context) error { return errors.New("внешний сервис недоступен") },
			wantKind:  settleFailed,
			wantState: string(queue.StateFailed),
			wantCause: "внешний сервис недоступен",
		},
		{
			name:      "обычная ошибка на последней попытке — dead",
			attempt:   maxAttempts,
			handle:    func(context.Context) error { return errors.New("внешний сервис недоступен") },
			wantKind:  settleDead,
			wantState: string(queue.StateDead),
			wantCause: "внешний сервис недоступен",
		},
		{
			name:           "истёк AttemptTimeout — сбой попытки с повтором",
			attempt:        1,
			attemptTimeout: 10 * time.Millisecond,
			handle:         blockUntilDone,
			wantKind:       settleFailed,
			wantState:      string(queue.StateFailed),
			wantCause:      "deadline exceeded",
		},
		{
			name:           "истёк AttemptTimeout на последней попытке — dead по исчерпанию попыток",
			attempt:        maxAttempts,
			attemptTimeout: 10 * time.Millisecond,
			handle:         blockUntilDone,
			wantKind:       settleDead,
			wantState:      string(queue.StateDead),
			wantCause:      "deadline exceeded",
		},
		{
			name:         "потолок исполнения темы исчерпан — failed, а не dead",
			attempt:      1,
			maxExecution: 10 * time.Millisecond,
			handle:       blockUntilDone,
			wantKind:     settleFailed,
			wantState:    string(queue.StateFailed),
			wantCause:    queue.ErrMaxExecutionExceeded.Error(),
		},
		{
			name:         "потолок исполнения на последней попытке — dead по исчерпанию попыток",
			attempt:      maxAttempts,
			maxExecution: 10 * time.Millisecond,
			handle:       blockUntilDone,
			wantKind:     settleDead,
			wantState:    string(queue.StateDead),
			wantCause:    queue.ErrMaxExecutionExceeded.Error(),
		},
		{
			name:      "аренда потеряна — исход не пишется",
			attempt:   1,
			baseCtx:   canceledWith(queue.ErrLeaseLost),
			handle:    blockUntilDone,
			wantKind:  settleNone,
			wantState: string(queue.StateProcessing),
		},
		{
			name:      "остановка движка — исход не пишется",
			attempt:   1,
			baseCtx:   canceledWith(errQueueStopped),
			handle:    blockUntilDone,
			wantKind:  settleNone,
			wantState: string(queue.StateProcessing),
		},
		{
			name:      "отмена без явной причины — canceled",
			attempt:   1,
			baseCtx:   canceledWith(context.Canceled),
			handle:    blockUntilDone,
			wantKind:  settleCanceled,
			wantState: string(queue.StateCanceled),
		},
		{
			name:    "сигнал отмены — canceled на любой попытке",
			attempt: 1,
			handle: func(context.Context) error {
				return fmt.Errorf("домен: %w", queue.ErrMustCancelTask)
			},
			wantKind:  settleCanceled,
			wantState: string(queue.StateCanceled),
		},
		{
			name:    "сигнал сохранения попытки — failed без расхода попытки",
			attempt: 1,
			handle: func(context.Context) error {
				return fmt.Errorf("валидация: %w", queue.ErrMustPreserveAttempt)
			},
			wantKind:  settleFailedPreserving,
			wantState: string(queue.StateFailed),
			wantCause: queue.ErrMustPreserveAttempt.Error(),
		},
		{
			name:    "отсрочка обработчика — failed с его интервалом",
			attempt: 1,
			handle: func(context.Context) error {
				return queue.MustDelay(2 * time.Second)
			},
			wantKind:  settleFailed,
			wantState: string(queue.StateFailed),
			wantDelay: 2 * time.Second,
			wantCause: queue.ErrMustDelay.Error(),
		},
		{
			name:    "отсрочка больше потолка — ограничена BackoffMax",
			attempt: 1,
			handle: func(context.Context) error {
				return queue.MustDelay(time.Hour)
			},
			wantKind:  settleFailed,
			wantState: string(queue.StateFailed),
			wantDelay: backoffMax,
			wantCause: queue.ErrMustDelay.Error(),
		},
		{
			name:    "отсрочка на последней попытке — dead",
			attempt: maxAttempts,
			handle: func(context.Context) error {
				return queue.MustDelay(time.Second)
			},
			wantKind:  settleDead,
			wantState: string(queue.StateDead),
			wantCause: queue.ErrMustDelay.Error(),
		},
		{
			name:      "паника обработчика — сбой попытки, движок жив",
			attempt:   1,
			handle:    func(context.Context) error { panic("внезапная паника") },
			wantKind:  settleFailed,
			wantState: string(queue.StateFailed),
			wantCause: "паника в обработчике",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			repo.attempts = tc.attempt
			q := New(repo, discardLogger(), Config{Workers: 1, WorkerID: "test-worker"})

			k := kind{
				handler: handlerFunc(func(ctx context.Context, _ queue.Claim) error { return tc.handle(ctx) }),
				opts: queue.KindOptions{
					MaxAttempts:         maxAttempts,
					AttemptTimeout:      tc.attemptTimeout,
					MaxExecutionTimeout: tc.maxExecution,
					BackoffBase:         backoffBase,
					BackoffMax:          backoffMax,
				},
			}
			ctx := context.Background()
			if tc.baseCtx != nil {
				ctx = tc.baseCtx(t)
			}
			c := queue.Claim{ID: "job-1", Topic: "тема", Attempt: tc.attempt}

			before := time.Now()
			// execute не должен паниковать: паника обработчика — исход
			// сбоя, а не падение движка.
			o := q.execute(ctx, c, k)
			if o.kind != tc.wantKind {
				t.Fatalf("вид исхода = %d, ожидался %d", o.kind, tc.wantKind)
			}

			q.settle(ctx, c, o)
			got := repo.snapshot()

			if got.state != tc.wantState {
				t.Fatalf("состояние строки = %q, ожидалось %q", got.state, tc.wantState)
			}
			if tc.wantCause != "" && !strings.Contains(got.cause, tc.wantCause) {
				t.Fatalf("last_error = %q, ожидалась подстрока %q", got.cause, tc.wantCause)
			}

			wantAttempts := tc.attempt
			switch tc.wantKind {
			case settleNone:
				// Ничего не пишется: ни транзакции, ни записи.
				if got.begins != 0 || got.writes != 0 {
					t.Fatalf("settleNone обязан не писать: транзакций %d, записей %d", got.begins, got.writes)
				}
				if got.cause != "" {
					t.Fatalf("settleNone не должен оставлять last_error: %q", got.cause)
				}
			case settleFailedPreserving:
				// Попытка возвращается: счётчик не растёт.
				wantAttempts--
			}
			if got.writes != 0 && got.attempts != wantAttempts {
				t.Fatalf("попыток после записи %d, ожидалось %d", got.attempts, wantAttempts)
			}

			switch tc.wantKind {
			case settleFailed, settleFailedPreserving:
				// Оба исхода возвращают задание в оборот: у них есть момент
				// следующей попытки (у preserving он посчитан по номеру попытки).
				if got.availableAt.IsZero() {
					t.Fatalf("исход %d обязан нести момент следующей попытки", tc.wantKind)
				}
				if tc.wantDelay > 0 {
					want := before.Add(tc.wantDelay)
					if got.availableAt.Before(want.Add(-window)) || got.availableAt.After(want.Add(window)) {
						t.Fatalf("available_at = %s, ожидалось около %s (now + %s)",
							got.availableAt, want, tc.wantDelay)
					}
				}
				if got.availableAt.After(before.Add(backoffMax + window)) {
					t.Fatalf("available_at = %s выходит за потолок отсрочки %s",
						got.availableAt, backoffMax)
				}
			default:
				if !got.availableAt.IsZero() {
					t.Fatalf("исход %d не назначает available_at, получено %s", tc.wantKind, got.availableAt)
				}
			}
		})
	}
}

// TestSettlePanicDoesNotStopWorker — паника обработчика перехватывается:
// попытка получает исход сбоя, следующий цикл поллинга продолжает работу
// (retry/spec.md «Паника обработчика не уничтожает воркер»).
func TestSettlePanicDoesNotStopWorker(t *testing.T) {
	repo := newFakeRepo()
	repo.nextClaim = func(call int) []queue.Claim {
		return []queue.Claim{{ID: fmt.Sprintf("job-%d", call), Topic: "тема", Attempt: call}}
	}

	var calls atomic.Int32
	handler := handlerFunc(func(_ context.Context, _ queue.Claim) error {
		if calls.Add(1) == 1 {
			panic("внезапная паника")
		}
		return nil
	})

	q := New(repo, discardLogger(), Config{
		Workers:      1,
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
	defer func() { _ = q.Close() }()

	waitFor(t, "задание дошло до done после паники", 2*time.Second, func() bool {
		return repo.snapshot().state == string(queue.StateDone)
	})
	q.Stop()

	// Движок продолжает поллинг, пока его не остановили, поэтому важен не
	// точный счёт вызовов, а факт: первая попытка паниковала и получила
	// исход сбоя, а вторая дошла до done — воркер выжил.
	if got := calls.Load(); got < 2 {
		t.Fatalf("обработчик вызван %d раз, ожидалось не меньше 2 (паника, затем успех)", got)
	}
	got := repo.snapshot()
	if got.state != string(queue.StateDone) {
		t.Fatalf("состояние строки = %q, ожидалось %q", got.state, queue.StateDone)
	}
	if got.writes < 2 {
		t.Fatalf("зафиксированных исходов %d, ожидалось не меньше 2 (сбой от паники и успех)", got.writes)
	}
}

// TestSettleOnSucceededInSameTransaction — OnSucceeded вызывается в той же
// транзакции и до Commit: следующее задание фиксируется вместе с
// закрытием текущего (D9). Ошибка колбэка откатывает оба изменения.
func TestSettleOnSucceededInSameTransaction(t *testing.T) {
	tests := []struct {
		name string
		// o — исход попытки.
		o outcome
		// onSucceededErr — ошибка постановки следующего задания.
		onSucceededErr error
		// zeroRows — guard не пропустил запись исхода.
		zeroRows bool
		// wantCalled — вызывался ли колбэк.
		wantCalled bool
		// wantCommitted — зафиксировалась ли транзакция.
		wantCommitted bool
		// wantState — состояние строки после исхода.
		wantState string
	}{
		{
			name:          "успех — колбэк в той же транзакции до Commit",
			o:             outcome{kind: settleDone},
			wantCalled:    true,
			wantCommitted: true,
			wantState:     string(queue.StateDone),
		},
		{
			name:           "ошибка постановки следующего — текущее не закрывается",
			o:              outcome{kind: settleDone},
			onSucceededErr: errors.New("постановка следующего не удалась"),
			wantCalled:     true,
			wantCommitted:  false,
			wantState:      string(queue.StateProcessing),
		},
		{
			name:          "сбой попытки — колбэк не вызывается",
			o:             outcome{kind: settleFailed, cause: "сбой", availableAt: time.Now().Add(time.Second)},
			wantCalled:    false,
			wantCommitted: true,
			wantState:     string(queue.StateFailed),
		},
		{
			name:          "отмена — колбэк не вызывается",
			o:             outcome{kind: settleCanceled, cause: "отмена"},
			wantCalled:    false,
			wantCommitted: true,
			wantState:     string(queue.StateCanceled),
		},
		{
			name:          "guard не пропустил — колбэк не вызывается и ничего не фиксируется",
			o:             outcome{kind: settleDone},
			zeroRows:      true,
			wantCalled:    false,
			wantCommitted: false,
			wantState:     string(queue.StateProcessing),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			repo.zeroRows = tc.zeroRows
			q := New(repo, discardLogger(), Config{Workers: 1, WorkerID: "test-worker"})

			var (
				called                  bool
				callbackTx              queue.Tx
				committedDuringCallback bool
			)
			c := queue.Claim{
				ID:      "job-1",
				Topic:   "тема",
				Attempt: 1,
				OnSucceeded: func(_ context.Context, tx queue.Tx) error {
					called = true
					callbackTx = tx
					if ft, ok := tx.(*fakeTx); ok {
						committedDuringCallback = ft.committed
					}
					return tc.onSucceededErr
				},
			}

			q.settle(context.Background(), c, tc.o)

			got := repo.snapshot()
			if called != tc.wantCalled {
				t.Fatalf("OnSucceeded вызван: %v, ожидалось %v", called, tc.wantCalled)
			}
			if !tc.wantCalled {
				if got.state != tc.wantState {
					t.Fatalf("состояние строки = %q, ожидалось %q", got.state, tc.wantState)
				}
				return
			}

			// Колбэк получил ту же транзакцию, которой пишется исход, и
			// она ещё не была зафиксирована.
			if tx := repo.txUsed(); tx == nil || callbackTx != queue.Tx(tx) {
				t.Fatalf("OnSucceeded получил не ту транзакцию, которой пишется исход")
			}

			if committedDuringCallback {
				t.Fatalf("OnSucceeded вызван после Commit: следующее задание не в одной транзакции с текущим")
			}

			if gotCommitted := repo.txUsed().committed; gotCommitted != tc.wantCommitted {
				t.Fatalf("транзакция зафиксирована: %v, ожидалось %v", gotCommitted, tc.wantCommitted)
			}
			if got.state != tc.wantState {
				t.Fatalf("состояние строки = %q, ожидалось %q", got.state, tc.wantState)
			}
			if tc.wantCommitted && got.writes != 1 {
				t.Fatalf("зафиксированных записей %d, ожидалась 1", got.writes)
			}
			if !tc.wantCommitted && got.writes != 0 {
				t.Fatalf("после отката записей быть не должно, получено %d", got.writes)
			}
		})
	}
}

// TestSettlePassesLeaseWorker — движок передаёт в порт владельца аренды
// (c.Lease.Worker), а не идентификатор воркера из настроек движка. Guard
// Mark* в SQL сравнивает locked_by = worker (D13): подстановка «своего»
// воркера вместо арендовавшего означала бы запись исхода чужого задания —
// и ровно ноль строк в ответ на законный исход.
func TestSettlePassesLeaseWorker(t *testing.T) {
	const (
		// configWorker — идентификатор воркера из настроек движка: именно
		// его нельзя передавать в порт.
		configWorker = "worker-из-настроек"
		// leaseWorker — воркер, держащий аренду взятого задания.
		leaseWorker = "worker-держащий-аренду"
	)

	tests := []struct {
		name string
		// o — исход попытки.
		o outcome
		// wantKind — ожидаемый вид записи в фейке.
		wantKind string
		// wantWorker — ожидаемый владелец аренды в записи.
		wantWorker string
		// wantNoWorker — запись не несёт владельца: отменяет не
		// владелец аренды (markCanceledSQL идёт без locked_by).
		wantNoWorker bool
	}{
		{
			name:       "успех — владелец аренды из задания",
			o:          outcome{kind: settleDone},
			wantKind:   "done",
			wantWorker: leaseWorker,
		},
		{
			name:       "сбой с повтором — владелец аренды из задания",
			o:          outcome{kind: settleFailed, cause: "сбой", availableAt: time.Now().Add(time.Second)},
			wantKind:   "failed",
			wantWorker: leaseWorker,
		},
		{
			name:       "сбой без расхода попытки — владелец аренды из задания",
			o:          outcome{kind: settleFailedPreserving, cause: "сбой", availableAt: time.Now().Add(time.Second)},
			wantKind:   "failedPreserving",
			wantWorker: leaseWorker,
		},
		{
			name:       "смерть — владелец аренды из задания",
			o:          outcome{kind: settleDead, cause: "попытки исчерпаны"},
			wantKind:   "dead",
			wantWorker: leaseWorker,
		},
		{
			name:         "отмена — владения не требует",
			o:            outcome{kind: settleCanceled},
			wantKind:     "canceled",
			wantNoWorker: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			q := New(repo, discardLogger(), Config{Workers: 1, WorkerID: configWorker})

			// Владелец аренды намеренно отличается от воркера движка:
			// иначе подмена одного другим осталась бы незамеченной.
			c := queue.Claim{
				ID:      "job-1",
				Topic:   "тема",
				Attempt: 1,
				Lease:   queue.Lease{Worker: leaseWorker, Until: time.Now().Add(time.Minute)},
			}

			q.settle(context.Background(), c, tc.o)

			ops := repo.lastOps()
			if len(ops) != 1 {
				t.Fatalf("записей исхода %d, ожидалась 1", len(ops))
			}
			if ops[0].kind != tc.wantKind {
				t.Fatalf("вид записи = %q, ожидался %q", ops[0].kind, tc.wantKind)
			}
			if ops[0].id != c.ID {
				t.Fatalf("запись исхода адресована заданию %q, ожидалось %q", ops[0].id, c.ID)
			}

			if tc.wantNoWorker {
				if ops[0].worker != "" {
					t.Fatalf("запись %q передала владельца %q, а не требует владения",
						ops[0].kind, ops[0].worker)
				}
				return
			}
			if ops[0].worker != tc.wantWorker {
				t.Fatalf("запись %q передала владельца %q, ожидался владелец аренды %q",
					ops[0].kind, ops[0].worker, tc.wantWorker)
			}
			if ops[0].worker == configWorker {
				t.Fatalf("в порт ушёл воркер из настроек движка вместо владельца аренды")
			}
		})
	}
}

// TestSettleChainEnqueuesNextJob — цепочка заданий (D9, retry/spec.md
// «Цепочка заданий не рвётся», «Сбой транзакции закрытия»): обработчик
// задаёт следующее задание через Claim.Chain, движок ставит его в той же
// транзакции, которой закрывает текущее, — до Commit. Ошибка постановки
// откатывает оба изменения, а без цепочки постановка не вызывается вовсе.
func TestSettleChainEnqueuesNextJob(t *testing.T) {
	const topic = queue.Topic("тема")

	// next — задание, которое обработчик ставит следом.
	next := queue.Job{
		Topic:          topic,
		Payload:        queue.Payload(`{"шаг":2}`),
		IdempotencyKey: "шаг-2",
	}

	tests := []struct {
		name string
		// chain — обработчик задаёт следующее задание.
		chain bool
		// enqueueErr — ошибка постановки следующего задания.
		enqueueErr error
		// wantEnqueueCalls — сколько раз движок позвал EnqueueTx.
		wantEnqueueCalls int
		// wantState — состояние текущего задания после исхода.
		wantState string
		// wantWrites — сколько записей исхода зафиксировано.
		wantWrites int
		// wantEnqueued — сколько заданий видно в очереди.
		wantEnqueued int
	}{
		{
			name:             "цепочка — следующее задание в одной транзакции с текущим",
			chain:            true,
			wantEnqueueCalls: 1,
			wantState:        string(queue.StateDone),
			wantWrites:       1,
			wantEnqueued:     1,
		},
		{
			name:             "ошибка постановки следующего — текущее не закрыто",
			chain:            true,
			enqueueErr:       errors.New("постановка следующего не удалась"),
			wantEnqueueCalls: 1,
			wantState:        string(queue.StateProcessing),
			wantWrites:       0,
			wantEnqueued:     0,
		},
		{
			name:             "без цепочки — постановка не вызывается",
			chain:            false,
			wantEnqueueCalls: 0,
			wantState:        string(queue.StateDone),
			wantWrites:       1,
			wantEnqueued:     0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := newFakeRepo()
			repo.enqueueTxErr = tc.enqueueErr
			q := newUnitEngine(repo, Config{})

			k := kind{
				handler: handlerFunc(func(_ context.Context, c queue.Claim) error {
					if tc.chain {
						c.Chain(next)
					}
					return nil
				}),
				opts: queue.KindOptions{MaxAttempts: 1},
			}
			c := queue.Claim{
				ID:      "job-1",
				Topic:   topic,
				Attempt: 1,
				Lease:   queue.Lease{Worker: "test-worker", Until: time.Now().Add(time.Minute)},
			}

			// Попытка целиком: движок сам готовит цепочку (WithChain) и
			// OnSucceeded — проверяется боевой путь, а не ручная сборка
			// Claim.
			q.runAttempt(context.Background(), c, k)

			calls, jobs, beforeCommit := repo.enqueueTxSnapshot()
			if calls != tc.wantEnqueueCalls {
				t.Fatalf("EnqueueTx вызван %d раз, ожидалось %d", calls, tc.wantEnqueueCalls)
			}

			got := repo.snapshot()
			if got.state != tc.wantState {
				t.Fatalf("состояние строки = %q, ожидалось %q", got.state, tc.wantState)
			}
			if got.writes != tc.wantWrites {
				t.Fatalf("зафиксированных записей %d, ожидалось %d", got.writes, tc.wantWrites)
			}
			if enqueued := repo.enqueuedJobs(); len(enqueued) != tc.wantEnqueued {
				t.Fatalf("в очереди заданий %d, ожидалось %d", len(enqueued), tc.wantEnqueued)
			}

			if calls == 0 {
				return
			}

			// Постановка обязана идти до фиксации: иначе следующее задание
			// окажется не в одной транзакции с закрытием текущего.
			for i, before := range beforeCommit {
				if !before {
					t.Fatalf("EnqueueTx №%d вызван после Commit: следующее задание не в одной транзакции с текущим", i+1)
				}
			}
			if !reflect.DeepEqual(jobs[0], next) {
				t.Fatalf("в постановку ушло задание %+v, ожидалось %+v", jobs[0], next)
			}

			if tc.enqueueErr != nil {
				// Откат: ни текущее не закрыто, ни следующее не поставлено.
				return
			}
			if enqueued := repo.enqueuedJobs(); !reflect.DeepEqual(enqueued[0], next) {
				t.Fatalf("в очередь попало задание %+v, ожидалось %+v", enqueued[0], next)
			}
		})
	}
}
