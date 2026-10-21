// Цикл поллинга (D7) и исполнение попытки (D15): воркер берёт не больше
// числа свободных мест, исполняет попытку и записывает исход в
// транзакции. Решение «повтор или терминальный исход» принимается здесь
// по context.Cause и по сигналам обработчика.
package brooq

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2" //nolint:gosec // джиттер не криптографический: G404 здесь не применим
	"runtime/debug"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

const (
	// backoffFactor — множитель роста отсрочки (D6).
	backoffFactor = 2.0
	// settleTimeout — сколько отводится на запись исхода. Запись идёт
	// вне контекста попытки (он уже отменён), но не бесконечно: иначе
	// остановка движка ждала бы зависшую базу.
	settleTimeout = 10 * time.Second
)

// runWorker — одна горутина-слот темы: одна итерация за раз, между
// итерациями пауза с джиттером. Первый тик немедленный, поэтому старт
// движка не ждёт период (D7).
func (q *Queue) runWorker(ctx context.Context, topic queue.Topic, slot int, k kind) {
	q.log.Debug("воркер запущен", "topic", topic, "slot", slot, "worker", q.worker)

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		q.pollOnce(ctx, topic, slot, k)

		// Reset после работы: период = работа + пауза, иначе медленная
		// база заставляла бы цикл наезжать сам на себя (D7).
		timer.Reset(q.pollPeriod())
	}
}

// pollOnce — одна итерация: взять задания темы и исполнить их. Ошибка
// взятия не роняет воркер — следующая итерация наступит по таймеру.
func (q *Queue) pollOnce(ctx context.Context, topic queue.Topic, slot int, k kind) {
	claims, err := q.claim(ctx, topic, slot)
	if err != nil {
		q.log.Error("взятие заданий не удалось", "topic", topic, "error", err)
		return
	}
	if len(claims) == 0 {
		// Пустая очередь — исход взятия empty (D11): по этой паре
		// видно, что воркер жив и работает.
		q.incOutcomeMetric(metricClaimTotal, topic, claimOutcomeEmpty)
		return
	}
	for _, c := range claims {
		q.incOutcomeMetric(metricClaimTotal, topic, claimOutcomeClaimed)
		q.runAttempt(ctx, c, k)
	}
}

// claim берёт задания темы от имени слота slot. Размер выборки — число
// свободных мест воркера: у горутины-слота место одно, поэтому limit = 1;
// брать больше значило бы держать аренду без исполнения (D4). Взятие —
// запись, поэтому идёт в транзакции и фиксируется до
// исполнения: аренда не должна ждать обработчика.
//
// В locked_by уходит идентификатор слота (workerID), а не процесса: иначе
// guard владения не изолирует слоты одного движка (D13).
func (q *Queue) claim(ctx context.Context, topic queue.Topic, slot int) ([]queue.Claim, error) {
	tx, err := q.repo.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("открытие транзакции взятия темы %s: %w", topic, err)
	}
	// Откат после фиксации не аварийный (см. queue.Tx), поэтому он
	// отложен безусловно.
	defer func() { _ = tx.Rollback(ctx) }()

	claims, err := q.repo.Claim(ctx, tx, topic, q.workerID(slot), q.cfg.Lease, 1)
	if err != nil {
		return nil, err
	}
	if len(claims) == 0 {
		// Пустая очередь — не ошибка: просто следующий тик.
		return nil, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("фиксация взятия темы %s: %w", topic, err)
	}
	return claims, nil
}

// runAttempt исполняет одну попытку и записывает её исход.
//
// Здесь же готовится цепочка (D9): движок подставляет Claim.Chain место
// хранения следующего задания, а OnSucceeded ставит его в транзакции
// закрытия текущего (settle вызывает OnSucceeded до Commit).
func (q *Queue) runAttempt(ctx context.Context, c queue.Claim, k kind) {
	var next *queue.Job
	c = c.WithChain(func(job queue.Job) { next = &job })
	c.OnSucceeded = func(ctx context.Context, tx queue.Tx) error {
		if next == nil {
			// Обработчик цепочку не задал: закрывать текущее нечем
			// дополнительно.
			return nil
		}
		if _, _, err := q.repo.EnqueueTx(ctx, tx, *next); err != nil {
			return err
		}
		return nil
	}

	// Занятость пула (D11): слот занят с начала попытки до записи
	// исхода.
	q.busyInc(c.Topic)
	defer q.busyDec(c.Topic)

	start := time.Now()
	o := q.execute(ctx, c, k)
	q.observeAttemptDuration(c.Topic, time.Since(start))
	q.settle(ctx, c, o)
}

// Исходы взятия для лейбла outcome метрики brooq_claim_total (D11).
const (
	// claimOutcomeClaimed — задание взято.
	claimOutcomeClaimed = "claimed"
	// claimOutcomeEmpty — готовых заданий не было.
	claimOutcomeEmpty = "empty"
)

// execute исполняет попытку под двумя предохранителями (D15):
// AttemptTimeout гасит контекст по времени попытки, MaxExecutionTimeout —
// безусловный потолок темы, даже если обработчик игнорирует контекст.
// Паника обработчика перехватывается и считается сбоем попытки, воркер
// остаётся жив.
//
// Рядом с обработчиком живут heartbeat (4.6, D13) и watchdog (4.7,
// D15): heartbeat продлевает аренду, пока не отменён контекст попытки,
// watchdog через грацию фиксирует, что горутина не вышла после отмены.
func (q *Queue) execute(ctx context.Context, c queue.Claim, k kind) outcome {
	attemptCtx, cancelAttempt := context.WithCancelCause(ctx)

	// Heartbeat гибнет по ctx задачи (D15 п.1): отмена гасит тикер
	// немедленно, не дожидаясь выхода зависшего обработчика. Ждём его
	// выхода здесь же, чтобы продление не пережило попытку.
	heartbeatDone := q.startHeartbeat(attemptCtx, cancelAttempt, c)
	defer func() {
		cancelAttempt(nil)
		<-heartbeatDone
	}()

	if k.opts.MaxExecutionTimeout > 0 {
		hardCeiling := time.AfterFunc(k.opts.MaxExecutionTimeout, func() {
			cancelAttempt(queue.ErrMaxExecutionExceeded)
		})
		defer hardCeiling.Stop()
	}

	callCtx := attemptCtx
	if k.opts.AttemptTimeout > 0 {
		var cancelTimeout context.CancelFunc
		callCtx, cancelTimeout = context.WithTimeout(attemptCtx, k.opts.AttemptTimeout)
		defer cancelTimeout()
	}

	// handlerDone закрывается по возврате обработчика: по нему watchdog
	// отличает вышедшую горутину от зависшей. Закрывается до выхода
	// execute, поэтому watchdog не переживёт попытку.
	handlerDone := make(chan struct{})
	defer close(handlerDone)
	q.watch(callCtx, handlerDone, c)

	err := q.callHandler(callCtx, c, k)
	return q.outcomeOf(callCtx, c, k, err)
}

// callHandler вызывает обработчик, перехватывая панику: паника — сбой
// попытки, а не падение процесса.
func (q *Queue) callHandler(ctx context.Context, c queue.Claim, k kind) (err error) {
	defer func() {
		if r := recover(); r != nil {
			q.log.Error("паника в обработчике",
				"topic", c.Topic, "job", c.ID, "attempt", c.Attempt,
				"panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("паника в обработчике: %v", r)
		}
	}()
	return k.handler.Handle(ctx, c)
}

// outcomeOf выводит исход попытки: сначала по причине прерывания
// контекста (D15), затем по сигналу обработчика, затем по обычной
// ошибке.
func (q *Queue) outcomeOf(callCtx context.Context, c queue.Claim, k kind, err error) outcome {
	abort, cause := queue.ClassifyCause(callCtx)

	// Плановая остановка движка — не исход задания: строку не трогаем,
	// задание вернёт Reaper после истечения аренды (D8).
	if errors.Is(cause, errQueueStopped) {
		return outcome{kind: settleNone}
	}

	switch abort {
	case queue.AbortRetryable:
		// Сбой попытки с повтором: истёк AttemptTimeout или жёсткий
		// потолок исполнения темы (D15). dead достигается только
		// исчерпанием попыток, а не самим
		// фактом потолка — это делает failure.
		return q.failure(c, k, causeText(cause))
	case queue.AbortTerminal:
		if errors.Is(cause, queue.ErrLeaseLost) {
			// Аренда потеряна (D14): отменяющий уже перевёл строку в
			// canceled, либо задание перехвачено другим воркером.
			// Запись воркера была бы перезаписью чужого решения — не
			// пишем ничего.
			return outcome{kind: settleNone}
		}
		// Отмена без явной причины: исполнять попытку дальше нельзя,
		// повтор не назначается.
		return outcome{kind: settleCanceled, cause: causeText(cause)}
	}

	if err == nil {
		return outcome{kind: settleDone}
	}

	switch {
	case errors.Is(err, queue.ErrMustCancelTask):
		return outcome{kind: settleCanceled, cause: err.Error()}
	case errors.Is(err, queue.ErrMustPreserveAttempt):
		// Попытка не расходуется: attempts вернёт MarkFailedPreservingAttempt.
		return outcome{
			kind:        settleFailedPreserving,
			cause:       err.Error(),
			availableAt: q.nextAttemptAt(c, k, 0),
		}
	case errors.Is(err, queue.ErrMustDelay):
		delay, _ := queue.DelayOf(err)
		return q.failureWithDelay(c, k, err.Error(), delay)
	default:
		return q.failure(c, k, err.Error())
	}
}

// failure — обычный сбой: dead при исчерпании попыток, иначе failed с
// backoff (D5).
func (q *Queue) failure(c queue.Claim, k kind, cause string) outcome {
	return q.failureWithDelay(c, k, cause, 0)
}

// failureWithDelay — сбой с отсрочкой: явный интервал обработчика (0 —
// посчитать backoff) ограничивается BackoffMax; на последней попытке
// задание становится dead (retry/spec.md).
func (q *Queue) failureWithDelay(c queue.Claim, k kind, cause string, delay time.Duration) outcome {
	if c.Attempt >= k.opts.MaxAttempts {
		return outcome{kind: settleDead, cause: cause}
	}
	return outcome{
		kind:        settleFailed,
		cause:       cause,
		availableAt: q.nextAttemptAt(c, k, delay),
	}
}

// nextAttemptAt считает момент следующей попытки: явный интервал
// обработчика, ограниченный BackoffMax, либо backoff по номеру попытки.
func (q *Queue) nextAttemptAt(c queue.Claim, k kind, delay time.Duration) time.Time {
	if delay <= 0 {
		delay = backoffDelay(k.opts.BackoffBase, k.opts.BackoffMax, backoffFactor, c.Attempt)
	} else if k.opts.BackoffMax > 0 && delay > k.opts.BackoffMax {
		delay = k.opts.BackoffMax
	}
	return time.Now().Add(delay)
}

// causeText — текст причины для last_error: без секретов, пустая
// причина — пустая строка.
func causeText(cause error) string {
	if cause == nil {
		return ""
	}
	return cause.Error()
}

// pollPeriod — период до следующего тика: PollInterval ±25 % (D7).
func (q *Queue) pollPeriod() time.Duration {
	base := q.cfg.PollInterval
	delta := base / 4
	if delta <= 0 {
		return base
	}
	// Отрезок [base-delta, base+delta] включительно.
	return base - delta + rand.N(2*delta+1) //nolint:gosec // джиттер периода опроса, не криптография: math/rand здесь уместен
}
