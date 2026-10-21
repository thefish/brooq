// Запись исхода попытки: одна транзакция на исход, следующее задание
// ставится в той же транзакции (D9, OnSucceeded).
package brooq

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// settleKind — как записать исход попытки.
type settleKind int

const (
	// settleNone — исход не пишется: движок останавливается, задание
	// остаётся в processing до истечения аренды.
	settleNone settleKind = iota
	// settleDone — успех: done безусловно.
	settleDone
	// settleFailed — сбой с повтором: failed и момент availableAt.
	settleFailed
	// settleFailedPreserving — сбой без расхода попытки.
	settleFailedPreserving
	// settleDead — попытки исчерпаны: терминальный исход.
	settleDead
	// settleCanceled — отмена: терминальный исход.
	settleCanceled
)

// outcome — решение об исходе попытки, выведенное из причины контекста и
// ответа обработчика.
type outcome struct {
	// kind — какой исход писать.
	kind settleKind
	// cause — текст для last_error.
	cause string
	// availableAt — момент следующей попытки (для failed).
	availableAt time.Time
}

// settle записывает исход попытки в транзакции: Mark* и, для успеха,
// OnSucceeded до Commit — следующее задание фиксируется вместе с
// закрытием текущего. Если строка уже не processing (терминальное
// задание или потерянная аренда), исход не пишется и OnSucceeded не
// вызывается.
func (q *Queue) settle(ctx context.Context, c queue.Claim, o outcome) {
	if o.kind == settleNone {
		return
	}

	// Запись исхода не должна срываться отменой контекста попытки: причина
	// уже разобрана, а исход обязан быть записан. Таймаут страхует
	// остановку движка от зависшей базы.
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
	defer cancel()

	tx, err := q.repo.Begin(settleCtx)
	if err != nil {
		q.log.Error("запись исхода: транзакция не открыта",
			"topic", c.Topic, "job", c.ID, "attempt", c.Attempt, "error", err)
		return
	}
	// Откат после фиксации не аварийный (см. queue.Tx).
	defer func() { _ = tx.Rollback(settleCtx) }()

	// worker — владелец аренды: guard Mark* требует locked_by = worker,
	// чтобы воркер, потерявший аренду, не дописал исход чужого задания.
	worker := c.Lease.Worker

	var n int64
	switch o.kind {
	case settleDone:
		n, err = q.repo.MarkDone(settleCtx, tx, c.ID, worker)
	case settleFailed:
		n, err = q.repo.MarkFailed(settleCtx, tx, c.ID, worker, o.cause, o.availableAt)
	case settleFailedPreserving:
		n, err = q.repo.MarkFailedPreservingAttempt(settleCtx, tx, c.ID, worker, o.cause, o.availableAt)
	case settleDead:
		n, err = q.repo.MarkDead(settleCtx, tx, c.ID, worker, o.cause)
	case settleCanceled:
		n, err = q.repo.MarkCanceled(settleCtx, tx, c.ID)
	default:
		err = fmt.Errorf("неизвестный исход попытки: %d", o.kind)
	}
	if err != nil {
		q.log.Error("запись исхода не удалась",
			"topic", c.Topic, "job", c.ID, "attempt", c.Attempt,
			"outcome", o.kind, "error", err)
		return
	}
	if n == 0 {
		// Guard не пропустил: строка уже терминальная (например,
		// отменена снаружи) или аренда уже не у этого воркера — исход
		// писать некому.
		q.log.Warn("исход не записан: задание уже не processing или аренда потеряна",
			"topic", c.Topic, "job", c.ID, "attempt", c.Attempt, "outcome", o.kind)
		return
	}

	if o.kind == settleDone && c.OnSucceeded != nil {
		if err := c.OnSucceeded(settleCtx, tx); err != nil {
			// Откат: текущее не считается закрытым, следующее задание
			// не поставлено — оба изменения отменяются вместе.
			q.log.Error("следующее задание не поставлено, текущее не закрыто",
				"topic", c.Topic, "job", c.ID, "error", err)
			return
		}
	}

	if err := tx.Commit(settleCtx); err != nil {
		q.log.Error("фиксация исхода не удалась",
			"topic", c.Topic, "job", c.ID, "attempt", c.Attempt, "error", err)
		return
	}

	// Исход записан — только теперь он считается состоявшимся (D11).
	q.incOutcomeMetric(metricSettleTotal, c.Topic, settleOutcomeLabel(o.kind))
}

// Исходы записи для лейбла outcome метрики brooq_settle_total (D11).
const (
	// settleOutcomeDone — успех.
	settleOutcomeDone = "done"
	// settleOutcomeRetry — сбой с повтором (включая случай без
	// расхода попытки: исход всё равно retry).
	settleOutcomeRetry = "retry"
	// settleOutcomeDead — попытки исчерпаны.
	settleOutcomeDead = "dead"
	// settleOutcomeCanceled — отмена.
	settleOutcomeCanceled = "canceled"
)

// settleOutcomeLabel переводит вид исхода в значение лейбла outcome
// метрики brooq_settle_total (D11): четыре значения без пятого —
// неписанный исход (settleNone) сюда не доходит.
func settleOutcomeLabel(k settleKind) string {
	switch k {
	case settleDone:
		return settleOutcomeDone
	case settleFailed, settleFailedPreserving:
		return settleOutcomeRetry
	case settleDead:
		return settleOutcomeDead
	case settleCanceled:
		return settleOutcomeCanceled
	default:
		return "unknown"
	}
}
