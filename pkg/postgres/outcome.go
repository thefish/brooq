// Запись исхода попытки (задача 3.3, D5, D14): done, failed с отсрочкой,
// dead и canceled. Решение «повтор или dead» принимает Go (D5): SQL
// получает готовое состояние и, для failed, готовый интервал отсрочки.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// markDoneSQL — успех. Условие state = 'processing' AND locked_by = $2
// и есть защита инварианта 3: терминальное задание не перезаписывается, а
// исход пишется только тому заданию, аренда которого всё ещё у этого
// воркера (D13).
const markDoneSQL = `
UPDATE job
SET state      = 'done',
    last_error = NULL,
    updated_at = now()
WHERE id = $1 AND state = 'processing' AND locked_by = $2`

// markFailedSQL — сбой с повтором: задание ждёт available_at, который
// посчитан в Go (D6), а не в SQL.
const markFailedSQL = `
UPDATE job
SET state        = 'failed',
    last_error   = $2,
    available_at = $3,
    updated_at   = now()
WHERE id = $1 AND state = 'processing' AND locked_by = $4`

// markFailedPreservingAttemptSQL — сбой без расхода попытки (сигнал
// ErrMustPreserveAttempt): attempts уменьшается на единицу, потому что
// взятие уже списало попытку (D5), а сигнал просит вернуть её. Нижняя
// граница GREATEST(..., 0) страхует строку с attempts = 0 от минуса.
// Условие state = 'processing' AND locked_by = $4 то же, что у прочих
// исходов.
const markFailedPreservingAttemptSQL = `
UPDATE job
SET state        = 'failed',
    last_error   = $2,
    available_at = $3,
    attempts     = GREATEST(attempts - 1, 0),
    updated_at   = now()
WHERE id = $1 AND state = 'processing' AND locked_by = $4`

// markDeadSQL — попытки исчерпаны; терминальное состояние пишется
// только вместе с записанным сбоем (D5).
const markDeadSQL = `
UPDATE job
SET state      = 'dead',
    last_error = $2,
    updated_at = now()
WHERE id = $1 AND state = 'processing' AND locked_by = $3`

// markCanceledSQL — отмена по D14: включая задание в полёте. Терминальное
// задание под условие не попадает, поэтому отмена для него — no-op.
const markCanceledSQL = `
UPDATE job
SET state      = 'canceled',
    updated_at = now()
WHERE id = $1 AND state IN ('pending', 'failed', 'processing')`

// MarkDone записывает успех: done безусловно, в том числе на последней
// попытке (D5). Guard: строка всё ещё в processing И аренда всё ещё моя
// (locked_by = worker, D13). Возвращает число изменённых строк: 0 —
// задание уже терминальное или аренду держит другой воркер, повторной
// записи не произошло.
func (r *Repository) MarkDone(ctx context.Context, tx queue.Tx, id, worker string) (int64, error) {
	n, err := execRows(ctx, tx, markDoneSQL, id, worker)
	if err != nil {
		return 0, fmt.Errorf("запись успеха задания %s: %w", id, err)
	}
	return n, nil
}

// MarkFailed записывает сбой с повтором: состояние failed и готовый
// момент следующей попытки availableAt (backoff посчитан в Go, D6).
// cause попадает в last_error — без секретов. Guard: строка всё ещё в
// processing И аренда всё ещё моя (locked_by = worker, D13).
// Возвращает число изменённых строк: 0 — задание уже терминальное или
// аренду держит другой воркер.
func (r *Repository) MarkFailed(ctx context.Context, tx queue.Tx, id, worker, cause string, availableAt time.Time) (int64, error) {
	n, err := execRows(ctx, tx, markFailedSQL, id, cause, availableAt, worker)
	if err != nil {
		return 0, fmt.Errorf("запись сбоя задания %s: %w", id, err)
	}
	return n, nil
}

// MarkFailedPreservingAttempt записывает сбой так, что число попыток не
// меняется: взятие уже списало попытку, поэтому здесь она возвращается
// (attempts = attempts - 1). Задание уходит в failed и будет взято снова;
// состояние dead этим путём не достигается. Guard: строка всё ещё в
// processing И аренда всё ещё моя (locked_by = worker, D13).
// Возвращает число изменённых строк: 0 — задание уже терминальное или
// аренду держит другой воркер.
func (r *Repository) MarkFailedPreservingAttempt(ctx context.Context, tx queue.Tx, id, worker, cause string, availableAt time.Time) (int64, error) {
	n, err := execRows(ctx, tx, markFailedPreservingAttemptSQL, id, cause, availableAt, worker)
	if err != nil {
		return 0, fmt.Errorf("запись сбоя без расхода попытки задания %s: %w", id, err)
	}
	return n, nil
}

// MarkDead записывает сбой, исчерпавший попытки: задание становится
// мёртвым (терминальное). Guard: строка всё ещё в processing И аренда
// всё ещё моя (locked_by = worker, D13). Возвращает число изменённых
// строк: 0 — задание уже терминальное или аренду держит другой воркер.
func (r *Repository) MarkDead(ctx context.Context, tx queue.Tx, id, worker, cause string) (int64, error) {
	n, err := execRows(ctx, tx, markDeadSQL, id, cause, worker)
	if err != nil {
		return 0, fmt.Errorf("запись смерти задания %s: %w", id, err)
	}
	return n, nil
}

// MarkCanceled переводит задание в canceled, включая случай полёта
// (D14). Возвращает число изменённых строк: 0 — задание уже
// терминальное, отмена не состоялась.
func (r *Repository) MarkCanceled(ctx context.Context, tx queue.Tx, id string) (int64, error) {
	n, err := execRows(ctx, tx, markCanceledSQL, id)
	if err != nil {
		return 0, fmt.Errorf("отмена задания %s: %w", id, err)
	}
	return n, nil
}
