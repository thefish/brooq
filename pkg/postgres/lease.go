// Продление аренды (задача 3.6, D13): пока обработчик жив, воркер
// периодически отодвигает locked_until. Продление идёт в транзакции
// вызывающего и защищено guard-ом владельца.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// renewLeaseSQL — продление аренды по D13 дословно.
//
//   - guard locked_by = $2 обязателен: без него воркер, застрявший на
//     GC-паузе или блипе сети, проснётся после истечения аренды и
//     продлит её чужому заданию — два обработчика одновременно сочтут
//     себя владельцами. С guard-ом такого окна нет: возврат 0 строк
//     означает «аренда не моя»;
//   - guard state = 'processing' отсекает задание, уже переведённое
//     отменяющим в canceled (D14): отмена в полёте проходит именно так;
//   - updated_at обновляется вместе с арендой: иначе задание в работе
//     выглядит для janitor-подобных задач как старое;
//   - бьёт в PK (WHERE id = $1), обновляет одну строку.
const renewLeaseSQL = `
UPDATE job
SET locked_until = now() + $3::interval,
    updated_at   = now()
WHERE id = $1 AND state = 'processing' AND locked_by = $2
RETURNING id`

// RenewLease продлевает аренду задания id на срок lease, если задание
// всё ещё в processing и аренду держит тот же воркер worker. Возвращает
// число изменённых строк: 0 — «аренда не моя или задание уже не в
// processing», и это не ошибка, а сигнал о потере аренды (D14) — его
// разбирает вызывающий. Запись идёт в транзакции вызывающего:
// фиксирует её он же.
func (r *Repository) RenewLease(ctx context.Context, tx queue.Tx, id, worker string, lease time.Duration) (int64, error) {
	n, err := execRows(ctx, tx, renewLeaseSQL, id, worker, lease)
	if err != nil {
		return 0, fmt.Errorf("продление аренды задания %s: %w", id, err)
	}
	return n, nil
}
