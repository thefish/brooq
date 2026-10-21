// Heartbeat (задача 4.6, D13): горутина рядом с обработчиком продлевает
// аренду каждые Lease/3, пока живёт контекст попытки. Тикер привязан к
// ctx задачи, а не к выходу обработчика (D15): зависший обработчик не
// продлевает аренду вечно, а после отмены аренда перестаёт продлеваться
// немедленно.
package brooq

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// startHeartbeat запускает продление аренды задания c, пока не отменён
// ctx (контекст попытки). Возвращает канал, закрывающийся при выходе
// горутины: вызывающий обязан отменить ctx и дождаться его, иначе
// продление переживёт попытку.
//
// Два разных нуля не смешиваются (D14):
//
//   - 0 строк от продления — аренда потеряна (отмена снаружи или
//     перехват воркером): ctx гасится причиной ErrLeaseLost, счётчик
//     brooq_lease_lost_total растёт, продление прекращается;
//   - ошибка запроса — сбой базы, а не потеря аренды: она пишется в лог
//     и продление продолжается — блип сети не должен убивать долгую
//     задачу.
func (q *Queue) startHeartbeat(ctx context.Context, cancelAttempt context.CancelCauseFunc, c queue.Claim) <-chan struct{} {
	period := q.cfg.Lease / 3
	if period <= 0 {
		// Страховка от нуля: New уже гарантирует Lease > 0.
		period = time.Second
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(period)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Отмена гасит тикер немедленно, не дожидаясь выхода
				// зависшего обработчика (D15 п.1).
				return
			case <-ticker.C:
			}

			n, err := q.renewLease(ctx, c)
			if err != nil {
				q.log.Warn("продление аренды не удалось",
					"topic", c.Topic, "job", c.ID, "error", err)
				continue
			}
			if n == 0 {
				q.log.Warn("аренда потеряна: продление не прошло guard",
					"topic", c.Topic, "job", c.ID)
				q.incMetric(metricLeaseLostTotal, string(c.Topic))
				cancelAttempt(queue.ErrLeaseLost)
				return
			}
		}
	}()
	return done
}

// renewLease продлевает аренду в отдельной транзакции и фиксирует её
// сразу: незафиксированное продление аренду не отодвинуло бы.
// Возвращает число изменённых строк из порта: 0 — потеря аренды, а не
// ошибка.
func (q *Queue) renewLease(ctx context.Context, c queue.Claim) (int64, error) {
	tx, err := q.repo.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("открытие транзакции продления: %w", err)
	}
	// Откат после фиксации не аварийный (см. queue.Tx).
	defer func() { _ = tx.Rollback(ctx) }()

	// worker — владелец аренды: guard RenewLease требует locked_by =
	// worker, поэтому берём его из взятого задания, а не из настроек.
	n, err := q.repo.RenewLease(ctx, tx, c.ID, c.Lease.Worker, q.cfg.Lease)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		// Ничего не изменено — фиксировать нечего.
		return 0, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("фиксация продления аренды: %w", err)
	}
	return n, nil
}
