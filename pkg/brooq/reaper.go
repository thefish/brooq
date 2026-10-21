// Возврат истёкших аренд (задача 4.5, D8): одна горутина на движок, а не
// на тему — SQL сборщика по теме не фильтрует. SQL здесь нет:
// оператор живёт в pkg/postgres, движок лишь открывает транзакцию
// и пишет журнал.
package brooq

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

const (
	// reaperMinPeriod — нижняя граница периода сборщика: на короткой
	// аренде Lease/4 слишком мал и превратил бы возврат в постоянный
	// запрос по индексу (D8).
	reaperMinPeriod = 5 * time.Second
	// reaperLeaseDivisor — доля аренды: возврат происходит заметно
	// чаще, чем истекает аренда (D8).
	reaperLeaseDivisor = 4
)

// reaperPeriod — период сборщика по D8: max(5s, Lease/4). Явно заданный
// Config.ReaperPeriod (> 0) переопределяет расчёт.
func (q *Queue) reaperPeriod() time.Duration {
	if q.cfg.ReaperPeriod > 0 {
		return q.cfg.ReaperPeriod
	}
	period := q.cfg.Lease / reaperLeaseDivisor
	if period < reaperMinPeriod {
		period = reaperMinPeriod
	}
	return period
}

// runReaper — горутина сборщика: на каждый тик возвращает истёкшие
// аренды. Выход по контексту движка, как у воркеров, а не по таймеру:
// отмена контекста гасит горутину немедленно. Ошибка одного тика не
// убивает сборщик — следующий тик наступит по таймеру.
func (q *Queue) runReaper(ctx context.Context) {
	period := q.reaperPeriod()
	q.log.Debug("сборщик истёкших аренд запущен", "period", period)

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		reaped, err := q.reapOnce(ctx)
		if err != nil {
			q.log.Error("возврат истёкших аренд не удался", "error", err)
			continue
		}
		if len(reaped) == 0 {
			continue
		}
		for _, job := range reaped {
			q.incMetric(metricLeaseExpiredTotal, string(job.Topic))
		}
		q.log.Info("истёкшие аренды возвращены в оборот",
			"count", len(reaped), "topics", reapedTopics(reaped))
	}
}

// reapOnce — один тик сборщика: возврат истёкших аренд в транзакции,
// фиксация здесь же. Откат после фиксации не аварийный
// (см. queue.Tx), поэтому отложен безусловно.
func (q *Queue) reapOnce(ctx context.Context) ([]queue.ReapedJob, error) {
	tx, err := q.repo.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("открытие транзакции сборщика: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	reaped, err := q.repo.ReapExpired(ctx, tx)
	if err != nil {
		return nil, err
	}
	if len(reaped) == 0 {
		// Пустой возврат — не ошибка: коммитить нечего.
		return nil, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("фиксация возврата истёкших аренд: %w", err)
	}
	return reaped, nil
}

// reapedTopics собирает темы возвращённых заданий для журнала: тема —
// единственное, что осмысленно показать в одной записи лога.
func reapedTopics(reaped []queue.ReapedJob) []string {
	topics := make([]string, 0, len(reaped))
	for _, job := range reaped {
		topics = append(topics, string(job.Topic))
	}
	return topics
}
