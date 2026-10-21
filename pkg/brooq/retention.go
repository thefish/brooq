// Уборка терминальных заданий по TTL: одна горутина на движок, а не на
// тему — SQL уборки по теме не фильтрует. SQL здесь нет: оператор живёт
// в pkg/postgres (DeleteTerminal), движок открывает транзакцию, крутит
// пачки и пишет журнал с метрикой.
package brooq

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

const (
	// defaultRetentionBatch — размер пачки удаления, если в настройках
	// движка он не задан: одна пачка держит блокировки недолго, а цикл
	// добирает остаток.
	defaultRetentionBatch = 1000
	// retentionMinPeriod — нижняя граница расчётного периода уборки:
	// TTL измеряется днями, частить незачем.
	retentionMinPeriod = time.Hour
	// retentionMaxPeriod — верхняя граница расчётного периода: ждать
	// дольше суток после старта процесса незачем.
	retentionMaxPeriod = 24 * time.Hour
	// retentionDay — делитель TTL для расчёта периода уборки.
	retentionDay = 24 * time.Hour
	// retentionMaxBatchesPerTick — потолок пачек за один тик: при
	// огромном накоплении уборка не крутится бесконечно, остаток
	// добирается следующим тиком.
	retentionMaxBatchesPerTick = 100
)

// retentionPeriod — период уборки. Явно заданный Config.RetentionPeriod
// (> 0) переопределяет расчёт. По умолчанию — сутки, но не реже, чем
// раз в сутки, и не чаще, чем раз в час: TTL измеряется днями, поэтому
// частить незачем, а ждать сутки после старта процесса — долго.
func (q *Queue) retentionPeriod() time.Duration {
	if q.cfg.RetentionPeriod > 0 {
		return q.cfg.RetentionPeriod
	}
	period := q.cfg.RetentionTTL / retentionDay
	if period < retentionMinPeriod {
		period = retentionMinPeriod
	}
	if period > retentionMaxPeriod {
		period = retentionMaxPeriod
	}
	return period
}

// runRetention — горутина уборки терминальных заданий: на каждый тик
// удаляет пачки строк, пролежавших терминальными дольше TTL. Выход по
// контексту движка, как у воркеров. Ошибка одного тика не убивает
// горутину — следующий тик наступит по таймеру.
func (q *Queue) runRetention(ctx context.Context) {
	if q.cfg.RetentionTTL <= 0 {
		// Уборка выключена: в Start горутина и не запускается, но
		// прямой вызов не должен крутить пустой таймер.
		return
	}

	period := q.retentionPeriod()
	q.log.Debug("уборка терминальных заданий запущена",
		"period", period, "ttl", q.cfg.RetentionTTL, "batch", q.cfg.RetentionBatch)

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if err := q.reapTerminal(ctx); err != nil {
			q.log.Error("уборка терминальных заданий не удалась", "error", err)
		}
	}
}

// reapTerminal — один тик уборки: цикл пачек, пока DeleteTerminal
// возвращает ровно RetentionBatch строк (значит есть ещё). Число пачек
// за тик ограничено retentionMaxBatchesPerTick: при огромном накоплении
// тик заканчивается предупреждением, а остаток добирается следующим
// тиком — иначе одна уборка заняла бы неограниченное время.
func (q *Queue) reapTerminal(ctx context.Context) error {
	batch := q.cfg.RetentionBatch
	if batch < 1 {
		batch = defaultRetentionBatch
	}

	total := 0
	for i := 0; i < retentionMaxBatchesPerTick; i++ {
		deleted, err := q.deleteTerminalBatch(ctx, batch)
		if err != nil {
			if total > 0 {
				// Удалённое уже зафиксировано: журнал не должен
				// потерять этот кусок.
				q.log.Info("терминальные задания убраны частично",
					"count", total, "ttl", q.cfg.RetentionTTL)
			}
			return err
		}
		total += len(deleted)
		for _, job := range deleted {
			q.incMetric(metricRetentionDeletedTotal, string(job.Topic))
		}
		if len(deleted) < batch {
			// Пачка неполная (в том числе пустая) — убирать больше
			// нечего до следующего тика. Пустая пачка не ошибка.
			break
		}
	}

	if total > 0 {
		q.log.Info("терминальные задания убраны", "count", total, "ttl", q.cfg.RetentionTTL)
	}
	if total >= retentionMaxBatchesPerTick*batch {
		q.log.Warn("уборка упёрлась в лимит пачек за тик: осталось на следующий тик",
			"limit", retentionMaxBatchesPerTick, "batch", batch)
	}
	return nil
}

// deleteTerminalBatch — одна пачка уборки в собственной транзакции:
// фиксация здесь же, чтобы пачка не держала блокировки до конца тика.
// Откат после фиксации не аварийный (см. queue.Tx), поэтому отложен
// безусловно.
func (q *Queue) deleteTerminalBatch(ctx context.Context, batch int) ([]queue.DeletedJob, error) {
	tx, err := q.repo.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("открытие транзакции уборки: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	deleted, err := q.repo.DeleteTerminal(ctx, tx, q.cfg.RetentionTTL, batch)
	if err != nil {
		return nil, err
	}
	if len(deleted) == 0 {
		// Пустая пачка — не ошибка: коммитить нечего.
		return nil, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("фиксация уборки терминальных заданий: %w", err)
	}
	return deleted, nil
}
