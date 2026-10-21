// Watchdog зависших обработчиков (задача 4.7, D15 п.3): после отмены
// контекста попытки даётся грация; если обработчик за неё не вышел — в
// лог пишется ERROR и растёт brooq_hung_workers_total{topic}.
package brooq

import (
	"context"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// watch отмеряет грацию после отмены контекста, который получил
// обработчик (callCtx), и фиксирует, что горутина не вышла: ERROR в лог
// с темой, заданием и длительностью плюс счётчик
// brooq_hung_workers_total{topic}.
//
// Горутину в Go убить снаружи нельзя — watchdog ничего не прерывает, он
// только фиксирует факт. Утечка горутины и её память остаются до
// перезапуска процесса: это признанная дыра (см. docs/architecture.md).
//
// Аренда при зависшем обработчике перестаёт продлеваться сама собой:
// тикер heartbeat привязан к ctx задачи и гаснет при отмене (D13, D15
// п.1), поэтому отдельного условия «перестать продлевать» здесь нет —
// Reaper вернёт задание в оборот после истечения Lease.
func (q *Queue) watch(callCtx context.Context, handlerDone <-chan struct{}, c queue.Claim) {
	grace := q.cfg.WatchdogGrace
	if grace <= 0 {
		// New уже подставил Lease вместо нуля; страховка на случай
		// движка, собранного в обход New.
		grace = q.cfg.Lease
	}
	if grace <= 0 {
		return
	}

	go func() {
		// Обработчик вышел раньше отмены — наблюдать не за чем.
		select {
		case <-handlerDone:
			return
		case <-callCtx.Done():
		}

		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-handlerDone:
			return
		case <-timer.C:
		}

		q.log.Error("обработчик не вышел после отмены контекста: горутина считается зависшей",
			"topic", c.Topic, "job", c.ID, "attempt", c.Attempt, "grace", grace)
		q.incMetric(metricHungWorkersTotal, string(c.Topic))
	}()
}
