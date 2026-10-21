// Порт метрик (D11, D15): движку запрещено импортировать
// internal/observability, поэтому он видит только узкий
// интерфейс счётчиков и gauge'ей. Конкретную реализацию (реестр
// observability) подставляет сборка процесса; nil означает «не считать».
package brooq

import (
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// Metrics — узкий порт метрик движка.
type Metrics interface {
	// Inc увеличивает счётчик name на 1 с лейблами labels.
	Inc(name string, labels map[string]string)
	// Add увеличивает счётчик name на delta с лейблами labels. Дельта
	// нужна счётчикам числа попыток: пара
	// brooq_attempt_duration_seconds_{sum,count} публикуется
	// приращениями, как в Middleware реестра (D11).
	Add(name string, labels map[string]string, delta uint64)
	// AddFloat увеличивает счётчик name на дробную delta — сумма
	// длительностей попыток в секундах: доли секунды не теряются, иначе
	// brooq_attempt_duration_seconds_sum остаётся нулём для быстрых
	// заданий и средняя длительность (SLO) неверна.
	AddFloat(name string, labels map[string]string, delta float64)
	// Set устанавливает gauge name в абсолютное значение value. Значение
	// дробное: возраст старейшего задания измеряется секундами с долями.
	Set(name string, value float64, labels map[string]string)
}

// Имена метрик движка (D11). Имена и лейблы — дословно из таблицы D11:
// счётчики с суффиксом _total, gauge без него.
const (
	// metricJobs — gauge числа заданий по паре «тема + состояние»;
	// источник — тикер статистики (CountByState).
	metricJobs = "brooq_jobs"
	// metricReadyOldestAgeSeconds — gauge возраста старейшего готового
	// задания темы; источник — тикер статистики (OldestReadyAge).
	metricReadyOldestAgeSeconds = "brooq_ready_oldest_age_seconds"
	// metricClaimTotal — счётчик взятий по исходу: claimed/empty.
	metricClaimTotal = "brooq_claim_total"
	// metricSettleTotal — счётчик записанных исходов:
	// done/retry/dead/canceled.
	metricSettleTotal = "brooq_settle_total"
	// metricAttemptDurationSum — сумма длительностей попыток, секунды
	// (дробная: доли секунды не отбрасываются).
	// Реестр не умеет гистограмм, поэтому длительность публикуется парой
	// «сумма + число» (как summary в Middleware): по ним считается
	// средняя длительность попытки темы. Пара, а не гистограмма, потому
	// что среднее и число попыток — то, что нужно для SLO.
	metricAttemptDurationSum = "brooq_attempt_duration_seconds_sum"
	// metricAttemptDurationCount — число исполненных попыток, вторая
	// половина пары длительности.
	metricAttemptDurationCount = "brooq_attempt_duration_seconds_count"
	// metricLeaseExpiredTotal — счётчик возвращённых Reaper-ом заданий
	// с истёкшей арендой (D8).
	metricLeaseExpiredTotal = "brooq_lease_expired_total"
	// metricLeaseLostTotal — heartbeat вернул 0 строк: аренда потеряна
	// (отмена снаружи или перехват воркером), D13, D14.
	metricLeaseLostTotal = "brooq_lease_lost_total"
	// metricHungWorkersTotal — watchdog: обработчик не вышел после
	// отмены контекста в пределах грации (D15).
	metricHungWorkersTotal = "brooq_hung_workers_total"
	// metricWorkerBusy — gauge занятых слотов пула воркеров темы.
	metricWorkerBusy = "brooq_worker_busy"
	// metricRetentionDeletedTotal — счётчик удалённых уборкой
	// терминальных строк по темам: сколько заданий не копится вечно.
	metricRetentionDeletedTotal = "brooq_retention_deleted_total"
)

// Лейблы метрик (D11).
const (
	// labelTopic — тема задания.
	labelTopic = "topic"
	// labelState — состояние задания.
	labelState = "state"
	// labelOutcome — исход события (взятие, запись исхода).
	labelOutcome = "outcome"
)

// incMetric увеличивает счётчик, если порт метрик задан. nil-овый порт
// допустим: в тестах и при сборке без наблюдаемости счётчики просто не
// ведутся.
func (q *Queue) incMetric(name, topic string) {
	if q.cfg.Metrics == nil {
		return
	}
	q.cfg.Metrics.Inc(name, map[string]string{labelTopic: topic})
}

// incOutcomeMetric увеличивает счётчик с лейблами темы и исхода.
func (q *Queue) incOutcomeMetric(name string, topic queue.Topic, outcome string) {
	if q.cfg.Metrics == nil {
		return
	}
	q.cfg.Metrics.Inc(name, map[string]string{
		labelTopic:   string(topic),
		labelOutcome: outcome,
	})
}

// addTopicMetric увеличивает счётчик темы на целую delta.
func (q *Queue) addTopicMetric(name string, topic queue.Topic, delta uint64) {
	if q.cfg.Metrics == nil {
		return
	}
	q.cfg.Metrics.Add(name, map[string]string{labelTopic: string(topic)}, delta)
}

// addTopicMetricFloat увеличивает счётчик темы на дробную delta (сумма
// длительностей попыток в секундах).
func (q *Queue) addTopicMetricFloat(name string, topic queue.Topic, delta float64) {
	if q.cfg.Metrics == nil {
		return
	}
	q.cfg.Metrics.AddFloat(name, map[string]string{labelTopic: string(topic)}, delta)
}

// setTopicMetric устанавливает gauge темы в абсолютное значение.
func (q *Queue) setTopicMetric(name string, topic queue.Topic, value float64) {
	if q.cfg.Metrics == nil {
		return
	}
	q.cfg.Metrics.Set(name, value, map[string]string{labelTopic: string(topic)})
}

// observeAttemptDuration публикует пару длительности попытки: сумма
// секунд и число попыток. Сумма — дробная: доли секунды не отбрасываются,
// иначе у быстрых заданий sum остаётся нулём и средняя длительность
// (sum/count), на которой держится SLO, неверна.
func (q *Queue) observeAttemptDuration(topic queue.Topic, d time.Duration) {
	if q.cfg.Metrics == nil {
		return
	}
	seconds := d.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	q.addTopicMetricFloat(metricAttemptDurationSum, topic, seconds)
	q.addTopicMetric(metricAttemptDurationCount, topic, 1)
}

// busyInc занимает один слот темы и публикует gauge абсолютным
// значением. Порт метрик не умеет инкремент со знаком, поэтому счётчик
// занятых слотов ведёт сам движок, а наружу отдаёт абсолют (D11).
func (q *Queue) busyInc(topic queue.Topic) {
	q.busyMu.Lock()
	q.busy[topic]++
	busy := q.busy[topic]
	q.busyMu.Unlock()
	q.publishBusy(topic, busy)
}

// busyDec освобождает один слот темы и публикует новый абсолют.
// Отрицательный результат — следствие ошибки балансировки (освобождений
// больше, чем занятий), поэтому значение зажимается в 0: счётчик занятых
// слотов не может быть меньше нуля.
func (q *Queue) busyDec(topic queue.Topic) {
	q.busyMu.Lock()
	q.busy[topic]--
	busy := q.busy[topic]
	if busy < 0 {
		busy = 0
	}
	q.busy[topic] = busy
	q.busyMu.Unlock()
	q.publishBusy(topic, busy)
}

// publishBusy публикует gauge brooq_worker_busy абсолютным значением
// счётчика занятых слотов. Отрицательное значение означает ошибку
// балансировки: наружу уходит 0, а в лог — одно предупреждение на
// переход, без спама.
func (q *Queue) publishBusy(topic queue.Topic, busy int64) {
	if busy < 0 {
		q.log.Warn("gauge занятых слотов ушёл в минус: освобождений больше, чем занятий",
			"topic", string(topic), "busy", busy)
		busy = 0
	}
	// Значение проверено выше: отрицательным быть не может.
	q.setTopicMetric(metricWorkerBusy, topic, float64(busy))
}
