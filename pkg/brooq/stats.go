// Сбор статистики для gauge-метрик (задача 5.2, D11): раз в период
// движок читает число заданий по парам «тема + состояние» и возраст
// старейшего готового задания темы и публикует их абсолютными
// значениями. SQL здесь нет: оба чтения — методы порта Repo,
// операторы живут в pkg/postgres.
package brooq

import (
	"context"
	"sort"
	"strings"
	"time"
)

// defaultStatsPeriod — период сбора статистики по D11: раз в 30 с. При
// росте объёма добавляется лимит, как в pgqueue.
const defaultStatsPeriod = 30 * time.Second

// statsPeriod — период сбора: явно заданный Config.StatsPeriod (> 0)
// переопределяет дефолт D11.
func (q *Queue) statsPeriod() time.Duration {
	if q.cfg.StatsPeriod > 0 {
		return q.cfg.StatsPeriod
	}
	return defaultStatsPeriod
}

// runStats — горутина сбора статистики: на каждый тик публикует
// brooq_jobs{topic,state} и brooq_ready_oldest_age_seconds{topic}.
// Выход по контексту движка, как у воркеров: отмена гасит горутину
// немедленно. Ошибка чтения одного тика не убивает цикл — следующий тик
// наступит по таймеру.
func (q *Queue) runStats(ctx context.Context) {
	period := q.statsPeriod()
	q.log.Debug("сбор статистики запущен", "period", period)

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		q.collectStats(ctx)
	}
}

// collectStats — один тик: два чтения через пул и публикация
// gauge'ей. Без порта метрик читать незачем: результат некому отдать.
//
// Gauge отражает ТЕКУЩЕЕ состояние: после публикации нового набора серий
// все серии того же gauge, опубликованные в прошлый тик и в этом
// отсутствующие, публикуются нулём. Без этого опустевшая тема навсегда
// оставляет brooq_ready_oldest_age_seconds{topic} > 0 (вечный ложный
// алерт «очередь встала») и фантомный brooq_jobs{topic,state="processing"}.
// Ошибка чтения оставляет прошлый набор как есть: обнулять его на
// временном сбое БД нельзя.
func (q *Queue) collectStats(ctx context.Context) {
	if q.cfg.Metrics == nil {
		return
	}

	counts, err := q.repo.CountByState(ctx)
	if err != nil {
		q.log.Error("сбор числа заданий по состояниям не удался", "error", err)
	} else {
		samples := make([]gaugeSample, 0, len(counts))
		for _, c := range counts {
			samples = append(samples, gaugeSample{
				value: nonNegative(c.Count),
				labels: map[string]string{
					labelTopic: string(c.Topic),
					labelState: string(c.State),
				},
			})
		}
		q.publishGauge(metricJobs, samples)
	}

	ages, err := q.repo.OldestReadyAge(ctx)
	if err != nil {
		q.log.Error("сбор возраста старейшего готового задания не удался", "error", err)
		return
	}
	samples := make([]gaugeSample, 0, len(ages))
	for _, a := range ages {
		samples = append(samples, gaugeSample{
			value:  nonNegativeSeconds(a.Age.Seconds()),
			labels: map[string]string{labelTopic: string(a.Topic)},
		})
	}
	q.publishGauge(metricReadyOldestAgeSeconds, samples)
}

// gaugeSample — одно значение gauge вместе с набором лейблов.
type gaugeSample struct {
	value  float64
	labels map[string]string
}

// publishGauge публикует набор значений gauge name и обнуляет серии,
// которых в наборе нет: gauge без такой зачистки не умеет уменьшать
// число серий, и исчезнувшая пара «тема + состояние» продолжает
// печататься со старым значением.
func (q *Queue) publishGauge(name string, samples []gaugeSample) {
	q.statsMu.Lock()
	defer q.statsMu.Unlock()

	published := make(map[string]map[string]string, len(samples))
	for _, s := range samples {
		q.cfg.Metrics.Set(name, s.value, s.labels)
		published[statsLabelKey(s.labels)] = s.labels
	}

	// Зачистка идёт по фактически опубликованным ключам, а не по перебору
	// состояний: нулей для тем, которых очередь не касалась, не будет.
	for key, labels := range q.statsPublished[name] {
		if _, ok := published[key]; ok {
			continue
		}
		q.cfg.Metrics.Set(name, 0, labels)
	}
	q.statsPublished[name] = published
}

// statsLabelKey — стабильный ключ набора лейблов: пары «имя=значение»
// отсортированы, иначе порядок обхода карты дал бы разные ключи для
// одного и того же набора и зачистка не нашла бы старую серию.
func statsLabelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(labels))
	for k, v := range labels {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

// nonNegative приводит счётчик к float64: отрицательное значение —
// аномалия чтения, публикуется ноль, а не огромное число.
func nonNegative(v int64) float64 {
	if v < 0 {
		return 0
	}
	return float64(v)
}

// nonNegativeSeconds — то же для дробных секунд: возраст старейшего
// задания публикуется с долями, иначе всё, что моложе секунды, читается
// как ноль и алерт на застой не срабатывает.
func nonNegativeSeconds(v float64) float64 {
	if v < 0 {
		return 0
	}
	return v
}
