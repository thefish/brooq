// Выборка статистики для метрик (задача 3.5, D11): число заданий по паре
// «тема + состояние» и возраст старейшего готового задания по теме.
//
// Оба запроса — чтения, поэтому идут через пул, а не через транзакцию:
// снимок «примерно сейчас» для gauge достаточен, а
// транзакция ради чтения только держала бы снимок и мешала бы сборщику
// метрик не блокировать запись.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// countByStateSQL — число заданий по каждой паре «тема + состояние».
// Пары сортируются, чтобы вывод сборщика метрик был детерминированным.
// Покрывается индексом job_topic_idx (topic, state) из 00001.
const countByStateSQL = `
SELECT topic, state, count(*)
FROM job
GROUP BY topic, state
ORDER BY topic, state`

// oldestReadyAgeSQL — возраст старейшего готового задания по теме.
//
//   - готовые — это state IN ('pending','failed') с available_at <= now():
//     ровно те строки, что возьмёт Claim (D4);
//   - now() - min(available_at) — сколько задание ждёт своей очереди;
//     для пустой темы строки нет, и тема в результат не попадает;
//   - фильтр бьёт в job_ready_topic_idx (topic, available_at) из 00002;
//   - EXTRACT(EPOCH …) отдаёт секунды числом: так хранение interval не
//     зависит от отображения типа драйвером, а метод возвращает
//     time.Duration без промежуточного типа.
const oldestReadyAgeSQL = `
SELECT topic, EXTRACT(EPOCH FROM now() - min(available_at))
FROM job
WHERE state IN ('pending', 'failed') AND available_at <= now()
GROUP BY topic`

// CountByState возвращает число заданий по каждой паре «тема + состояние».
// Чтение идёт через пул. Пустая таблица — пустой результат без
// ошибки: отсутствие данных это не ошибка.
func (r *Repository) CountByState(ctx context.Context) ([]queue.TopicStateCount, error) {
	rows, err := r.pool.Query(ctx, countByStateSQL)
	if err != nil {
		return nil, fmt.Errorf("выборка числа заданий по состояниям: %w", err)
	}
	defer rows.Close()

	counts := make([]queue.TopicStateCount, 0)
	for rows.Next() {
		var (
			topic string
			state string
			count int64
		)
		if err := rows.Scan(&topic, &state, &count); err != nil {
			return nil, fmt.Errorf("чтение числа заданий по состояниям: %w", err)
		}
		counts = append(counts, queue.TopicStateCount{
			Topic: queue.Topic(topic),
			State: queue.State(state),
			Count: count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("обход числа заданий по состояниям: %w", err)
	}
	return counts, nil
}

// OldestReadyAge возвращает возраст старейшего готового задания по каждой
// теме: сколько времени самое давнее из готовых заданий уже ждёт взятия.
// Темы без готовых заданий в результат не попадают; пустая таблица — пустой
// результат без ошибки.
//
// Чтение идёт через пул: это gauge для метрики
// brooq_ready_oldest_age_seconds (D11).
func (r *Repository) OldestReadyAge(ctx context.Context) ([]queue.TopicAge, error) {
	rows, err := r.pool.Query(ctx, oldestReadyAgeSQL)
	if err != nil {
		return nil, fmt.Errorf("выборка возраста старейшего готового задания: %w", err)
	}
	defer rows.Close()

	ages := make([]queue.TopicAge, 0)
	for rows.Next() {
		var (
			topic   string
			seconds float64
		)
		if err := rows.Scan(&topic, &seconds); err != nil {
			return nil, fmt.Errorf("чтение возраста старейшего готового задания: %w", err)
		}
		ages = append(ages, queue.TopicAge{
			Topic: queue.Topic(topic),
			Age:   time.Duration(seconds * float64(time.Second)),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("обход возраста старейшего готового задания: %w", err)
	}
	return ages, nil
}
