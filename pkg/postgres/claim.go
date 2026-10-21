// Взятие задания (задача 3.2, D4): одна SQL-строка, попытка списывается
// в том же операторе, что и закрепление аренды (D5).
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// claimSQL — взятие по D4. Выборка кандидатов и UPDATE идут одной
// строкой: «прочитал, потом обновил» здесь невозможно по построению.
//
//   - state IN ('pending','failed') — failed это ожидание повтора,
//     поэтому повтор берётся после available_at;
//   - FOR NO KEY UPDATE SKIP LOCKED — занятая строка не задерживает
//     второе взятие: оно уходит к следующей;
//   - ORDER BY available_at, id — FIFO внутри темы, стабильный при
//     равном времени;
//   - attempts = attempts + 1 — попытка списывается при взятии, поэтому
//     RETURNING отдаёт уже номер состоявшейся попытки;
//   - locked_by и locked_until заполняются одновременно: задание в
//     processing всегда имеет аренду.
const claimSQL = `
WITH next AS (
    SELECT id
    FROM job
    WHERE state IN ('pending', 'failed')
      AND topic = $1
      AND available_at <= now()
    ORDER BY available_at, id
    LIMIT $2
    FOR NO KEY UPDATE SKIP LOCKED
)
UPDATE job j
SET state        = 'processing',
    locked_by    = $3,
    locked_until = now() + $4::interval,
    attempts     = j.attempts + 1,
    updated_at   = now()
FROM next
WHERE j.id = next.id
RETURNING j.id, j.topic, j.payload, j.attempts, j.locked_by, j.locked_until`

// Claim берёт до limit заданий темы topic для воркера worker и
// закрепляет их арендой на срок lease. Пустой результат — заданий,
// готовых к работе, нет; это не ошибка. limit меньше 1 означает «нечего
// брать», lease должен быть положительным (иначе аренда истечёт сразу).
//
// Взятие — запись, поэтому идёт через транзакцию вызывающего: аренда
// фиксируется вместе с тем, что вызывающий успел записать.
func (r *Repository) Claim(ctx context.Context, tx queue.Tx, topic queue.Topic,
	worker string, lease time.Duration, limit int) ([]queue.Claim, error) {
	if limit < 1 {
		return nil, nil
	}

	rows, err := tx.Query(ctx, claimSQL, string(topic), limit, worker, lease)
	if err != nil {
		return nil, fmt.Errorf("взятие заданий темы %s: %w", topic, err)
	}
	defer rows.Close()

	var claims []queue.Claim
	for rows.Next() {
		var (
			id       string
			jobTopic string
			payload  []byte
			attempt  int
			lockedBy string
			until    time.Time
		)
		if err := rows.Scan(&id, &jobTopic, &payload, &attempt, &lockedBy, &until); err != nil {
			return nil, fmt.Errorf("разбор взятого задания темы %s: %w", topic, err)
		}
		claims = append(claims, queue.Claim{
			ID:      id,
			Topic:   queue.Topic(jobTopic),
			Payload: payload,
			Attempt: attempt,
			Lease:   queue.Lease{Worker: lockedBy, Until: until},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("обход взятых заданий темы %s: %w", topic, err)
	}
	return claims, nil
}
