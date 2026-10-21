// Постановка задания в очередь (задача 3.1): одна форма вставки с
// идемпотентностью по паре «тема + ключ».
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// insertJobSQL — постановка задания. Идемпотентность закрыта частичным
// уникальным индексом job_idempotency_idx: конфликт разрешается именно
// по нему, поэтому предикат индекса повторён в ON CONFLICT. При
// NULL-ключе строки не конфликтуют между собой, а available_at в
// прошлом не нужен: нулевое время означает «готово сейчас».
//
// Вставка возвращает идентификатор созданной строки; пустой результат
// означает конфликт по индексу job_idempotency_idx.
const insertJobSQL = `
INSERT INTO job (id, topic, payload, idempotency_key, available_at)
VALUES (gen_random_uuid(), $1, $2, $3, COALESCE($4::timestamptz, now()))
ON CONFLICT (topic, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING id`

// selectJobIDByIdempotencySQL — идентификатор уже существующего
// задания по паре «тема + ключ». Отдельный оператор, а не CTE: снимок
// CTE взят до ожидания на конфликте, поэтому конкурентно
// закоммиченную строку он не увидит (design.md, D1).
const selectJobIDByIdempotencySQL = `
SELECT id FROM job WHERE topic = $1 AND idempotency_key = $2`

// Enqueue ставит задание в очередь через пул: открывает собственную
// короткую транзакцию, чтобы запись шла через pgx.Tx.
// Повторная постановка с тем же ключом идемпотентности не создаёт
// второе задание и не считается ошибкой.
//
// Возвращает идентификатор задания и признак created: created == false
// означает, что задание с той же парой «тема + ключ идемпотентности»
// уже было, а id принадлежит ему.
func (r *Repository) Enqueue(ctx context.Context, job queue.Job) (id string, created bool, err error) {
	tx, err := r.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	// Откат после Commit возвращает ErrTxClosed — это не авария
	// (см. queue.Tx), поэтому ошибка игнорируется.
	defer func() { _ = tx.Rollback(ctx) }()

	id, created, err = r.EnqueueTx(ctx, tx, job)
	if err != nil {
		return "", false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", false, fmt.Errorf("фиксация постановки задания темы %s: %w", job.Topic, err)
	}
	return id, created, nil
}

// EnqueueTx ставит задание в транзакции производителя: задание и
// побочные записи производителя фиксируются вместе — откат транзакции
// не оставляет задания.
//
// Возвращает идентификатор задания и признак created: created == false
// означает, что задание с той же парой «тема + ключ идемпотентности»
// уже было, а id принадлежит ему. Успешная вставка — один оператор;
// второй выполняется только при нуле строк от вставки (design.md, D2–D3).
func (r *Repository) EnqueueTx(ctx context.Context, tx queue.Tx, job queue.Job) (id string, created bool, err error) {
	// Пустой ключ означает «ключа нет»: в базе это NULL, иначе все
	// задания без ключа конфликтовали бы друг с другом.
	var key *string
	if job.IdempotencyKey != "" {
		k := job.IdempotencyKey
		key = &k
	}
	// Нулевое время — «готово сейчас»; в SQL это COALESCE с now().
	var availableAt *time.Time
	if !job.AvailableAt.IsZero() {
		at := job.AvailableAt
		availableAt = &at
	}

	rows, err := tx.Query(ctx, insertJobSQL,
		string(job.Topic), []byte(job.Payload), key, availableAt)
	if err != nil {
		return "", false, fmt.Errorf("постановка задания темы %s: %w", job.Topic, err)
	}
	defer rows.Close()

	if rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", false, fmt.Errorf("постановка задания темы %s: %w", job.Topic, err)
		}
		return id, true, nil
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("постановка задания темы %s: %w", job.Topic, err)
	}
	rows.Close() // до следующего оператора в этой же транзакции

	// Конфликт возможен только при непустом ключе: пустой ключ — это NULL,
	// частичный индекс такие строки не покрывает.
	if key == nil {
		return "", false, fmt.Errorf("постановка задания темы %s: вставка не создала строку без ключа идемпотентности", job.Topic)
	}

	rows, err = tx.Query(ctx, selectJobIDByIdempotencySQL, string(job.Topic), *key)
	if err != nil {
		return "", false, fmt.Errorf("постановка задания темы %s: %w", job.Topic, err)
	}
	defer rows.Close()

	if rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return "", false, fmt.Errorf("постановка задания темы %s: %w", job.Topic, err)
		}
		return id, false, nil
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("постановка задания темы %s: %w", job.Topic, err)
	}
	return "", false, fmt.Errorf("постановка задания темы %s: конфликт по ключу идемпотентности, но существующая строка не найдена", job.Topic)
}
