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
const insertJobSQL = `
INSERT INTO job (id, topic, payload, idempotency_key, available_at)
VALUES (gen_random_uuid(), $1, $2, $3, COALESCE($4::timestamptz, now()))
ON CONFLICT (topic, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`

// Enqueue ставит задание в очередь через пул: открывает собственную
// короткую транзакцию, чтобы запись шла через pgx.Tx.
// Повторная постановка с тем же ключом идемпотентности не создаёт
// второе задание и не считается ошибкой.
func (r *Repository) Enqueue(ctx context.Context, job queue.Job) error {
	tx, err := r.Begin(ctx)
	if err != nil {
		return err
	}
	// Откат после Commit возвращает ErrTxClosed — это не авария
	// (см. queue.Tx), поэтому ошибка игнорируется.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := r.EnqueueTx(ctx, tx, job); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("фиксация постановки задания темы %s: %w", job.Topic, err)
	}
	return nil
}

// EnqueueTx ставит задание в транзакции производителя: задание и
// побочные записи производителя фиксируются вместе — откат транзакции
// не оставляет задания.
func (r *Repository) EnqueueTx(ctx context.Context, tx queue.Tx, job queue.Job) error {
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

	if _, err := tx.Exec(ctx, insertJobSQL,
		string(job.Topic), []byte(job.Payload), key, availableAt); err != nil {
		return fmt.Errorf("постановка задания темы %s: %w", job.Topic, err)
	}
	return nil
}
