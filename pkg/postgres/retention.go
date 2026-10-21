// Уборка терминальных заданий по TTL: удаление строк в состоянии done,
// dead и canceled, которые пролежали терминальными дольше ttl.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// deleteTerminalSQL — одна пачка уборки.
//
//   - пачками, а не всё сразу: один большой DELETE держит блокировки и
//     порождает пик мёртвых строк. Вызывающий крутит цикл, пока
//     возвращается ровно batch строк, и между пачками может отдохнуть;
//   - FOR UPDATE SKIP LOCKED в подзапросе: несколько процессов (или
//     реплик) убирают разные строки, а не ждут друг друга;
//   - ORDER BY updated_at: сначала самое старое;
//   - бьёт в job_retention_idx из миграции 00003 — частичный индекс
//     ровно по терминальным строкам.
const deleteTerminalSQL = `
DELETE FROM job
WHERE id IN (
    SELECT id
    FROM job
    WHERE state IN ('done', 'dead', 'canceled')
      AND updated_at < now() - $1::interval
    ORDER BY updated_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING id, topic`

// DeleteTerminal удаляет до batch терминальных заданий, пролежавших
// терминальными дольше ttl, и отдаёт их идентификаторы с темами — для
// журнала и метрики вызывающего. Пустой результат — не ошибка: это
// значит «убирать больше нечего».
//
// batch меньше 1 — нечего убирать, к базе не обращаемся (как Claim при
// limit < 1). ttl не больше нуля — уборка выключена: это защита от
// случайного DELETE всей терминальной части таблицы.
//
// Удаление — запись, поэтому идёт через транзакцию вызывающего.
func (r *Repository) DeleteTerminal(ctx context.Context, tx queue.Tx,
	ttl time.Duration, batch int) ([]queue.DeletedJob, error) {
	if batch < 1 {
		return nil, nil
	}
	if ttl <= 0 {
		return nil, nil
	}

	rows, err := tx.Query(ctx, deleteTerminalSQL, ttl, batch)
	if err != nil {
		return nil, fmt.Errorf("уборка терминальных заданий: %w", err)
	}
	defer rows.Close()

	var deleted []queue.DeletedJob
	for rows.Next() {
		var job queue.DeletedJob
		var topic string
		if err := rows.Scan(&job.ID, &topic); err != nil {
			return nil, fmt.Errorf("чтение удалённого задания: %w", err)
		}
		job.Topic = queue.Topic(topic)
		deleted = append(deleted, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("чтение удалённых заданий: %w", err)
	}
	return deleted, nil
}
