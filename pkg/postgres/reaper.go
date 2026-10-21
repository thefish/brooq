// Возврат истёкших аренд (задача 3.4, D8): задание, чья аренда истекла,
// возвращается в pending. Попытки при этом не меняются — истечение
// аренды признак падения процесса, а не записанного сбоя.
package postgres

import (
	"context"
	"fmt"

	"github.com/thefish/brooq/pkg/queue"
)

// reapExpiredSQL — возврат истёкших аренд по D8 дословно.
//
//   - guard state = 'processing' обязателен: без него оператор трогал бы
//     терминальные задания (done, dead, canceled), у которых
//     locked_until остался с прошлой аренды, и воскрешал бы их. Он же
//     делает возврат идемпотентным: второй одновременный вызов увидит
//     строку уже в pending и в RETURNING её не получит — одно задание не
//     вернётся дважды;
//   - attempts не меняется: аренда истекла у упавшего воркера, а не
//     исчерпала попытки (инвариант 7);
//   - available_at = now(), без дополнительной задержки: сама аренда и
//     есть backoff упавшего воркера — задание не вернулось раньше, чем
//     истёк её срок;
//   - бьёт в job_lease_idx, который уже есть в миграции 00001.
const reapExpiredSQL = `
UPDATE job
SET state        = 'pending',
    locked_by    = NULL,
    locked_until = NULL,
    available_at = now(),
    updated_at   = now()
WHERE state = 'processing' AND locked_until < now()
RETURNING id, topic`

// ReapExpired возвращает в pending все задания, аренда которых истекла,
// и отдаёт их идентификаторы с темами — для журнала вызывающего.
// Возврат идемпотентен: два одновременных вызова не вернут одно задание
// дважды, потому что второй увидит уже изменённую строку и не попадёт
// под guard state = 'processing'. Запись идёт в транзакции вызывающего,
// фиксирует её он же.
func (r *Repository) ReapExpired(ctx context.Context, tx queue.Tx) ([]queue.ReapedJob, error) {
	rows, err := tx.Query(ctx, reapExpiredSQL)
	if err != nil {
		return nil, fmt.Errorf("возврат истёкших аренд: %w", err)
	}
	defer rows.Close()

	var reaped []queue.ReapedJob
	for rows.Next() {
		var job queue.ReapedJob
		var topic string
		if err := rows.Scan(&job.ID, &topic); err != nil {
			return nil, fmt.Errorf("чтение возвращённого задания: %w", err)
		}
		job.Topic = queue.Topic(topic)
		reaped = append(reaped, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("чтение возвращённых заданий: %w", err)
	}
	return reaped, nil
}
