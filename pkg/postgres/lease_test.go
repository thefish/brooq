// Приёмка продления аренды (задача 3.6, D13): пока обработчик жив, воркер
// периодически отодвигает locked_until. Продление обязано идти с guard-ом
// владельца и состояния: воркер, проснувшийся после истечения аренды, не
// продлевает её чужому заданию, а задание, переведённое отменяющим в
// canceled, продлить нельзя (D14). Снятие любого из двух условий guard-а из
// renewLeaseSQL видно здесь.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/queue"
)

// TestRenewLease — контракт продления аренды на подготовленных строках:
// своя аренда продлевается (1 строка, срок и updated_at сдвинуты), чужая
// аренда, задание вне processing и несуществующий id дают 0 строк без
// ошибки и не меняют строку. 0 строк — это сигнал о потере аренды (D14), а
// не сбой, поэтому «не продлилось» проверяется по числу строк и по
// неизменности строки, а не по ошибке.
func TestRenewLease(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const (
		// owner — воркер, за которым закреплена аренда подготовленной
		// строки.
		owner = "w-renew"
		// stranger — другой воркер: его продление обязано пройти мимо
		// строки.
		stranger = "w-stranger"
		// lease — срок продления, который запрашивают кейсы.
		lease = time.Hour
		// heldUntil — остаток аренды в подготовленной строке: заметно
		// меньше запрашиваемого, иначе продление было бы не видно.
		heldUntil = 10 * time.Minute
		// clockWindow — допуск на разницу между временем Go и базы: срок
		// аренды считается от now() базы.
		clockWindow = 5 * time.Second
		// missingID — валидный UUID, которого в таблице нет.
		missingID = "00000000-0000-0000-0000-0000000000ff"
	)

	// dur возвращает указатель на ожидаемый сдвиг срока аренды: nil в
	// таблице означает «locked_until не сдвигается».
	dur := func(d time.Duration) *time.Duration { return &d }

	// insertJob кладёт задание в состоянии state и отдаёт его id; owner —
	// воркер, за которым закреплена аренда ("" — аренды нет).
	insertJob := func(t *testing.T, state queue.State, owner string) string {
		t.Helper()

		var (
			lockedBy *string
			until    *time.Time
		)
		if owner != "" {
			lockedBy = &owner
			u := time.Now().Add(heldUntil)
			until = &u
		}

		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, attempts,
			                  available_at, updated_at, locked_by, locked_until)
			 VALUES (gen_random_uuid(), 't-renew', '{}'::jsonb, $1, 1,
			         now() - interval '1 hour', now() - interval '1 hour', $2, $3)
			 RETURNING id::text`,
			string(state), lockedBy, until,
		).Scan(&id))
		return id
	}

	// readJob читает строку целиком (leaseRow — из reaper_test.go: для
	// продления нужен и locked_until).
	readJob := func(t *testing.T, id string) leaseRow {
		t.Helper()

		var row leaseRow
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, attempts, available_at, updated_at, locked_by, locked_until
			   FROM job WHERE id = $1`, id,
		).Scan(&row.state, &row.attempts, &row.availableAt, &row.updatedAt,
			&row.lockedBy, &row.lockedUntil))
		return row
	}

	// renew — продление в собственной транзакции с фиксацией:
	// транзакцию заводит вызывающий, и без Commit продление не видно
	// снаружи.
	renew := func(t *testing.T, id, worker string, lease time.Duration) int64 {
		t.Helper()

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция продления")
		defer func() { _ = tx.Rollback(ctx) }()

		n, err := repo.RenewLease(ctx, tx, id, worker, lease)
		require.NoError(t, err, "продление — 0 строк или 1 строка, но не ошибка")
		require.NoError(t, tx.Commit(ctx))
		return n
	}

	// assertSameLease — срок аренды не изменился: nil и nil, либо два
	// равных значения.
	assertSameLease := func(t *testing.T, before, after leaseRow) {
		t.Helper()
		if before.lockedUntil == nil {
			assert.Nil(t, after.lockedUntil, "аренды не было и не появилось")
			return
		}
		require.NotNil(t, after.lockedUntil, "срок аренды не пропал")
		assert.True(t, before.lockedUntil.Equal(*after.lockedUntil),
			"locked_until не меняется: %v против %v", before.lockedUntil, after.lockedUntil)
	}

	tests := []struct {
		name string
		// state — состояние подготовленной строки.
		state queue.State
		// owner — воркер, за которым закреплена аренда ("" — аренды нет).
		owner string
		// renewBy — воркер, который продлевает аренду.
		renewBy string
		// lease — запрашиваемый срок продления.
		lease time.Duration
		// missing — продлевается id, которого нет в таблице.
		missing bool
		// wantRows — сколько строк обязано изменить продление.
		wantRows int64
		// wantUntilIn — ожидаемый locked_until как сдвиг от момента
		// вызова; nil — locked_until обязан остаться прежним.
		wantUntilIn *time.Duration
	}{
		{
			name:        "своя аренда в processing — продлена",
			state:       queue.StateProcessing,
			owner:       owner,
			renewBy:     owner,
			lease:       lease,
			wantRows:    1,
			wantUntilIn: dur(lease),
		},
		{
			name:     "чужая аренда — 0 строк, строка не изменилась",
			state:    queue.StateProcessing,
			owner:    owner,
			renewBy:  stranger,
			lease:    lease,
			wantRows: 0,
		},
		{
			// Отмена в полёте (D14) переводит задание в canceled: продление
			// после неё обязано не сработать, иначе воркер считал бы себя
			// владельцем отменённого задания.
			name:     "продление после отмены — 0 строк",
			state:    queue.StateCanceled,
			owner:    owner,
			renewBy:  owner,
			lease:    lease,
			wantRows: 0,
		},
		{
			name:     "продление задания в pending — 0 строк",
			state:    queue.StatePending,
			owner:    owner,
			renewBy:  owner,
			lease:    lease,
			wantRows: 0,
		},
		{
			// Граница guard-а владения: у processing аренда обязана быть,
			// но NULL-владелец не совпадает ни с каким воркером.
			name:     "продление processing без владельца — 0 строк",
			state:    queue.StateProcessing,
			owner:    "",
			renewBy:  owner,
			lease:    lease,
			wantRows: 0,
		},
		{
			name:     "несуществующий id — 0 строк",
			missing:  true,
			renewBy:  owner,
			lease:    lease,
			wantRows: 0,
		},
		{
			// Нулевой срок — граница арифметики: продление состоялось
			// (1 строка), но аренда не отодвинута, а сдвинута на «сейчас».
			name:        "нулевой срок — строка обновлена, аренда не отодвинута",
			state:       queue.StateProcessing,
			owner:       owner,
			renewBy:     owner,
			lease:       0,
			wantRows:    1,
			wantUntilIn: dur(0),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := missingID
			var before leaseRow
			if !tc.missing {
				id = insertJob(t, tc.state, tc.owner)
				before = readJob(t, id)
			}

			calledAt := time.Now()
			n := renew(t, id, tc.renewBy, tc.lease)
			assert.Equal(t, tc.wantRows, n, "число продлённых строк")

			if tc.missing {
				var count int
				require.NoError(t, pool.QueryRow(ctx,
					`SELECT COUNT(*)::int FROM job WHERE id = $1`, id).Scan(&count))
				assert.Zero(t, count, "продление не создаёт строку")
				return
			}

			after := readJob(t, id)
			assert.Equal(t, before.state, after.state, "состояние продлением не меняется")
			assert.Equal(t, before.lockedBy, after.lockedBy, "владелец аренды не меняется")
			assert.Equal(t, before.attempts, after.attempts, "продление не трогает счётчик попыток")

			if tc.wantUntilIn == nil {
				// 0 строк — строка осталась нетронутой целиком.
				assertSameLease(t, before, after)
				assert.True(t, before.updatedAt.Equal(after.updatedAt),
					"updated_at не меняется: строку никто не трогал")
				return
			}

			require.NotNil(t, after.lockedUntil, "у продлённого задания есть срок аренды")
			assert.WithinDuration(t, calledAt.Add(*tc.wantUntilIn), *after.lockedUntil, clockWindow,
				"locked_until = now + запрошенный срок")
			assert.True(t, after.updatedAt.After(before.updatedAt),
				"updated_at обновлён вместе с арендой: %s против %s", after.updatedAt, before.updatedAt)
		})
	}
}
