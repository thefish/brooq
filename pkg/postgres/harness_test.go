// Smoke-проверки харнесса: миграции применяются к чистой схеме тем же
// путём, что и на деплое (goose + каталог migrations/), и
// схема очереди несёт ограничения, на которые опирается движок. Это не
// тесты движка — их пишет первый change; здесь доказывается, что
// «make test с TEST_DATABASE_URL» вообще способен поймать поломку схемы.
package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/queue"
)

// TestSchemaApplied — миграции накатились: таблица job и её индексы есть,
// goose записал версию в журнал (второй прогон — no-op).
func TestSchemaApplied(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	var exists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables
		                WHERE table_schema = current_schema() AND table_name = 'job')`,
	).Scan(&exists))
	assert.True(t, exists, "таблица job создана миграцией 00001")

	for _, idx := range []string{"job_ready_idx", "job_lease_idx", "job_topic_idx"} {
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM pg_indexes
			                WHERE schemaname = current_schema() AND indexname = $1)`, idx,
		).Scan(&exists))
		assert.True(t, exists, "индекс %s создан миграцией 00001", idx)
	}

	var versions int
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM goose_db_version`).Scan(&versions))
	assert.GreaterOrEqual(t, versions, 2, "goose записал применённые версии (init + 00001)")
}

// TestJobStateConstraint — состояние задания ограничено перечнем: движок
// может опираться на CHECK, а не на договорённость в коде. Перечень
// расширен миграцией 00002 (шесть значений), но «значение вне перечня»
// остаётся ошибкой схемы.
func TestJobStateConstraint(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	// Значения вне перечня: мусорное, пустое и почти правильное
	// (canceled с двумя «l» — та самая опечатка, ради которой перечень
	// живёт в схеме, а не в комментарии).
	for _, tc := range []struct{ name, state string }{
		{"мусор", "мусор"},
		{"пустое значение", ""},
		{"canceled с двумя «l»", "cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pool.Exec(ctx,
				`INSERT INTO job (id, topic, payload, state) VALUES (gen_random_uuid(), 't', '{}'::jsonb, $1)`,
				tc.state)
			require.Error(t, err, "неизвестное состояние отклоняется CHECK-ограничением")
			assert.Contains(t, err.Error(), "job_state_check")
		})
	}
}

// TestJobStateCanceledAccepted — миграция 00002 расширила job_state_check
// до шести значений: 'canceled' принимается и читается обратно. Без этого
// расширения отмена задания (D14) падала бы на записи исхода.
func TestJobStateCanceledAccepted(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	var state string
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO job (id, topic, payload, state) VALUES (gen_random_uuid(), $1, '{}'::jsonb, 'canceled')
		 RETURNING state`, "t-canceled",
	).Scan(&state), "'canceled' принимается job_state_check")
	assert.Equal(t, "canceled", state)
}

// TestSchemaEngineIndexes — миграция 00002 добавила два индекса, на
// которые опирается движок: уникальный частичный по паре «тема + ключ»
// (идемпотентность постановки) и индекс горячего пути взятия по теме.
// Удаление любого из них — поломка схемы, а не кода.
func TestSchemaEngineIndexes(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	// Индексы миграции 00002: имя -> обязательные признаки определения.
	indexes := map[string][]string{
		"job_idempotency_idx": {"UNIQUE", "(topic, idempotency_key)", "WHERE (idempotency_key IS NOT NULL)"},
		"job_ready_topic_idx": {"(topic, available_at, id)"},
	}

	for name, wants := range indexes {
		var def string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes
			  WHERE schemaname = current_schema() AND indexname = $1`, name,
		).Scan(&def), "индекс %s создан миграцией 00002", name)
		for _, want := range wants {
			assert.Contains(t, def, want, "определение индекса %s: %s", name, def)
		}
	}
}

// TestJobIdempotencyUnique — идемпотентность держит схема, а не код:
// повторная вставка пары «тема + ключ»
// отклоняется уникальным индексом job_idempotency_idx. Здесь проверяется
// именно индекс — обход ON CONFLICT DO NOTHING проверяют тесты постановки.
func TestJobIdempotencyUnique(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()

	// insertJob вставляет задание с заданными темой и ключом; nil-ключ —
	// «ключа нет» (в базе NULL).
	insertJob := func(topic string, key *string) error {
		_, err := pool.Exec(ctx,
			`INSERT INTO job (id, topic, payload, idempotency_key)
			 VALUES (gen_random_uuid(), $1, '{}'::jsonb, $2)`, topic, key)
		return err
	}
	key := func(s string) *string { return &s }
	countJobs := func(topic string) int {
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1`, topic).Scan(&n))
		return n
	}
	// resetTopic убирает строки темы: `go test -count=N` повторяет тест в
	// той же схеме, и строки прошлого прогона исказили бы счёт.
	resetTopic := func(t *testing.T, topics ...string) {
		t.Helper()
		for _, topic := range topics {
			_, err := pool.Exec(ctx, `DELETE FROM job WHERE topic = $1`, topic)
			require.NoError(t, err)
		}
	}

	t.Run("повтор пары тема+ключ отклоняется", func(t *testing.T) {
		const topic = "t-idem-repeat"
		resetTopic(t, topic)
		require.NoError(t, insertJob(topic, key("k1")))

		err := insertJob(topic, key("k1"))
		require.Error(t, err, "второе задание на ту же пару не создаётся")
		assert.Contains(t, err.Error(), "job_idempotency_idx")
		assert.Equal(t, 1, countJobs(topic), "в таблице осталась одна строка")
	})

	t.Run("тот же ключ у другой темы — другое задание", func(t *testing.T) {
		resetTopic(t, "t-idem-a", "t-idem-b")
		require.NoError(t, insertJob("t-idem-a", key("shared")))
		require.NoError(t, insertJob("t-idem-b", key("shared")), "пара включает тему, а не только ключ")
	})

	t.Run("задания без ключа не дедуплицируются", func(t *testing.T) {
		const topic = "t-idem-null"
		resetTopic(t, topic)
		require.NoError(t, insertJob(topic, nil))
		require.NoError(t, insertJob(topic, nil), "NULL-ключ под частичный индекс не попадает")
		assert.Equal(t, 2, countJobs(topic))
	})
}

// jobRow — снимок строки задания: guard терминальности обязан оставить
// его байт в байт.
type jobRow struct {
	state       string
	attempts    int
	lastError   *string
	availableAt time.Time
	updatedAt   time.Time
	lockedBy    *string
}

// TestTerminalOutcomeGuard — guard терминальности в SQL записи исхода
// (D5, D14): запись исхода по заданию, уже
// находящемуся в терминальном состоянии, возвращает 0 строк и не меняет
// строку. Снятие условия state = 'processing' из
// markDoneSQL/markFailedSQL/markDeadSQL роняет этот тест — в этом и смысл
// проверки. Положительный контроль (запись по processing) доказывает, что
// guard не блокирует всё подряд, и заодно держит содержимое записи:
// состояние, счётчик попыток, last_error и available_at. Без этой части
// мутации вида «убрать attempts - 1», «писать dead вместо failed» или
// «забыть available_at» проходили бы гейт насквозь.
//
// Аренда строки выставляется при подготовке, а владелец берётся из самой
// строки: guard исхода теперь двухсоставный (state = 'processing' AND
// locked_by = worker), и без аренды в подготовленной строке он проверялся
// бы только половиной.
func TestTerminalOutcomeGuard(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const (
		// retryDelay — отсрочка повтора в положительных контролях:
		// заведомо в будущем, чтобы «failed ждёт available_at» было видно.
		retryDelay = time.Hour
		// clockWindow — допуск на разницу между моментом чтения строки и
		// моментом записи: время снимается в Go и в базе по отдельности.
		clockWindow = 5 * time.Second
		// terminalWorker — воркер, который держит аренду задания в
		// processing. Guard исхода требует locked_by = worker (D13),
		// поэтому взятая строка обязана нести владельца.
		terminalWorker = "w-terminal"
	)

	// attempts возвращает указатель на ожидаемое число попыток: nil в
	// таблице означает «счётчик не проверяется».
	attempts := func(n int) *int { return &n }

	// insertJob кладёт задание в заданном состоянии и отдаёт его id.
	// Задание в processing получает аренду: без неё строка нарушала бы
	// инвариант «processing всегда с арендой», а guard владения
	// (locked_by = worker) было бы не на чем проверить.
	insertJob := func(t *testing.T, state string) string {
		t.Helper()

		var (
			id       string
			lockedBy *string
			until    *time.Time
		)
		if state == string(queue.StateProcessing) {
			owner := terminalWorker
			lockedBy = &owner
			u := time.Now().Add(time.Hour)
			until = &u
		}

		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, attempts, last_error,
			                  available_at, locked_by, locked_until)
			 VALUES (gen_random_uuid(), 't-terminal', '{}'::jsonb, $1, 2,
			         'причина прошлого сбоя', now() - interval '1 hour', $2, $3)
			 RETURNING id::text`, state, lockedBy, until,
		).Scan(&id))
		return id
	}

	// readJob читает строку целиком.
	readJob := func(t *testing.T, id string) jobRow {
		t.Helper()

		var row jobRow
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, attempts, last_error, available_at, updated_at, locked_by
			   FROM job WHERE id = $1`, id,
		).Scan(&row.state, &row.attempts, &row.lastError, &row.availableAt, &row.updatedAt, &row.lockedBy))
		return row
	}

	tests := []struct {
		name string
		// state — исходное состояние строки.
		state string
		// mark — запись исхода, которая проверяется. worker — владелец
		// аренды из самой строки: guard исхода сравнивает locked_by = worker.
		mark func(tx queue.Tx, id, worker string) (int64, error)
		// wantRows — сколько строк обязана затронуть запись.
		wantRows int64
		// wantState — состояние после записи.
		wantState string
		// wantAttempts — ожидаемое число попыток после записи
		// (nil — счётчик не проверяется).
		wantAttempts *int
		// wantCause — подстрока last_error после записи ("" — не
		// проверяется).
		wantCause string
		// wantLastErrorNil — запись очищает причину сбоя (успех).
		wantLastErrorNil bool
		// wantAvailableIn — ожидаемый сдвиг available_at относительно
		// значения до вызова (0 — не проверяется).
		wantAvailableIn time.Duration
		// wantAvailableSame — available_at не менялся записью.
		wantAvailableSame bool
	}{
		{
			name:  "MarkDone по done — 0 строк",
			state: string(queue.StateDone),
			mark:  func(tx queue.Tx, id, worker string) (int64, error) { return repo.MarkDone(ctx, tx, id, worker) },
			// терминальное задание не перезаписывается
			wantRows:  0,
			wantState: string(queue.StateDone),
		},
		{
			name:  "MarkFailed по done — 0 строк",
			state: string(queue.StateDone),
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkFailed(ctx, tx, id, worker, "новый сбой", time.Now())
			},
			wantRows:  0,
			wantState: string(queue.StateDone),
		},
		{
			name:  "MarkFailedPreservingAttempt по done — 0 строк",
			state: string(queue.StateDone),
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkFailedPreservingAttempt(ctx, tx, id, worker, "новый сбой", time.Now())
			},
			wantRows:  0,
			wantState: string(queue.StateDone),
		},
		{
			name:  "MarkDead по done — 0 строк",
			state: string(queue.StateDone),
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkDead(ctx, tx, id, worker, "новый сбой")
			},
			wantRows:  0,
			wantState: string(queue.StateDone),
		},
		{
			name:      "MarkCanceled по done — no-op",
			state:     string(queue.StateDone),
			mark:      func(tx queue.Tx, id, worker string) (int64, error) { return repo.MarkCanceled(ctx, tx, id) },
			wantRows:  0,
			wantState: string(queue.StateDone),
		},
		{
			name:      "MarkDone по dead — 0 строк",
			state:     string(queue.StateDead),
			mark:      func(tx queue.Tx, id, worker string) (int64, error) { return repo.MarkDone(ctx, tx, id, worker) },
			wantRows:  0,
			wantState: string(queue.StateDead),
		},
		{
			name:  "MarkFailed по canceled — 0 строк",
			state: string(queue.StateCanceled),
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkFailed(ctx, tx, id, worker, "новый сбой", time.Now())
			},
			wantRows:  0,
			wantState: string(queue.StateCanceled),
		},
		{
			name:      "MarkCanceled по dead — no-op",
			state:     string(queue.StateDead),
			mark:      func(tx queue.Tx, id, worker string) (int64, error) { return repo.MarkCanceled(ctx, tx, id) },
			wantRows:  0,
			wantState: string(queue.StateDead),
		},
		{
			name:      "MarkDone по processing — контроль: запись проходит",
			state:     string(queue.StateProcessing),
			mark:      func(tx queue.Tx, id, worker string) (int64, error) { return repo.MarkDone(ctx, tx, id, worker) },
			wantRows:  1,
			wantState: string(queue.StateDone),
			// успех не тратит попытку и стирает причину прошлого сбоя
			wantAttempts:      attempts(2),
			wantLastErrorNil:  true,
			wantAvailableSame: true,
		},
		{
			name:      "MarkCanceled по processing — контроль: отмена в полёте проходит",
			state:     string(queue.StateProcessing),
			mark:      func(tx queue.Tx, id, worker string) (int64, error) { return repo.MarkCanceled(ctx, tx, id) },
			wantRows:  1,
			wantState: string(queue.StateCanceled),
			// отмена не тратит попытку и не назначает повтор
			wantAttempts:      attempts(2),
			wantAvailableSame: true,
		},
		{
			name:  "MarkFailed по processing — контроль: failed с отсрочкой и причиной",
			state: string(queue.StateProcessing),
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkFailed(ctx, tx, id, worker, "внешний сервис недоступен", time.Now().Add(retryDelay))
			},
			wantRows:  1,
			wantState: string(queue.StateFailed),
			// сбой не тратит и не возвращает попытку: счётчик остаётся
			// таким, каким его сделало взятие (retry/spec.md)
			wantAttempts:    attempts(2),
			wantCause:       "внешний сервис недоступен",
			wantAvailableIn: retryDelay,
		},
		{
			name:  "MarkFailedPreservingAttempt по processing — контроль: попытка возвращается",
			state: string(queue.StateProcessing),
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkFailedPreservingAttempt(ctx, tx, id, worker, "валидация не прошла", time.Now().Add(retryDelay))
			},
			wantRows:  1,
			wantState: string(queue.StateFailed),
			// взятие уже списало попытку (D5), поэтому запись без расхода
			// возвращает её: 2 -> 1. Без attempts - 1 в SQL здесь было бы 2.
			wantAttempts:    attempts(1),
			wantCause:       "валидация не прошла",
			wantAvailableIn: retryDelay,
		},
		{
			name:  "MarkDead по processing — контроль: dead, а не failed",
			state: string(queue.StateProcessing),
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkDead(ctx, tx, id, worker, "попытки исчерпаны")
			},
			wantRows:  1,
			wantState: string(queue.StateDead),
			// смерть не тратит попытку и не назначает повтор
			wantAttempts:      attempts(2),
			wantCause:         "попытки исчерпаны",
			wantAvailableSame: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id := insertJob(t, tc.state)
			before := readJob(t, id)

			// Владелец аренды берётся из строки, а не из головы: guard
			// сравнивает locked_by = worker, и подставленное "своё" значение
			// проверяло бы не то. У незанятых строк владельца нет — пустая
			// строка честно передаёт это в порт.
			var worker string
			if before.lockedBy != nil {
				worker = *before.lockedBy
			}

			tx, err := repo.Begin(ctx)
			require.NoError(t, err, "транзакция записи исхода")
			defer func() { _ = tx.Rollback(ctx) }()

			calledAt := time.Now()
			n, err := tc.mark(tx, id, worker)
			require.NoError(t, err, "guard — это 0 строк, а не ошибка")
			assert.Equal(t, tc.wantRows, n, "число затронутых строк")
			require.NoError(t, tx.Commit(ctx))

			after := readJob(t, id)
			assert.Equal(t, tc.wantState, after.state, "состояние после записи")

			if tc.wantRows == 0 {
				// 0 строк — строка осталась нетронутой целиком.
				assert.Equal(t, before.attempts, after.attempts, "счётчик попыток не меняется")
				assert.Equal(t, before.lastError, after.lastError, "причина сбоя не перезаписывается")
				assert.True(t, before.availableAt.Equal(after.availableAt), "available_at не меняется")
				assert.True(t, before.updatedAt.Equal(after.updatedAt), "updated_at не меняется: строку никто не трогал")
				assert.Equal(t, before.lockedBy, after.lockedBy, "владелец аренды не меняется")
				return
			}

			// Положительный контроль: guard не блокирует запись, и строка
			// изменилась ровно так, как обещает контракт исхода, — а не
			// только по счётчику затронутых строк.
			if tc.wantAttempts != nil {
				assert.Equal(t, *tc.wantAttempts, after.attempts, "число попыток после записи")
			}
			if tc.wantCause != "" {
				require.NotNil(t, after.lastError, "причина сбоя записана")
				assert.Contains(t, *after.lastError, tc.wantCause, "last_error после записи")
			}
			if tc.wantLastErrorNil {
				assert.Nil(t, after.lastError, "успех очищает причину прошлого сбоя")
			}
			if tc.wantAvailableIn != 0 {
				assert.True(t, after.availableAt.After(time.Now()),
					"available_at сдвинут в будущее, получено %s", after.availableAt)
				assert.WithinDuration(t, calledAt.Add(tc.wantAvailableIn), after.availableAt, clockWindow,
					"available_at = момент вызова + отсрочка")
			}
			if tc.wantAvailableSame {
				assert.True(t, before.availableAt.Equal(after.availableAt),
					"available_at не меняется: %s против %s", before.availableAt, after.availableAt)
			}
		})
	}
}

// TestClaimContract — контракт взятия задания (claim/spec.md,
// retry/spec.md, job-lifecycle/spec.md) на подготовленных строках: тема,
// время готовности, состояния-кандидаты, порядок выборки, счётчик
// попыток, аренда, limit и FOR NO KEY UPDATE SKIP LOCKED. Тест
// репозиторный, а не e2e: строки кладутся в таблицу напрямую, взятие идёт
// через Repository.Claim, поэтому любая правка claimSQL, ломающая договор
// (снятый фильтр по теме, забытый available_at, ORDER BY не по тем полям,
// отсутствие attempts + 1 или SKIP LOCKED), видна здесь.
func TestClaimContract(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const (
		// lease — срок аренды, который запрашивают все взятия теста.
		lease = 90 * time.Second
		// worker — идентификатор воркера, попадающий в locked_by.
		worker = "w-claim"
		// clockWindow — допуск на разницу между временем Go и базы.
		clockWindow = 5 * time.Second
	)

	// resetTopic убирает строки темы: `go test -count=N` повторяет тест в
	// той же схеме, и строки прошлого прогона исказили бы выборку.
	resetTopic := func(t *testing.T, topic queue.Topic) {
		t.Helper()
		_, err := pool.Exec(ctx, `DELETE FROM job WHERE topic = $1`, string(topic))
		require.NoError(t, err)
	}

	// putJob кладёт задание и отдаёт его id. Пустой id — сгенерировать;
	// явный нужен кейсу порядка при равном available_at.
	putJob := func(t *testing.T, id string, topic queue.Topic, state queue.State,
		availableAt time.Time, attempts int) string {

		t.Helper()
		var idArg any
		if id != "" {
			idArg = id
		}
		var gotID string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, attempts, available_at)
			 VALUES (COALESCE($1::uuid, gen_random_uuid()), $2, '{}'::jsonb, $3, $4, $5)
			 RETURNING id::text`,
			idArg, string(topic), string(state), attempts, availableAt,
		).Scan(&gotID))
		return gotID
	}

	// claim зовёт взятие в собственной транзакции и фиксирует её: аренда
	// видна снаружи только после Commit.
	claim := func(t *testing.T, topic queue.Topic, limit int) []queue.Claim {
		t.Helper()
		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция взятия")
		defer func() { _ = tx.Rollback(ctx) }()

		claims, err := repo.Claim(ctx, tx, topic, worker, lease, limit)
		require.NoError(t, err, "взятие — не ошибка")
		require.NoError(t, tx.Commit(ctx))
		return claims
	}

	// Одиночные кейсы: одна строка в таблице, одно взятие.
	tests := []struct {
		name string
		// topic — тема задания в таблице.
		topic queue.Topic
		// claimTopic — тема, по которой запрашивается взятие.
		claimTopic queue.Topic
		// state — состояние задания в таблице.
		state queue.State
		// availableIn — сдвиг available_at от момента постановки.
		availableIn time.Duration
		// attempts — счётчик попыток до взятия.
		attempts int
		// wantTaken — попадает ли задание в выборку.
		wantTaken bool
		// wantAttempt — ожидаемый номер состоявшейся попытки.
		wantAttempt int
	}{
		{
			name:        "задание другой темы не берётся",
			topic:       "t-claim-other",
			claimTopic:  "t-claim-mine",
			state:       queue.StatePending,
			availableIn: -time.Minute,
			wantTaken:   false,
		},
		{
			name:        "задание своей темы берётся — контроль к кейсу выше",
			topic:       "t-claim-mine",
			claimTopic:  "t-claim-mine",
			state:       queue.StatePending,
			availableIn: -time.Minute,
			wantTaken:   true,
			wantAttempt: 1,
		},
		{
			name:        "отложенное задание не берётся раньше срока",
			topic:       "t-claim-future",
			claimTopic:  "t-claim-future",
			state:       queue.StatePending,
			availableIn: time.Hour,
			wantTaken:   false,
		},
		{
			name:        "задание в failed берётся — сердцевина контракта повтора",
			topic:       "t-claim-failed",
			claimTopic:  "t-claim-failed",
			state:       queue.StateFailed,
			availableIn: -time.Minute,
			attempts:    3,
			wantTaken:   true,
			wantAttempt: 4,
		},
		{
			name:        "задание в failed до срока не берётся",
			topic:       "t-claim-failed-future",
			claimTopic:  "t-claim-failed-future",
			state:       queue.StateFailed,
			availableIn: time.Hour,
			attempts:    1,
			wantTaken:   false,
		},
		{
			name:        "задание в done не берётся",
			topic:       "t-claim-done",
			claimTopic:  "t-claim-done",
			state:       queue.StateDone,
			availableIn: -time.Minute,
			wantTaken:   false,
		},
		{
			name:        "задание в dead не берётся",
			topic:       "t-claim-dead",
			claimTopic:  "t-claim-dead",
			state:       queue.StateDead,
			availableIn: -time.Minute,
			wantTaken:   false,
		},
		{
			name:        "задание в canceled не берётся",
			topic:       "t-claim-canceled",
			claimTopic:  "t-claim-canceled",
			state:       queue.StateCanceled,
			availableIn: -time.Minute,
			wantTaken:   false,
		},
		{
			name:        "взятие увеличивает счётчик попыток на единицу",
			topic:       "t-claim-attempts",
			claimTopic:  "t-claim-attempts",
			state:       queue.StatePending,
			availableIn: -time.Minute,
			attempts:    4,
			wantTaken:   true,
			wantAttempt: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetTopic(t, tc.topic)
			if tc.claimTopic != tc.topic {
				resetTopic(t, tc.claimTopic)
			}
			id := putJob(t, "", tc.topic, tc.state, time.Now().Add(tc.availableIn), tc.attempts)

			claims := claim(t, tc.claimTopic, 1)

			if !tc.wantTaken {
				require.Empty(t, claims, "задание не должно попадать в выборку взятия")

				var (
					state    string
					attempts int
				)
				require.NoError(t, pool.QueryRow(ctx,
					`SELECT state, attempts FROM job WHERE id = $1`, id).Scan(&state, &attempts))
				assert.Equal(t, string(tc.state), state, "несостоявшееся взятие не меняет состояние")
				assert.Equal(t, tc.attempts, attempts, "несостоявшееся взятие не тратит попытку")
				return
			}

			require.Len(t, claims, 1, "взято ровно одно задание")
			assert.Equal(t, id, claims[0].ID, "взято ожидаемое задание")
			assert.Equal(t, tc.claimTopic, claims[0].Topic, "тема взятого задания")
			assert.Equal(t, tc.wantAttempt, claims[0].Attempt,
				"номер попытки в результате — уже списанная попытка")

			var (
				state    string
				attempts int
			)
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT state, attempts FROM job WHERE id = $1`, id).Scan(&state, &attempts))
			assert.Equal(t, string(queue.StateProcessing), state, "взятое задание в processing")
			assert.Equal(t, tc.wantAttempt, attempts, "счётчик попыток в строке")
		})
	}

	t.Run("отложенное задание берётся после наступления срока", func(t *testing.T) {
		const topic = "t-claim-delayed"
		resetTopic(t, topic)
		id := putJob(t, "", topic, queue.StatePending, time.Now().Add(time.Hour), 0)

		assert.Empty(t, claim(t, topic, 1), "срок не наступил — задание не берётся")

		// Срок наступил: ждать час тест не может, поэтому время готовности
		// сдвигается в прошлое напрямую в таблице.
		_, err := pool.Exec(ctx, `UPDATE job SET available_at = now() - interval '1 second' WHERE id = $1`, id)
		require.NoError(t, err)

		claims := claim(t, topic, 1)
		require.Len(t, claims, 1, "после срока задание доступно для взятия")
		assert.Equal(t, id, claims[0].ID)
	})

	t.Run("порядок выборки — available_at, затем id", func(t *testing.T) {
		const topic = "t-claim-order"
		resetTopic(t, topic)

		// Явные id и одно и то же время готовности: порядок при равенстве
		// задаёт id, и без контроля значений тест зависел бы от генератора
		// UUID. «Позднее» задание получает МЕНЬШИЙ id — так видно, что
		// первичен available_at, а не id.
		at := time.Now().Add(-time.Hour)
		earlyA := putJob(t, "00000000-0000-0000-0000-000000000002", topic, queue.StatePending, at, 0)
		earlyB := putJob(t, "00000000-0000-0000-0000-000000000003", topic, queue.StatePending, at, 0)
		late := putJob(t, "00000000-0000-0000-0000-000000000001", topic, queue.StatePending, at.Add(time.Minute), 0)

		claims := claim(t, topic, 3)
		require.Len(t, claims, 3, "взяты все готовые задания темы")

		got := make([]string, 0, len(claims))
		for _, c := range claims {
			got = append(got, c.ID)
		}
		assert.Equal(t, []string{earlyA, earlyB, late}, got,
			"сначала раннее время готовности, при равенстве — меньший id")
	})

	t.Run("limit ограничивает выборку", func(t *testing.T) {
		const topic = "t-claim-limit"
		resetTopic(t, topic)

		at := time.Now().Add(-time.Hour)
		for i := 0; i < 3; i++ {
			putJob(t, "", topic, queue.StatePending, at.Add(time.Duration(i)*time.Second), 0)
		}

		first := claim(t, topic, 2)
		require.Len(t, first, 2, "limit 2 из трёх готовых")

		second := claim(t, topic, 2)
		require.Len(t, second, 1, "третье задание осталось доступным")

		seen := make(map[string]bool, 3)
		for _, c := range append(first, second...) {
			require.False(t, seen[c.ID], "задание %s взято дважды", c.ID)
			seen[c.ID] = true
		}
		assert.Len(t, seen, 3, "три задания взяты ровно по одному разу")
	})

	t.Run("limit меньше 1 — пустой результат без ошибки", func(t *testing.T) {
		const topic = "t-claim-limit-zero"
		resetTopic(t, topic)
		putJob(t, "", topic, queue.StatePending, time.Now().Add(-time.Minute), 0)

		for _, limit := range []int{0, -1} {
			tx, err := repo.Begin(ctx)
			require.NoError(t, err, "транзакция взятия")

			claims, err := repo.Claim(ctx, tx, topic, worker, lease, limit)
			require.NoError(t, err, "limit %d — «нечего брать», а не ошибка", limit)
			assert.Empty(t, claims, "limit %d даёт пустой результат", limit)
			require.NoError(t, tx.Commit(ctx))
		}

		// Контроль: задание готово, его отсекает только limit.
		require.Len(t, claim(t, topic, 1), 1, "с limit 1 задание берётся")
	})

	t.Run("взятие проставляет воркера и срок аренды одновременно", func(t *testing.T) {
		const topic = "t-claim-lease"
		resetTopic(t, topic)
		id := putJob(t, "", topic, queue.StatePending, time.Now().Add(-time.Minute), 0)

		before := time.Now()
		claims := claim(t, topic, 1)
		require.Len(t, claims, 1)

		assert.Equal(t, worker, claims[0].Lease.Worker, "locked_by — идентификатор воркера")
		assert.WithinDuration(t, before.Add(lease), claims[0].Lease.Until, clockWindow,
			"locked_until = now + lease")

		var (
			lockedBy *string
			until    *time.Time
		)
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT locked_by, locked_until FROM job WHERE id = $1`, id).Scan(&lockedBy, &until))
		require.NotNil(t, lockedBy, "processing всегда с арендой: воркер заполнен")
		require.NotNil(t, until, "processing всегда с арендой: срок заполнен")
		assert.Equal(t, worker, *lockedBy)
		assert.WithinDuration(t, before.Add(lease), *until, clockWindow, "locked_until = now + lease")
	})

	t.Run("занятая строка не блокирует второе взятие", func(t *testing.T) {
		const topic = "t-claim-skip-locked"
		resetTopic(t, topic)

		at := time.Now().Add(-time.Minute)
		held := putJob(t, "", topic, queue.StatePending, at, 0)
		free := putJob(t, "", topic, queue.StatePending, at.Add(time.Second), 0)

		// Первая транзакция держит строку и не фиксируется: её строка
		// заблокирована до конца транзакции.
		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция первого воркера")
		defer func() { _ = tx.Rollback(ctx) }()

		first, err := repo.Claim(ctx, tx, topic, "w-held", lease, 1)
		require.NoError(t, err)
		require.Len(t, first, 1)
		require.Equal(t, held, first[0].ID, "первый воркер взял первое по порядку задание")

		// Второе взятие идёт в отдельной транзакции и с ограничением по
		// времени: без SKIP LOCKED оно бы ждало освобождения занятой строки
		// и упёрлось в дедлайн вместо того, чтобы взять свободное задание.
		ctxSecond, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		tx2, err := repo.Begin(ctxSecond)
		require.NoError(t, err, "транзакция второго воркера")
		defer func() { _ = tx2.Rollback(ctxSecond) }()

		second, err := repo.Claim(ctxSecond, tx2, topic, "w-free", lease, 1)
		require.NoError(t, err, "взятие не ждёт освобождения чужой строки")
		require.Len(t, second, 1, "второй воркер получил свободное задание")
		assert.Equal(t, free, second[0].ID, "занятая строка пропущена, а не отдана повторно")
		require.NoError(t, tx2.Commit(ctx))
	})
}

// TestEnqueueTxRollbackLeavesNoJob — постановка атомарна с записью
// производителя (job-lifecycle/spec.md «Постановка задания атомарна с
// записью производителя»): откат транзакции не оставляет задания,
// фиксация — оставляет его в очереди и доступным для взятия. Видимость
// строки внутри незафиксированной транзакции проверяется отдельно: иначе
// «после отката строки нет» доказывало бы и то, что EnqueueTx не пишет
// ничего вовсе.
func TestEnqueueTxRollbackLeavesNoJob(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const topic = "t-enqueue-tx"

	// reset убирает строки темы: `go test -count=N` повторяет тест в той же
	// схеме.
	reset := func(t *testing.T) {
		t.Helper()
		_, err := pool.Exec(ctx, `DELETE FROM job WHERE topic = $1`, topic)
		require.NoError(t, err)
	}

	// countJobs считает задания темы в таблице.
	countJobs := func(t *testing.T) int {
		t.Helper()
		var n int
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1`, topic).Scan(&n))
		return n
	}

	// job — задание производителя.
	job := queue.Job{Topic: topic, Payload: queue.Payload(`{"письмо":1}`)}

	t.Run("откат транзакции производителя не оставляет задания", func(t *testing.T) {
		reset(t)

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция производителя")
		_, _, err = repo.EnqueueTx(ctx, tx, job)
		require.NoError(t, err)

		// Внутри транзакции задание видно: иначе проверять откат было бы
		// нечего.
		var inTx int
		require.NoError(t, tx.QueryRow(ctx,
			`SELECT COUNT(*)::int FROM job WHERE topic = $1`, topic).Scan(&inTx))
		require.Equal(t, 1, inTx, "внутри транзакции задание записано")

		require.NoError(t, tx.Rollback(ctx))
		assert.Equal(t, 0, countJobs(t), "после отката задания в очереди нет")
	})

	t.Run("фиксация транзакции гарантирует задание в очереди", func(t *testing.T) {
		reset(t)

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция производителя")
		_, _, err = repo.EnqueueTx(ctx, tx, job)
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))

		require.Equal(t, 1, countJobs(t), "после фиксации задание в очереди")

		var state string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state FROM job WHERE topic = $1`, topic).Scan(&state))
		assert.Equal(t, string(queue.StatePending), state, "новое задание ждёт взятия")

		// Доступность для взятия — часть контракта фиксации.
		claimTx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция взятия")
		defer func() { _ = claimTx.Rollback(ctx) }()

		claims, err := repo.Claim(ctx, claimTx, queue.Topic(topic), "w-enqueue", time.Minute, 1)
		require.NoError(t, err)
		require.Len(t, claims, 1, "зафиксированное задание берётся")
		require.NoError(t, claimTx.Commit(ctx))
	})
}

// TestOutcomeOwnershipGuard — guard владения в SQL записи исхода (D13):
// исход пишется только тому заданию, аренда которого всё ещё у этого
// воркера (state = 'processing' AND locked_by = worker). Снятие условия
// locked_by = $N из markDoneSQL/markFailedSQL/
// markFailedPreservingAttemptSQL/markDeadSQL роняет этот тест: воркер,
// потерявший аренду, дописал бы исход чужого задания — и задание,
// взятое вторым воркером, закрылось бы исходом первого.
//
// MarkCanceled — отдельный случай: отменяет не владелец аренды (D14),
// поэтому владения он не требует и его сигнатура воркера не несёт.
func TestOutcomeOwnershipGuard(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	const (
		// owner — воркер, взявший задание и держащий аренду.
		owner = "w-owner"
		// stranger — другой воркер: его запись исхода обязана пройти мимо
		// строки.
		stranger = "w-stranger"
	)

	// attempts возвращает указатель на ожидаемое число попыток: nil в
	// таблице означает «счётчик не проверяется».
	attempts := func(n int) *int { return &n }

	// insertOwned кладёт взятое задание: processing, аренда у owner,
	// попытка уже списана (D5).
	insertOwned := func(t *testing.T) string {
		t.Helper()

		var id string
		require.NoError(t, pool.QueryRow(ctx,
			`INSERT INTO job (id, topic, payload, state, attempts, last_error,
			                  available_at, locked_by, locked_until)
			 VALUES (gen_random_uuid(), 't-ownership', '{}'::jsonb, 'processing', 2,
			         'причина прошлого сбоя', now() - interval '1 hour',
			         $1, now() + interval '1 hour')
			 RETURNING id::text`, owner,
		).Scan(&id))
		return id
	}

	// readJob читает строку целиком.
	readJob := func(t *testing.T, id string) jobRow {
		t.Helper()

		var row jobRow
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, attempts, last_error, available_at, updated_at, locked_by
			   FROM job WHERE id = $1`, id,
		).Scan(&row.state, &row.attempts, &row.lastError, &row.availableAt, &row.updatedAt, &row.lockedBy))
		return row
	}

	// assertUntouched сравнивает строку с исходным снимком: чужая запись
	// не меняет ничего, включая updated_at и владельца аренды.
	assertUntouched := func(t *testing.T, before, after jobRow) {
		t.Helper()
		assert.Equal(t, before.state, after.state, "состояние не меняется")
		assert.Equal(t, before.attempts, after.attempts, "счётчик попыток не меняется")
		assert.Equal(t, before.lastError, after.lastError, "причина сбоя не перезаписывается")
		assert.True(t, before.availableAt.Equal(after.availableAt), "available_at не меняется")
		assert.True(t, before.updatedAt.Equal(after.updatedAt), "updated_at не меняется: строку никто не трогал")
		assert.Equal(t, before.lockedBy, after.lockedBy, "владелец аренды не меняется")
	}

	tests := []struct {
		name string
		// mark — запись исхода; воркер приходит параметром.
		mark func(tx queue.Tx, id, worker string) (int64, error)
		// wantState — состояние после записи владельцем аренды.
		wantState string
		// wantAttempts — число попыток после записи владельцем
		// (nil — счётчик не проверяется).
		wantAttempts *int
		// wantCause — подстрока last_error после записи владельцем ("" —
		// не проверяется).
		wantCause string
	}{
		{
			name: "MarkDone",
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkDone(ctx, tx, id, worker)
			},
			wantState:    string(queue.StateDone),
			wantAttempts: attempts(2),
		},
		{
			name: "MarkFailed",
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkFailed(ctx, tx, id, worker, "сбой от владельца", time.Now().Add(time.Hour))
			},
			wantState:    string(queue.StateFailed),
			wantAttempts: attempts(2),
			wantCause:    "сбой от владельца",
		},
		{
			name: "MarkFailedPreservingAttempt",
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkFailedPreservingAttempt(ctx, tx, id, worker, "сбой от владельца", time.Now().Add(time.Hour))
			},
			wantState:    string(queue.StateFailed),
			wantAttempts: attempts(1),
			wantCause:    "сбой от владельца",
		},
		{
			name: "MarkDead",
			mark: func(tx queue.Tx, id, worker string) (int64, error) {
				return repo.MarkDead(ctx, tx, id, worker, "сбой от владельца")
			},
			wantState:    string(queue.StateDead),
			wantAttempts: attempts(2),
			wantCause:    "сбой от владельца",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("чужой воркер — 0 строк, строка не изменилась", func(t *testing.T) {
				id := insertOwned(t)
				before := readJob(t, id)

				tx, err := repo.Begin(ctx)
				require.NoError(t, err, "транзакция записи исхода")
				defer func() { _ = tx.Rollback(ctx) }()

				n, err := tc.mark(tx, id, stranger)
				require.NoError(t, err, "потерянная аренда — 0 строк, а не ошибка")
				assert.Equal(t, int64(0), n, "исход чужому воркеру не пишется")
				require.NoError(t, tx.Commit(ctx))

				after := readJob(t, id)
				assertUntouched(t, before, after)
				assert.Equal(t, string(queue.StateProcessing), after.state,
					"задание осталось в обработке: закрывать его чужой записи нельзя")
			})

			t.Run("владелец аренды — 1 строка, исход записан", func(t *testing.T) {
				id := insertOwned(t)
				before := readJob(t, id)

				tx, err := repo.Begin(ctx)
				require.NoError(t, err, "транзакция записи исхода")
				defer func() { _ = tx.Rollback(ctx) }()

				n, err := tc.mark(tx, id, owner)
				require.NoError(t, err, "запись владельца — не ошибка")
				assert.Equal(t, int64(1), n, "исход владельцу аренды пишется")
				require.NoError(t, tx.Commit(ctx))

				after := readJob(t, id)
				assert.Equal(t, tc.wantState, after.state, "состояние после записи владельцем")
				if tc.wantAttempts != nil {
					assert.Equal(t, *tc.wantAttempts, after.attempts, "число попыток после записи")
				}
				if tc.wantCause != "" {
					require.NotNil(t, after.lastError, "причина сбоя записана")
					assert.Contains(t, *after.lastError, tc.wantCause, "last_error после записи")
				}
				assert.False(t, before.updatedAt.Equal(after.updatedAt),
					"запись владельца обновила строку")
			})
		})
	}

	t.Run("пустой воркер не совпадает ни с чьей арендой", func(t *testing.T) {
		// Пустое значение — граница guard-а: у взятого задания locked_by
		// никогда не NULL, поэтому locked_by = '' не совпадёт ни с чем (а
		// NULL-семантика SQL не даст ложного совпадения и подавно).
		id := insertOwned(t)
		before := readJob(t, id)

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция записи исхода")
		defer func() { _ = tx.Rollback(ctx) }()

		n, err := repo.MarkDone(ctx, tx, id, "")
		require.NoError(t, err, "пустой воркер — 0 строк, а не ошибка")
		assert.Equal(t, int64(0), n, "пустой воркер исход не пишет")
		require.NoError(t, tx.Commit(ctx))

		after := readJob(t, id)
		assertUntouched(t, before, after)
	})

	t.Run("MarkCanceled — владения не требует", func(t *testing.T) {
		// Отменяет не владелец аренды (D14): markCanceledSQL идёт без
		// locked_by, поэтому отмена проходит независимо от того, кто взял
		// задание, — и именно поэтому у метода нет параметра воркера.
		id := insertOwned(t)

		tx, err := repo.Begin(ctx)
		require.NoError(t, err, "транзакция отмены")
		defer func() { _ = tx.Rollback(ctx) }()

		n, err := repo.MarkCanceled(ctx, tx, id)
		require.NoError(t, err, "отмена — не ошибка")
		assert.Equal(t, int64(1), n, "отмена не спрашивает владения арендой")
		require.NoError(t, tx.Commit(ctx))

		after := readJob(t, id)
		assert.Equal(t, string(queue.StateCanceled), after.state, "задание отменено")
	})
}

// TestEnqueueIdempotency — идемпотентность постановки через публичный
// путь (job-lifecycle/spec.md «Идемпотентность постановки по ключу»).
// Отдельно от сырых INSERT в TestJobIdempotencyUnique: там проверяется
// индекс, а здесь — конверсия ключа в EnqueueTx. Мутация «пустой ключ
// считать настоящим ключом» (pkg/postgres/enqueue.go) проходит
// все прочие тесты и тихо схлопывает все задания темы без ключа в одно;
// поэтому пустой ключ обязан лежать в строке как NULL, а ключ из
// пробелов — как настоящий ключ, который дедуплицирует.
func TestEnqueueIdempotency(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := New(pool)

	// key возвращает указатель на ожидаемый ключ: nil в таблице означает
	// «ключ не проверяется по значению».
	key := func(s string) *string { return &s }

	tests := []struct {
		name string
		// topic — тема кейса: своя, чтобы строки кейсов не мешали друг
		// другу.
		topic string
		// enqueue — последовательность постановок.
		enqueue func(t *testing.T, topic queue.Topic)
		// wantRows — сколько заданий темы обязано оказаться в таблице.
		wantRows int
		// wantKey — ожидаемое значение idempotency_key в единственной
		// строке.
		wantKey *string
		// wantKeyNull — ключ в строках NULL.
		wantKeyNull bool
	}{
		{
			name:  "два Enqueue без ключа — два задания",
			topic: "t-enqueue-nokey",
			enqueue: func(t *testing.T, topic queue.Topic) {
				for i := 0; i < 2; i++ {
					_, _, err := repo.Enqueue(ctx, queue.Job{
						Topic:   topic,
						Payload: queue.Payload(`{"письмо":1}`),
					})
					require.NoError(t, err, "постановка без ключа %d", i+1)
				}
			},
			wantRows:    2,
			wantKeyNull: true,
		},
		{
			name:  "Enqueue с пустым ключом — NULL в строке, повтор с пустым — два задания",
			topic: "t-enqueue-empty-key",
			enqueue: func(t *testing.T, topic queue.Topic) {
				for i := 0; i < 2; i++ {
					_, _, err := repo.Enqueue(ctx, queue.Job{
						Topic:          topic,
						Payload:        queue.Payload(`{"письмо":1}`),
						IdempotencyKey: "",
					})
					require.NoError(t, err, "постановка с пустым ключом %d", i+1)
				}
			},
			wantRows:    2,
			wantKeyNull: true,
		},
		{
			name:  "ключ из пробелов — настоящий ключ, дедуплицирует",
			topic: "t-enqueue-spaces-key",
			enqueue: func(t *testing.T, topic queue.Topic) {
				for i := 0; i < 2; i++ {
					_, _, err := repo.Enqueue(ctx, queue.Job{
						Topic:          topic,
						Payload:        queue.Payload(`{"письмо":1}`),
						IdempotencyKey: "   ",
					})
					require.NoError(t, err, "постановка с ключом из пробелов %d", i+1)
				}
			},
			wantRows: 1,
			wantKey:  key("   "),
		},
		{
			name:  "EnqueueTx с ключом — дедуплицирует, как Enqueue",
			topic: "t-enqueue-tx-key",
			enqueue: func(t *testing.T, topic queue.Topic) {
				job := queue.Job{
					Topic:          topic,
					Payload:        queue.Payload(`{"письмо":1}`),
					IdempotencyKey: "k-tx",
				}
				_, _, err := repo.Enqueue(ctx, job)
				require.NoError(t, err, "первая постановка через пул")

				tx, err := repo.Begin(ctx)
				require.NoError(t, err, "транзакция производителя")
				defer func() { _ = tx.Rollback(ctx) }()

				_, _, err = repo.EnqueueTx(ctx, tx, job)
				require.NoError(t, err,
					"повтор с тем же ключом в транзакции — не ошибка")
				require.NoError(t, tx.Commit(ctx))
			},
			wantRows: 1,
			wantKey:  key("k-tx"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Строки темы убираются: `go test -count=N` повторяет тест в той
			// же схеме, и строки прошлого прогона исказили бы счёт.
			_, err := pool.Exec(ctx, `DELETE FROM job WHERE topic = $1`, tc.topic)
			require.NoError(t, err)

			tc.enqueue(t, queue.Topic(tc.topic))

			var got int
			require.NoError(t, pool.QueryRow(ctx,
				`SELECT COUNT(*)::int FROM job WHERE topic = $1`, tc.topic).Scan(&got))
			require.Equal(t, tc.wantRows, got, "число заданий темы в очереди")

			rows, err := pool.Query(ctx,
				`SELECT idempotency_key FROM job WHERE topic = $1 ORDER BY idempotency_key NULLS LAST`, tc.topic)
			require.NoError(t, err)
			defer rows.Close()

			var keys []*string
			for rows.Next() {
				var k *string
				require.NoError(t, rows.Scan(&k))
				keys = append(keys, k)
			}
			require.NoError(t, rows.Err(), "обход ключей темы")

			if tc.wantKeyNull {
				for i, k := range keys {
					assert.Nil(t, k, "строка %d: пустой ключ обязан лежать в базе как NULL", i+1)
				}
			}
			if tc.wantKey != nil {
				require.Len(t, keys, 1, "ключ проверяется по значению в единственной строке")
				require.NotNil(t, keys[0], "настоящий ключ не должен стать NULL")
				assert.Equal(t, *tc.wantKey, *keys[0], "ключ сохранён как есть, без обрезки")
			}
		})
	}
}
