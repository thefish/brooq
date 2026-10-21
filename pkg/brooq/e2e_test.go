// Package brooq — e2e-тесты движка на настоящей PostgreSQL: изолированная
// схема, миграции каталога migrations/ через goose, реальное
// хранилище pkg/postgres поверх пула. Движок здесь не подменяется
// фейком: проверяется конечный сценарий «производитель положил задание →
// воркер взял его → исход записан и виден».
//
// Без TEST_DATABASE_URL тесты пропускаются (t.Skip), поэтому `make test`
// без базы зелёный, но ничего не доказывает про движок; с базой —
// `TEST_DATABASE_URL=… go test ./pkg/brooq/`.
//
// Миграции НЕ встраиваются в бинарник через //go:embed: goose читает
// каталог с диска — тот же, что накатывает `make migrate-up`.
// Каталог задаётся MIGRATIONS_DIR, по умолчанию ../../migrations
// относительно каталога пакета (go test запускает бинарник из него).
// Базу поднимает `make test-db-up` (docker), а не сам тест.
package brooq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/db"
	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

// Порядок проверки: настоящее хранилище обязано удовлетворять порту
// движка без адаптера — иначе e2e собирал бы движок поверх чего-то
// другого, чем продакшен.
var _ Repo = (*postgres.Repository)(nil)

// e2eMigrationsDir — каталог migrations/ в корне модуля. Тестовый
// бинарник запускается с рабочим каталогом пакета, поэтому путь
// поднимается на два уровня. Переопределяется MIGRATIONS_DIR.
const e2eMigrationsDir = "../../migrations"

// e2eTimeout — сколько ждём исхода задания: с большим запасом, чтобы
// медленная машина давала сообщение о провале, а не флак.
const e2eTimeout = 10 * time.Second

// e2ePollInterval — период опроса состояния задания в базе.
const e2ePollInterval = 20 * time.Millisecond

// e2eTestPool — пул соединений на тестовую схему (инициализируется в TestMain).
var e2eTestPool *pgxpool.Pool

// e2eTopicSeq — счётчик тем: схема одна на прогон пакета, а `go test
// -count=N` повторяет тест в ней же. Уникальная тема на каждый вызов
// оставляет прошлому прогону его собственные строки и не даёт тесту
// «пройти» на уже готовом задании.
var e2eTopicSeq atomic.Int64

// e2eTopic возвращает тему, уникальную в пределах прогона пакета.
func e2eTopic(t *testing.T, base string) queue.Topic {
	t.Helper()
	return queue.Topic(fmt.Sprintf("%s-%d", base, e2eTopicSeq.Add(1)))
}

// countingHandler — обработчик, считающий вызовы: рейс взятия обязан дать
// ровно один вызов, а не два. Счётчик под мьютексом, потому что вызывать
// его могут разные воркеры (в том числе в разных движках).
type countingHandler struct {
	mu    sync.Mutex
	calls int
}

// Handle инкрементирует счётчик и сообщает успех.
func (h *countingHandler) Handle(_ context.Context, _ queue.Claim) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	return nil
}

// count возвращает число состоявшихся вызовов.
func (h *countingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// blockingHandler — обработчик, который держит попытку и уважает ctx: он
// ждёт либо отмены контекста попытки (тогда возвращает ctx.Err()), либо
// истечения work (тогда возвращает nil). Так проверяются отмена в полёте
// (задание гасит отмена, а не таймаут попытки) и продление аренды живым
// обработчиком.
type blockingHandler struct {
	// work — сколько «работать» до успеха; 0 — работать до отмены.
	work time.Duration
	// started отдаёт взятое задание; буфер 1, поэтому повторный вызов
	// обработчика не блокируется на отправке.
	started chan queue.Claim
	// done закрывается, когда Handle вернулся.
	done chan struct{}

	mu       sync.Mutex
	calls    int
	cause    error
	doneOnce sync.Once
}

// newBlockingHandler собирает блокирующий обработчик.
func newBlockingHandler(work time.Duration) *blockingHandler {
	return &blockingHandler{
		work:    work,
		started: make(chan queue.Claim, 1),
		done:    make(chan struct{}),
	}
}

// Handle держит попытку и выходит по контексту или по истечении work.
// Паники нет и при повторном вызове: done закрывается один раз, иначе
// ошибочное второе исполнение задания уронило бы тест паникой вместо
// внятного сообщения.
func (h *blockingHandler) Handle(ctx context.Context, c queue.Claim) error {
	h.mu.Lock()
	h.calls++
	h.mu.Unlock()

	select {
	case h.started <- c:
	default:
	}

	var err error
	if h.work > 0 {
		timer := time.NewTimer(h.work)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-timer.C:
		}
	} else {
		<-ctx.Done()
		err = ctx.Err()
	}

	h.mu.Lock()
	h.cause = context.Cause(ctx)
	h.mu.Unlock()

	h.doneOnce.Do(func() { close(h.done) })
	return err
}

// count — сколько раз обработчик был вызван.
func (h *blockingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

// causeOf — причина отмены контекста попытки, которую увидел обработчик.
func (h *blockingHandler) causeOf() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cause
}

// migrationsDir возвращает каталог миграций: MIGRATIONS_DIR, если задан,
// иначе e2eMigrationsDir.
func migrationsDir() string {
	if dir := os.Getenv("MIGRATIONS_DIR"); dir != "" {
		return dir
	}
	return e2eMigrationsDir
}

// resolveMigrationsDir возвращает абсолютный путь к каталогу миграций.
// MIGRATIONS_DIR бывает относительным (например, `migrations` из
// Makefile — верный для goose, которого зовут из корня модуля): go test
// запускает бинарник пакета с рабочим каталогом этого пакета, поэтому
// сначала путь разрешается от cwd, а если такого каталога там нет — от
// корня модуля. Так и `go test ./...` в обход make находит миграции.
func resolveMigrationsDir(dir string) (string, error) {
	if abs, err := filepath.Abs(dir); err == nil && isDir(abs) {
		return abs, nil
	}
	if filepath.IsAbs(dir) {
		return "", fmt.Errorf("каталог не найден: %s", dir)
	}
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, dir), nil
}

// isDir сообщает, что path существует и является каталогом.
func isDir(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

// moduleRoot поднимается от рабочего каталога до каталога с go.mod.
func moduleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("рабочий каталог: %w", err)
	}
	for {
		if st, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil && !st.IsDir() {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("корень модуля (go.mod) не найден от %s", dir)
		}
		dir = parent
	}
}

// applyMigrations применяет к схеме каталог миграций dir через goose —
// тот же путь, что и make deploy: никакого drift между
// тестами и продакшеном. search_path задаётся параметрами DSN, чтобы
// goose писал goose_db_version в изолированную схему.
func applyMigrations(dsn, schema, dir string) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("goose: установка диалекта: %w", err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("goose: разбор DSN: %w", err)
	}
	cfg.RuntimeParams["search_path"] = schema
	// stdlib.OpenDB берёт pgx.ConnConfig целиком, включая RuntimeParams:
	// sql.Open("pgx", cfg.ConnString()) потерял бы search_path, и goose
	// накатил бы миграции в public вместо изолированной схемы.
	sqlDB := stdlib.OpenDB(*cfg)
	defer func() { _ = sqlDB.Close() }()
	if err := goose.Up(sqlDB, dir); err != nil {
		return fmt.Errorf("goose up (%s): %w", dir, err)
	}
	return nil
}

// setUpSchema создаёт изолированную схему, применяет к ней каталог
// миграций dir и возвращает пул с search_path на неё.
func setUpSchema(dsn, schema, dir string) (*pgxpool.Pool, func()) {
	ctx := context.Background()
	bootstrap, err := db.Connect(ctx, dsn, db.DefaultParams())
	if err != nil {
		fmt.Printf("не удалось подключиться к тестовой БД: %v\n", err)
		os.Exit(1)
	}
	if err := db.DropSchemaWithGuard(ctx, bootstrap, schema); err != nil {
		fmt.Printf("не удалось очистить схему %s: %v\n", schema, err)
		db.ExitWithSchemaDrop(ctx, bootstrap, schema)
	}
	if _, err := bootstrap.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		fmt.Printf("не удалось создать схему %s: %v\n", schema, err)
		db.ExitWithSchemaDrop(ctx, bootstrap, schema)
	}

	if err := applyMigrations(dsn, schema, dir); err != nil {
		fmt.Printf("не удалось применить миграции: %v\n", err)
		db.ExitWithSchemaDrop(ctx, bootstrap, schema)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		fmt.Printf("не удалось разобрать DSN: %v\n", err)
		db.ExitWithSchemaDrop(ctx, bootstrap, schema)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		fmt.Printf("не удалось создать тестовый пул: %v\n", err)
		db.ExitWithSchemaDrop(ctx, bootstrap, schema)
	}
	return pool, func() {
		if err := db.DropSchemaWithGuard(ctx, bootstrap, schema); err != nil {
			fmt.Printf("не удалось убрать схему %s: %v\n", schema, err)
		}
		bootstrap.Close()
		pool.Close()
	}
}

// TestMain настраивает изолированную схему и выполняет тесты пакета:
// без TEST_DATABASE_URL e2e-тесты сами себя пропускают.
func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		fmt.Println("TEST_DATABASE_URL не задана: e2e-тесты движка будут пропущены")
		os.Exit(m.Run())
	}

	dir, err := resolveMigrationsDir(migrationsDir())
	if err != nil {
		fmt.Printf("не удалось определить каталог миграций: %v\n", err)
		os.Exit(1)
	}
	if !isDir(dir) {
		fmt.Printf("каталог миграций не найден: %s (задайте MIGRATIONS_DIR)\n", dir)
		os.Exit(1)
	}

	var cleanup func()
	e2eTestPool, cleanup = setUpSchema(dsn, fmt.Sprintf("brooq_queue_%d", os.Getpid()), dir)

	code := m.Run()
	cleanup()
	os.Exit(code)
}

// newTestPool возвращает пул на изолированную схему; без
// TEST_DATABASE_URL — Skip.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if e2eTestPool == nil {
		t.Skip("TEST_DATABASE_URL не задана")
	}
	return e2eTestPool
}

// newEngine собирает движок поверх настоящего хранилища: один воркер и
// короткий поллинг, чтобы ожидание исхода в тесте было быстрым. Логгер
// глушится: вывод движка — не предмет проверки.
func newEngine(t *testing.T, repo *postgres.Repository, workerID string, topic queue.Topic,
	h queue.Handler) *Queue {
	t.Helper()

	return newEngineWith(t, repo, topic, h, Config{
		PollInterval: e2ePollInterval,
		Lease:        30 * time.Second,
		WorkerID:     workerID,
	})
}

// newEngineWith собирает движок поверх настоящего хранилища с явными
// настройками: короткая аренда и частый сборщик истёкших аренд нужны
// тестам отмены и heartbeat, порт метрик — тестам метрик, короткий период
// статистики — тестам gauge'ей. Настройки темы те же, что у newEngine:
// тест не должен проверять движок с другими предохранителями, чем
// обычный.
func newEngineWith(t *testing.T, repo *postgres.Repository, topic queue.Topic,
	h queue.Handler, cfg Config) *Queue {
	t.Helper()

	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = e2ePollInterval
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	q := New(repo, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	require.NoError(t, q.RegisterKind(topic, h, queue.KindOptions{
		MaxAttempts:         3,
		AttemptTimeout:      e2eTimeout,
		MaxExecutionTimeout: e2eTimeout,
		BackoffBase:         100 * time.Millisecond,
		BackoffMax:          time.Second,
	}), "регистрация темы %s", topic)
	return q
}

// waitJobState ждёт, пока задание темы придёт в состояние want: опрос с
// интервалом и внятным сообщением при провале, без «сна на всякий случай».
func waitJobState(t *testing.T, pool *pgxpool.Pool, topic queue.Topic, want queue.State) {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(e2eTimeout)
	var state string
	for {
		err := pool.QueryRow(ctx, `SELECT state FROM job WHERE topic = $1`, string(topic)).Scan(&state)
		switch {
		case err == nil && state == string(want):
			return
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			t.Fatalf("чтение состояния задания темы %s: %v", topic, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("задание темы %s не пришло в состояние %q за %s (текущее состояние %q)",
				topic, want, e2eTimeout, state)
		}
		time.Sleep(e2ePollInterval)
	}
}

// TestE2EClaimRace — рейс взятия: одно задание, два независимых движка
// на одну тему. Обещание «взятие не более одного раза в каждый момент»
// (инвариант 2) держится на FOR UPDATE SKIP LOCKED, поэтому проверяется
// двумя настоящими воркерами, а не двумя горутинами одного движка.
func TestE2EClaimRace(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "e2e-race")

	_, _, err := repo.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"n":1}`),
	})
	require.NoError(t, err, "постановка единственного задания")

	first, second := &countingHandler{}, &countingHandler{}
	q1 := newEngine(t, repo, "e2e-race-1", topic, first)
	q2 := newEngine(t, repo, "e2e-race-2", topic, second)
	// Остановка идемпотентна: Stop уже вызван, Close лишь добивает горутины.
	defer func() {
		_ = q1.Close()
		_ = q2.Close()
	}()

	// Оба движка стартуют на уже лежащее задание: рейс неизбежен.
	q1.Start()
	q2.Start()

	waitJobState(t, pool, topic, queue.StateDone)
	q1.Stop()
	q2.Stop()

	total := first.count() + second.count()
	assert.Equal(t, 1, total, "обработчик вызван ровно один раз: задание закреплено за одним воркером")
	if first.count() > 1 || second.count() > 1 {
		t.Logf("вызовы по движкам: первый=%d, второй=%d", first.count(), second.count())
	}

	var (
		state    string
		attempts int
		lockedBy *string
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT state, attempts, locked_by FROM job WHERE topic = $1`, string(topic),
	).Scan(&state, &attempts, &lockedBy))
	assert.Equal(t, string(queue.StateDone), state)
	assert.Equal(t, 1, attempts, "попытка списывается при взятии ровно один раз (D5)")
	require.NotNil(t, lockedBy, "задание в done помнит, кто его взял")
	assert.Contains(t, []string{"e2e-race-1", "e2e-race-2"}, *lockedBy)
}

// TestE2EHappyPath — критерий готовности change'а (задача 6.3):
// производитель кладёт задание в транзакции, воркер берёт его ровно один
// раз, исход записан в ту же строку и читается одним SELECT (инвариант 4).
// Дополнительно: повтор постановки с тем же ключом не создаёт второе
// задание (инвариант 8).
func TestE2EHappyPath(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "e2e-happy")
	payload := queue.Payload(`{"order":"42"}`)
	const key = "e2e-happy-key"

	// Производитель: задание фиксируется вместе с его транзакцией.
	tx, err := repo.Begin(ctx)
	require.NoError(t, err, "транзакция производителя")
	_, _, err = repo.EnqueueTx(ctx, tx, queue.Job{
		Topic:          topic,
		Payload:        payload,
		IdempotencyKey: key,
	})
	require.NoError(t, err, "постановка в транзакции производителя")
	require.NoError(t, tx.Commit(ctx), "фиксация транзакции производителя")

	// Повтор постановки с тем же ключом: не ошибка и не второе задание.
	tx, err = repo.Begin(ctx)
	require.NoError(t, err, "транзакция повторной постановки")
	_, _, err = repo.EnqueueTx(ctx, tx, queue.Job{
		Topic:          topic,
		Payload:        payload,
		IdempotencyKey: key,
	})
	require.NoError(t, err, "повтор постановки с тем же ключом не ошибка")
	require.NoError(t, tx.Commit(ctx))

	handler := &countingHandler{}
	engine := newEngine(t, repo, "e2e-happy-1", topic, handler)
	defer func() { _ = engine.Close() }()
	engine.Start()

	waitJobState(t, pool, topic, queue.StateDone)
	engine.Stop()

	// Исход читается одним SELECT из той же строки, где лежит задание.
	var (
		state       string
		attempts    int
		gotPayload  []byte
		lastError   *string
		lockedBy    *string
		lockedUntil *time.Time
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT state, attempts, payload, last_error, locked_by, locked_until
		   FROM job WHERE topic = $1`, string(topic),
	).Scan(&state, &attempts, &gotPayload, &lastError, &lockedBy, &lockedUntil))

	assert.Equal(t, string(queue.StateDone), state, "исход записан в строку задания")
	assert.Equal(t, 1, attempts, "задание взято ровно один раз")
	assert.JSONEq(t, string(payload), string(gotPayload), "тело задания не искажено (инвариант 5)")
	assert.Nil(t, lastError, "успех не оставляет причины сбоя")
	assert.Equal(t, 1, handler.count(), "обработчик исполнен ровно один раз")

	// Аренда: done — терминальное состояние, повторно задание не берётся
	// (markDone не чистит locked_by/locked_until — инвариант 1 требует
	// аренду только в processing, поэтому остаток не нарушает схему, но
	// фиксируем его как наблюдение).
	if lockedBy != nil {
		t.Logf("после done: locked_by=%s, locked_until=%v", *lockedBy, lockedUntil)
	}

	var rows int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COUNT(*)::int FROM job WHERE topic = $1`, string(topic),
	).Scan(&rows))
	assert.Equal(t, 1, rows, "повтор постановки с тем же ключом не создал второе задание")
}

// readLockedUntil читает срок аренды единственного задания темы: по нему
// видно, что heartbeat сдвигает аренду вперёд, пока обработчик жив.
func readLockedUntil(t *testing.T, pool *pgxpool.Pool, topic queue.Topic) time.Time {
	t.Helper()

	var until *time.Time
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT locked_until FROM job WHERE topic = $1`, string(topic)).Scan(&until),
		"чтение срока аренды задания темы %s", topic)
	require.NotNil(t, until, "задание в processing всегда с арендой")
	return *until
}

// TestCancelInFlight — отмена в полёте (задача 6.4, D14): обработчик
// блокируется и уважает ctx, поэтому отмена гасит попытку за порядок
// Lease/3 (тик продления аренды, которым отмена и доходит до попытки), а
// не по таймауту попытки. Строку переводит отменяющий, а воркер,
// потерявший аренду, в неё не пишет: canceled остаётся canceled.
func TestCancelInFlight(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "e2e-cancel")

	const lease = 300 * time.Millisecond
	// cancelBound — сколько отводится на выход обработчика после отмены:
	// порядок величины Lease/3 (тик продления аренды) плюс запас на CI и
	// -race. Секундный бюджет означал бы, что задание гасит таймаут
	// попытки, а не отмена, — то есть тест ничего бы не проверял.
	cancelBound := lease/3 + 150*time.Millisecond

	handler := newBlockingHandler(0)
	engine := newEngineWith(t, repo, topic, handler, Config{
		PollInterval: e2ePollInterval,
		Lease:        lease,
		WorkerID:     "e2e-cancel-1",
	})
	defer func() { _ = engine.Close() }()

	_, _, err := repo.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"long":true}`),
	})
	require.NoError(t, err, "постановка долгого задания")
	engine.Start()

	var claim queue.Claim
	select {
	case claim = <-handler.started:
	case <-time.After(e2eTimeout):
		t.Fatal("обработчик не взял задание за отведённое время")
	}
	require.NotEmpty(t, claim.ID, "взятое задание приходит с идентификатором")

	start := time.Now()
	require.NoError(t, engine.Cancel(ctx, claim.ID), "отмена взятого задания")

	select {
	case <-handler.done:
	case <-time.After(cancelBound):
		t.Fatalf("обработчик не вышел за %s после отмены: отмена обязана гасить попытку за Lease/3=%s",
			cancelBound, lease/3)
	}
	elapsed := time.Since(start)
	assert.Less(t, elapsed, cancelBound,
		"отмена доходит до обработчика за доли секунды, а не по таймауту попытки (%s)", e2eTimeout)

	// Причина, которую увидел обработчик, — потеря аренды: строку перевёл
	// отменяющий, а продление не прошло guard по состоянию (D14).
	assert.ErrorIs(t, handler.causeOf(), queue.ErrLeaseLost,
		"отмена в полёте приходит обработчику как потеря аренды")

	// Строка: canceled от отменяющего, без причины сбоя и без второй
	// попытки. Воркер исход не пишет (settleNone) — иначе он перезаписал
	// бы чужое решение.
	waitJobState(t, pool, topic, queue.StateCanceled)

	// Воркер уже записал бы исход, если бы собирался: его settle идёт
	// сразу после возврата обработчика. Поэтому строку опрашиваем
	// несколько периодов подряд: canceled обязан остаться, last_error не
	// появиться, attempts не вырасти.
	deadline := time.Now().Add(10 * e2ePollInterval)
	for {
		var (
			state     string
			attempts  int
			lastError *string
		)
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, attempts, last_error FROM job WHERE topic = $1`, string(topic),
		).Scan(&state, &attempts, &lastError))

		assert.Equal(t, string(queue.StateCanceled), state, "исход отмены не перезаписан воркером")
		assert.Equal(t, 1, attempts, "отмена не списывает вторую попытку")
		assert.Nil(t, lastError, "отмена не оставляет причины сбоя")
		assert.Equal(t, 1, handler.count(), "задание исполнено ровно один раз")

		if time.Now().After(deadline) {
			break
		}
		time.Sleep(e2ePollInterval)
	}
}

// TestHeartbeatKeepsLease — heartbeat держит аренду, пока обработчик жив
// (задача 6.4, D13): обработчик работает три срока аренды, сборщик
// истёкших аренд тикает каждые 50ms, а задание всё это время остаётся в
// processing, аренда сдвигается вперёд и второй раз не исполняется.
func TestHeartbeatKeepsLease(t *testing.T) {
	pool := newTestPool(t)
	ctx := context.Background()
	repo := postgres.New(pool)
	topic := e2eTopic(t, "e2e-heartbeat")

	const (
		// lease — срок аренды: продление идёт каждые Lease/3 = 100ms.
		lease = 300 * time.Millisecond
		// work — сколько работает обработчик: три срока аренды. Без
		// heartbeat задание было бы возвращено в pending трижды.
		work = 3 * lease
		// reaperPeriod — период сборщика истёкших аренд: за время
		// работы обработчика он успевает попытаться вернуть задание
		// больше десяти раз.
		reaperPeriod = 50 * time.Millisecond
	)

	handler := newBlockingHandler(work)
	engine := newEngineWith(t, repo, topic, handler, Config{
		PollInterval: e2ePollInterval,
		Lease:        lease,
		ReaperPeriod: reaperPeriod,
		WorkerID:     "e2e-heartbeat-1",
	})
	defer func() { _ = engine.Close() }()

	_, _, err := repo.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"long":true}`),
	})
	require.NoError(t, err, "постановка долгого задания")
	engine.Start()

	var claim queue.Claim
	select {
	case claim = <-handler.started:
	case <-time.After(e2eTimeout):
		t.Fatal("обработчик не взял задание за отведённое время")
	}
	require.NotEmpty(t, claim.ID)

	firstUntil := readLockedUntil(t, pool, topic)
	start := time.Now()

	// Пока обработчик работает, задание обязано оставаться в processing, а
	// аренда — уходить в будущее: иначе сборщик вернул бы его в pending.
	var (
		samples  int
		maxUntil time.Time
	)
sampling:
	for {
		select {
		case <-handler.done:
			break sampling
		default:
		}

		var (
			state    string
			attempts int
			until    *time.Time
		)
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT state, attempts, locked_until FROM job WHERE topic = $1`, string(topic),
		).Scan(&state, &attempts, &until))

		require.Equal(t, string(queue.StateProcessing), state,
			"живая аренда держит задание в processing, пока обработчик работает")
		require.Equal(t, 1, attempts, "возврат по аренде не списывал бы вторую попытку")
		require.NotNil(t, until, "processing всегда с арендой")
		require.True(t, until.After(time.Now()), "аренда продлена в будущее, а не истекла")
		if until.After(maxUntil) {
			maxUntil = *until
		}
		samples++
		time.Sleep(e2ePollInterval)
	}

	assert.GreaterOrEqual(t, samples, 10, "пока обработчик работал, состояние опрошено много раз")
	assert.GreaterOrEqual(t, time.Since(start), work*9/10, "обработчик работал заметно дольше срока аренды")
	assert.True(t, maxUntil.After(firstUntil),
		"аренда продлевалась: locked_until сдвинулся вперёд (%v → %v)", firstUntil, maxUntil)

	// Второго исполнения не было: задание закрыто один раз и одной попыткой.
	waitJobState(t, pool, topic, queue.StateDone)
	assert.Equal(t, 1, handler.count(), "задание исполнено ровно один раз")

	var attempts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT attempts FROM job WHERE topic = $1`, string(topic)).Scan(&attempts))
	assert.Equal(t, 1, attempts, "повторного взятия не было")
}
