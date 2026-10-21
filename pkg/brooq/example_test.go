// Тест «снаружи модуля»: файл лежит в пакете brooq_test, поэтому
// импортирует библиотеку так же, как это делает чужой потребитель —
// brooq.New поверх postgres.New, и ничего больше. Это и есть
// доказательство, что internal/ не протёк в публичный API: если бы Queue
// или Repository ссылались на тип из internal/*, внешний тестовый пакет
// не собрался бы вовсе.
//
// Харнесс пакета brooq (изолированная схема, миграции) живёт во
// внутренних *_test.go и сюда по определению не виден, поэтому схема
// поднимается здесь же и тем же путём, что на деплое: goose + каталог
// migrations/. Без TEST_DATABASE_URL тест пропускается.
package brooq_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thefish/brooq/pkg/brooq"
	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

const (
	// libTimeout — сколько ждём исхода задания: с большим запасом, чтобы
	// медленная машина давала сообщение о провале, а не флак.
	libTimeout = 10 * time.Second
	// libPollInterval — период опроса состояния задания.
	libPollInterval = 20 * time.Millisecond
	// libMigrationsDir — каталог migrations/ в корне модуля: go test
	// запускает бинарник с рабочим каталогом пакета.
	libMigrationsDir = "../../migrations"
)

// countingHandler — обработчик темы, считающий вызовы: задание обязано
// быть исполнено ровно один раз.
type countingHandler struct {
	calls atomic.Int64
}

// Handle реализует queue.Handler.
func (h *countingHandler) Handle(_ context.Context, _ queue.Claim) error {
	h.calls.Add(1)
	return nil
}

// TestLibraryUsage — конечный сценарий очереди, собранный только из
// публичных пакетов: производитель ставит задание, воркер берёт его,
// исход записан и виден в той же строке.
func TestLibraryUsage(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL не задана")
	}

	ctx := context.Background()
	pool := newLibPool(t, ctx, dsn)

	repo := postgres.New(pool)
	engine := brooq.New(repo, discardLog(), brooq.Config{
		Workers:      1,
		PollInterval: libPollInterval,
		Lease:        30 * time.Second,
		WorkerID:     "lib-usage",
	})

	handler := &countingHandler{}
	const topic = queue.Topic("lib-usage")
	require.NoError(t, engine.RegisterKind(topic, handler, queue.KindOptions{
		MaxAttempts:         3,
		AttemptTimeout:      libTimeout,
		MaxExecutionTimeout: libTimeout,
		BackoffBase:         100 * time.Millisecond,
		BackoffMax:          time.Second,
	}), "регистрация темы")

	engine.Start()
	// Close останавливает воркеров и дожидается их выхода (Stop внутри).
	defer func() { _ = engine.Close() }()

	_, _, err := engine.Enqueue(ctx, queue.Job{
		Topic:   topic,
		Payload: queue.Payload(`{"order":"42"}`),
	})
	require.NoError(t, err, "постановка задания через фасад")

	waitLibState(t, ctx, pool, topic, queue.StateDone)

	assert.Equal(t, int64(1), handler.calls.Load(), "обработчик исполнен ровно один раз")

	var (
		state    string
		attempts int
	)
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT state, attempts FROM job WHERE topic = $1`, string(topic),
	).Scan(&state, &attempts))
	assert.Equal(t, string(queue.StateDone), state, "исход записан в строку задания")
	assert.Equal(t, 1, attempts, "попытка списана при взятии ровно один раз")
}

// discardLog глушит вывод движка: он не предмет проверки.
func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newLibPool поднимает изолированную схему, накатывает в неё migrations/
// и возвращает пул с search_path на неё. Схема убирается по завершении
// теста; соединения пула закрываются раньше неё (Cleanup — LIFO).
func newLibPool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()

	schema := fmt.Sprintf("brooq_lib_%d", os.Getpid())

	bootstrap, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err, "подключение к тестовой БД")
	t.Cleanup(func() {
		if _, err := bootstrap.Exec(context.Background(),
			"DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Logf("не удалось убрать схему %s: %v", schema, err)
		}
		bootstrap.Close()
	})

	// Схема могла остаться от убитого прогона с тем же pid.
	_, err = bootstrap.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	require.NoError(t, err, "очистка схемы %s", schema)
	_, err = bootstrap.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err, "создание схемы %s", schema)

	require.NoError(t, applyLibMigrations(dsn, schema, resolveLibMigrationsDir(t)),
		"миграции каталога migrations/")

	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err, "разбор DSN")
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err, "пул на схему %s", schema)
	t.Cleanup(pool.Close)
	return pool
}

// resolveLibMigrationsDir находит каталог миграций: MIGRATIONS_DIR, если
// он существует (Makefile задаёт абсолютный путь), иначе каталог
// migrations/ в корне модуля.
func resolveLibMigrationsDir(t *testing.T) string {
	t.Helper()

	candidates := []string{libMigrationsDir}
	if dir := os.Getenv("MIGRATIONS_DIR"); dir != "" {
		candidates = append([]string{dir}, candidates...)
	}
	for _, dir := range candidates {
		abs, err := filepath.Abs(dir)
		if err != nil {
			continue
		}
		if isLibDir(abs) {
			return filepath.Clean(abs)
		}
	}
	t.Fatalf("каталог миграций не найден (пробовали %v)", candidates)
	return ""
}

// isLibDir сообщает, что path существует и является каталогом. Вынесено
// в отдельную функцию, как isDir в pkg/postgres: путь приходит из
// MIGRATIONS_DIR, значение задаёт сам запускающий тесты, поэтому «path
// traversal» здесь не угроза; перед проверкой путь нормализуется.
func isLibDir(path string) bool {
	//nolint:gosec // путь задаёт сам запускающий тесты (MIGRATIONS_DIR), каталог миграций — не внешний ввод
	st, err := os.Stat(filepath.Clean(path))
	return err == nil && st.IsDir()
}

// applyLibMigrations применяет каталог миграций dir к схеме schema через
// goose — тот же путь, что и на деплое.
func applyLibMigrations(dsn, schema, dir string) error {
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("goose: установка диалекта: %w", err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("goose: разбор DSN: %w", err)
	}
	// search_path задаётся параметрами DSN, чтобы goose писал
	// goose_db_version в изолированную схему.
	cfg.RuntimeParams["search_path"] = schema
	sqlDB := stdlib.OpenDB(*cfg)
	defer func() { _ = sqlDB.Close() }()
	if err := goose.Up(sqlDB, dir); err != nil {
		return fmt.Errorf("goose up (%s): %w", dir, err)
	}
	return nil
}

// waitLibState ждёт, пока задание темы придёт в состояние want: опрос с
// коротким шагом и внятным сообщением при провале.
func waitLibState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, topic queue.Topic, want queue.State) {
	t.Helper()

	deadline := time.Now().Add(libTimeout)
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
				topic, want, libTimeout, state)
		}
		time.Sleep(libPollInterval)
	}
}
