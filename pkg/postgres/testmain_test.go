// Package postgres — TestMain интеграционных тестов: создаёт
// изолированную схему, применяет к ней миграции каталога migrations/
// через goose (единственный источник схемы), прогоняет тесты
// и убирает схему. Без TEST_DATABASE_URL — только unit-тесты пакета,
// интеграционные сами себя пропускают (t.Skip), поэтому `make test`
// без базы зелёный, но ничего не доказывает про миграции.
//
// Миграции НЕ встраиваются в бинарник через //go:embed: goose читает
// каталог с диска — тот же, что накатывает `make migrate-up`.
// Каталог задаётся MIGRATIONS_DIR, по умолчанию ../.. /migrations
// относительно каталога пакета (go test запускает бинарник из него).
// Базу поднимает `make test-db-up` (docker), а не сам тест.
package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/thefish/brooq/pkg/db"
)

// defaultMigrationsDir — каталог migrations/ в корне модуля. Тестовый
// бинарник запускается с рабочим каталогом пакета, поэтому путь
// поднимается на два уровня. Переопределяется MIGRATIONS_DIR.
const defaultMigrationsDir = "../../migrations"

// testPool — пул соединений на тестовую схему (инициализируется в TestMain).
var testPool *pgxpool.Pool

// migrationsDir возвращает каталог миграций: MIGRATIONS_DIR, если задан,
// иначе defaultMigrationsDir.
func migrationsDir() string {
	if dir := os.Getenv("MIGRATIONS_DIR"); dir != "" {
		return dir
	}
	return defaultMigrationsDir
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

// TestMain настраивает изолированную схему и выполняет тесты.
func TestMain(m *testing.M) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		fmt.Println("TEST_DATABASE_URL не задана: интеграционные тесты будут пропущены")
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
	testPool, cleanup = setUpSchema(dsn, fmt.Sprintf("brooq_repo_%d", os.Getpid()), dir)

	code := m.Run()
	cleanup()
	os.Exit(code)
}

// applyMigrations применяет к схеме каталог миграций dir через goose —
// тот же путь, что и make deploy: никакого drift между
// тестами и продакшеном; второй прогон — no-op. search_path
// задаётся через параметры DSN, чтобы goose писал goose_db_version в
// изолированную схему.
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

// newTestPool возвращает пул на изолированную схему; без
// TEST_DATABASE_URL — Skip.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if testPool == nil {
		t.Skip("TEST_DATABASE_URL не задана")
	}
	return testPool
}
