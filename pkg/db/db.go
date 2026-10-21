// Package db управляет пулом соединений PostgreSQL на базе pgxpool:
// инициализация, проверка доступности (ping) и корректное закрытие.
//
// Пакет нейтрален: ни домена, ни SQL он не знает, поэтому его
// подключают и юнит, и тесты — SQL живёт в pkg/postgres.
package db

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Params — параметры пула соединений с разумными значениями по умолчанию.
type Params struct {
	// MaxConns — максимальное число соединений в пуле.
	MaxConns int32
	// MinConns — минимальное число поддерживаемых соединений.
	MinConns int32
	// MaxConnLifetime — максимальное время жизни соединения.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime — максимальное время простоя соединения.
	MaxConnIdleTime time.Duration
	// ConnectTimeout — таймаут установки соединения.
	ConnectTimeout time.Duration
}

// DefaultParams возвращает параметры пула по умолчанию.
func DefaultParams() Params {
	return Params{
		MaxConns:        10,
		MinConns:        1,
		MaxConnLifetime: time.Hour,
		MaxConnIdleTime: 30 * time.Minute,
		ConnectTimeout:  5 * time.Second,
	}
}

// Connect создаёт пул соединений pgx по DSN, настраивает его
// параметрами poolParams и проверяет доступность базы данных
// через Ping. При неудаче возвращает ошибку с причиной.
func Connect(ctx context.Context, dsn string, poolParams Params) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("разбор DSN: %w", err)
	}

	cfg.MaxConns = poolParams.MaxConns
	cfg.MinConns = poolParams.MinConns
	cfg.MaxConnLifetime = poolParams.MaxConnLifetime
	cfg.MaxConnIdleTime = poolParams.MaxConnIdleTime
	cfg.ConnConfig.ConnectTimeout = poolParams.ConnectTimeout

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("создание пула соединений: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, poolParams.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping базы данных: %w", err)
	}

	return pool, nil
}

// DropSchemaWithGuard — DROP SCHEMA ... CASCADE с зачисткой осиротевших
// транзакций. Тестовая БД общая: kill -9 прогона оставляет соединения в
// «idle in transaction», которые держат лок на схеме; pid-имена схем
// переиспользуются после рестарта контейнера, и следующий DROP висел на
// их локах навсегда (параллельный запуск — здоровая активность не бывает
// idle-in-transaction, поэтому сирот можно чистить).
func DropSchemaWithGuard(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	// Завершаем только зависшие НАДОЛГО транзакции упавших прогонов:
	// живые параллельные тесты держат транзакции доли секунды (xact_start
	// свежий), поэтому порог 5 секунд отсекает их; сироты kill -9/рестарта
	// контейнера сидят во idle-in-transaction часами и держат лок на схеме.
	if _, err := pool.Exec(ctx,
		`SELECT pg_terminate_backend(pid)
		  FROM pg_stat_activity
		  WHERE datname = current_database()
		    AND state = 'idle in transaction'
		    AND xact_start < now() - interval '5 seconds'
		    AND pid <> pg_backend_pid()`); err != nil {
		return fmt.Errorf("зачистка зависших транзакций: %w", err)
	}
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		return fmt.Errorf("удаление схемы %s: %w", schema, err)
	}
	return nil
}

// ExitWithSchemaDrop drops a test schema and exits the process with code 1.
// It is needed on test-setup failure paths that run BEFORE a cleanup func
// exists: os.Exit does not run deferred calls, so without an explicit DROP the
// abandoned schema stays in the database forever - the schema name carries the
// PID, every run gets a fresh one, and the next cleanup never finds it.
// Called only from test TestMain/setUpSchema helpers.
func ExitWithSchemaDrop(ctx context.Context, bootstrap *pgxpool.Pool, schema string) {
	if err := DropSchemaWithGuard(ctx, bootstrap, schema); err != nil {
		fmt.Printf("failed to drop schema %s: %v\n", schema, err)
	}
	bootstrap.Close()
	os.Exit(1)
}
