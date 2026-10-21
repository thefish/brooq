// Команда brooq запускает юнит очереди: движок (воркеры зарегистрированных
// тем) и служебную поверхность (healthz и metrics на loopback).
// Здесь только композиция: конфиг, логгер, пул, наблюдаемость, graceful
// shutdown. Логика очереди живёт в pkg/brooq (движок) и pkg/postgres
// (SQL), не в main.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thefish/brooq/internal/health"
	"github.com/thefish/brooq/internal/observability"
	"github.com/thefish/brooq/pkg/brooq"
	"github.com/thefish/brooq/pkg/config"
	"github.com/thefish/brooq/pkg/db"
	"github.com/thefish/brooq/pkg/logger"
	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

// Version — версия сборки, подставляется ldflags в Makefile (make build).
var Version = "dev"

// demoTopic — тема демонстрационного обработчика: критерий готовности
// change'а — «положить задание и поймать его консьюмером».
const demoTopic = queue.Topic("demo")

// demoHandler — демонстрационный обработчик до появления реальных
// потребителей: логирует взятое задание и сообщает об успехе. Заменяется
// реальным обработчиком при подключении первого потребителя.
type demoHandler struct {
	log *slog.Logger
}

// Handle логирует задание и возвращает успех: исход попытки — done.
func (h demoHandler) Handle(_ context.Context, job queue.Claim) error {
	h.log.Info("задание получено",
		"topic", job.Topic, "job", job.ID, "attempt", job.Attempt)
	return nil
}

// engineMetrics — адаптер порта метрик движка поверх реестра
// observability. Живёт в точке сборки, потому что движку запрещено
// импортировать internal/*, а реестр — часть внутренностей
// процесса. help-строка пустая: реестр подставляет имя метрики.
type engineMetrics struct {
	reg *observability.Registry
}

// Inc увеличивает счётчик реестра на 1.
func (m engineMetrics) Inc(name string, labels map[string]string) {
	m.reg.Inc(name, "", labels)
}

// Add увеличивает счётчик реестра на delta.
func (m engineMetrics) Add(name string, labels map[string]string, delta uint64) {
	m.reg.Add(name, "", labels, delta)
}

// AddFloat увеличивает счётчик реестра на дробную delta (сумма
// длительностей попыток в секундах).
func (m engineMetrics) AddFloat(name string, labels map[string]string, delta float64) {
	m.reg.AddFloat(name, "", labels, delta)
}

// Set устанавливает gauge реестра в абсолютное значение (дробное:
// возраст старейшего задания и занятость пула — в секундах с долями).
func (m engineMetrics) Set(name string, value float64, labels map[string]string) {
	m.reg.Set(name, "", labels, value)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "brooq завершился с ошибкой: %v\n", err)
		os.Exit(1)
	}
}

// run собирает процесс и крутит служебную поверхность до сигнала
// остановки. Ошибки возвращаются наверх: код выхода — не 0.
func run() error {
	cfg, err := config.Load(config.Options{})
	if err != nil {
		return err
	}

	log, err := logger.New(cfg.LogLevel, cfg.LogFormat, "brooq")
	if err != nil {
		return fmt.Errorf("инициализация логгера: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.DatabaseURL, db.DefaultParams())
	if err != nil {
		return fmt.Errorf("подключение к базе: %w", err)
	}
	defer pool.Close()

	// Реестр создаётся до движка: движок публикует в него brooq_* через
	// адаптер, поэтому /metrics показывает очередь сразу после старта.
	metrics := observability.New()

	// Движок собирается из пула, логгера и настроек. Регистрация темы —
	// до Start (D9): повторная регистрация и неверные настройки — ошибка
	// старта, а не исполнения.
	engine := brooq.New(postgres.New(pool), log, brooq.Config{
		Workers:         cfg.QueueWorkers,
		PollInterval:    cfg.QueuePollInterval,
		Lease:           cfg.QueueLease,
		ReaperPeriod:    cfg.QueueReaperPeriod,
		WatchdogGrace:   cfg.QueueWatchdogGrace,
		StatsPeriod:     cfg.QueueStatsPeriod,
		RetentionTTL:    cfg.QueueRetentionTTL,
		RetentionBatch:  cfg.QueueRetentionBatch,
		RetentionPeriod: cfg.QueueRetentionPeriod,
		Metrics:         engineMetrics{reg: metrics},
	})
	if err := engine.RegisterKind(demoTopic, demoHandler{log: log}, queue.KindOptions{
		MaxAttempts:         cfg.QueueMaxAttempts,
		AttemptTimeout:      cfg.QueueAttemptTimeout,
		MaxExecutionTimeout: cfg.QueueMaxExecutionTimeout,
		BackoffBase:         cfg.QueueBackoffBase,
		BackoffMax:          cfg.QueueBackoffMax,
	}); err != nil {
		return fmt.Errorf("регистрация темы %s: %w", demoTopic, err)
	}
	engine.Start()
	// Close дожидается выхода воркеров; на пути с ошибкой поверхности
	// тоже нужен, иначе горутины переживут return.
	defer func() {
		if err := engine.Close(); err != nil {
			log.Error("остановка движка", "error", err)
		}
	}()
	log.Info("движок запущен",
		"workers", cfg.QueueWorkers, "lease", cfg.QueueLease, "topics", demoTopic)

	mux := http.NewServeMux()
	mux.Handle("/metrics", http.HandlerFunc(metrics.Handler))
	mux.Handle("/healthz", health.Handler(pool, log))

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           metrics.Middleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("brooq слушает", "addr", cfg.HTTPAddr, "version", Version)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("служебная поверхность: %w", err)
	case <-ctx.Done():
	}

	log.Info("brooq останавливается")
	// Сначала движок: новые задания не берутся, а исполненные получают
	// исход. Задания в полёте исход не получают — их вернёт Reaper после
	// истечения аренды (D8). Затем служебная поверхность.
	engine.Stop()
	if err := engine.Close(); err != nil {
		return fmt.Errorf("остановка движка: %w", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("остановка служебной поверхности: %w", err)
	}
	return nil
}
