package health

// Интеграционный тест: пул указывает на слушателя
// «accept-but-drop» (TCP-соединение принимается, ответа нет — чёрная дыра).
// Без per-call дедлайна пинг висел бы до ConnectTimeout, и /healthz не
// вернул бы 503 в пределах времени теста. Дедлайн
// хендлера — единственное, что ограничивает ответ, ConnectTimeout пула
// сознательно больше дедлайна, чтобы тест проверял именно дедлайн.

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestHealthzBlackHole: реальный слушатель принимает TCP-соединение пула
// и молчит; /healthz обязан ответить 503 по истечении PingTimeout, а не
// висеть до ConnectTimeout.
func TestHealthzBlackHole(t *testing.T) {
	// Слушатель, который принимает соединение и никогда не отвечает.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("не удалось открыть слушатель: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Держим соединение открытым, но не пишем ни байта.
			_ = conn
		}
	}()

	dsn := fmt.Sprintf("postgres://probe:probe@%s/probe?sslmode=disable", ln.Addr())
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("разбор DSN: %v", err)
	}
	// ConnectTimeout заведомо больше PingTimeout: если хендлер не вернёт
	// 503 по своему дедлайну, тест краснеет по таймауту клиента, а не
	// «спасается» общим таймаутом пула.
	cfg.ConnConfig.ConnectTimeout = PingTimeout * 6
	cfg.MaxConns = 1
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("создание пула: %v", err)
	}
	t.Cleanup(pool.Close)

	srv := httptest.NewServer(Handler(pool, slog.New(slog.DiscardHandler)))
	t.Cleanup(srv.Close)

	client := &http.Client{Timeout: PingTimeout * 3}
	start := time.Now()
	resp, err := client.Get(srv.URL + "/healthz")
	elapsed := time.Since(start)
	if err != nil {
		// Без дедлайна хендлера запрос упирался бы в таймаут клиента:
		// пинг висит до ConnectTimeout, ответа в пределах теста нет.
		t.Fatalf("GET /healthz: %v (за %v) — /healthz не ответил в пределах времени", err, elapsed.Round(time.Millisecond))
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("код = %d, ожидался 503", resp.StatusCode)
	}
	// Нижняя граница: дедлайн не может сработать раньше времени, при
	// этом ответ обязан прийти именно по таймауту, а не мгновенно.
	if elapsed < PingTimeout-500*time.Millisecond {
		t.Errorf("503 пришёл за %v — раньше дедлайна %v: ошибка не по таймауту",
			elapsed.Round(time.Millisecond), PingTimeout)
	}
	if elapsed >= PingTimeout*3 {
		t.Errorf("503 пришёл за %v — не уложился в %v",
			elapsed.Round(time.Millisecond), PingTimeout*3)
	}
}
