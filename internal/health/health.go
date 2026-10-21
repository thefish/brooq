// Package health предоставляет healthcheck-хендлер /healthz: 200
// {"status":"ok"} при доступной БД, 503 {"status":"unavailable"} при
// недоступной. Живёт на служебной поверхности loopback и
// никогда не стоит за аутентификацией — иначе мониторинг слепнет.
//
// Пул проверяется по интерфейсу Pinger, а не по конкретному
// *pgxpool.Pool, чтобы хендлер можно было тестировать на заглушке.
package health

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// PingTimeout — бюджет времени на проверку пула: /healthz — это liveness
// probe, он не должен висеть, если БД «провалилась в чёрную дыру».
// Дедлайн сознательно больше ConnectTimeout пула (5 с в db.DefaultParams),
// чтобы не ругаться 503 на здоровых, но медленных БД.
const PingTimeout = 10 * time.Second

// Pinger — минимальный контракт проверки доступности БД.
// *pgxpool.Pool удовлетворяет ему (Ping(ctx) error).
type Pinger interface {
	Ping(ctx context.Context) error
}

// Handler возвращает http.HandlerFunc для GET /healthz: 200 JSON
// {"status":"ok"}, когда pool.Ping проходит; 503 JSON
// {"status":"unavailable"} при ошибке (ошибка логируется, процесс
// продолжает работать — пул сам переживёт короткий даунтайм БД).
func Handler(pool Pinger, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Ping с собственным дедлайном: без него обесточенная БД могла бы
		// висеть в Ping дольше, чем живы мониторинг и балансировщик.
		ctx, cancel := context.WithTimeout(r.Context(), PingTimeout)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			log.Error("healthcheck: база данных недоступна", "error", err)
			writeJSON(w, http.StatusServiceUnavailable, `{"status":"unavailable"}`)
			return
		}
		writeJSON(w, http.StatusOK, `{"status":"ok"}`)
	}
}

// writeJSON пишет фиксированный JSON-ответ с корректным Content-Type.
func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
