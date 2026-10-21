// Package loopback — общие middleware служебных и внутренних контуров:
// слушатели живут только на 127.0.0.1, каждый запрос требует
// сервисный bearer-токен. Сравнение токена — постоянного времени
// (тайминг секрета не раскрывает).
package loopback

import (
	"crypto/subtle"
	"log/slog"
	"net"
	"net/http"
	"strings"
)

// IsLoopback — локальная петля (IPv4 loopback/::1): единственный
// разрешённый источник запросов к внутренним контурам.
func IsLoopback(host string) bool {
	if host == "::1" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Only — middleware «только петля»: запрос не с 127.0.0.1 отклоняется.
func Only(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil || !IsLoopback(host) {
				log.Warn("внутренний контур: запрос не с петли отклонён", "remote", r.RemoteAddr)
				http.Error(w, "loopback only", http.StatusForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Bearer — middleware сервисного токена пары (Authorization: Bearer).
func Bearer(token string, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
				log.Warn("внутренний контур: невалидный токен")
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
