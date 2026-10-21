package health

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakePinger — заглушка Pinger для тестов хендлера.
type fakePinger struct {
	err error
}

func (f fakePinger) Ping(_ context.Context) error { return f.err }

func doRequest(t *testing.T, p Pinger) *httptest.ResponseRecorder {
	t.Helper()
	log := slog.New(slog.DiscardHandler)
	req := httptest.NewRequest(http.MethodGet, "/healthz", http.NoBody)
	rec := httptest.NewRecorder()
	Handler(p, log).ServeHTTP(rec, req)
	return rec
}

// statusBody извлекает поле status из JSON-ответа.
func statusBody(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("ответ не является JSON: %v (%q)", err, rec.Body.String())
	}
	return body.Status
}

func TestHealthOK(t *testing.T) {
	rec := doRequest(t, fakePinger{err: nil})
	if rec.Code != http.StatusOK {
		t.Errorf("код = %d, ожидался 200", rec.Code)
	}
	if got := statusBody(t, rec); got != "ok" {
		t.Errorf("status = %q, ожидалось ok", got)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, ожидался application/json", ct)
	}
}

func TestHealthDBDown(t *testing.T) {
	rec := doRequest(t, fakePinger{err: errors.New("connection refused")})
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("код = %d, ожидался 503", rec.Code)
	}
	if got := statusBody(t, rec); got != "unavailable" {
		t.Errorf("status = %q, ожидалось unavailable", got)
	}
}

// deadlinePinger фиксирует контекст, с которым вызывается Ping.
type deadlinePinger struct {
	gotCtx context.Context
}

func (p *deadlinePinger) Ping(ctx context.Context) error {
	p.gotCtx = ctx
	return nil
}

func TestHealthPingHasDeadline(t *testing.T) {
	p := &deadlinePinger{}
	rec := doRequest(t, p)
	if rec.Code != http.StatusOK {
		t.Errorf("код = %d, ожидался 200", rec.Code)
	}
	if p.gotCtx == nil {
		t.Fatal("Ping не вызван")
	}
	if _, ok := p.gotCtx.Deadline(); !ok {
		t.Error("контекст Ping не имеет дедлайна: /healthz может висеть при обесточенной БД")
	}
}
