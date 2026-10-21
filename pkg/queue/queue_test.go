package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestStateValues — значения шести состояний: по ним пишутся
// CHECK-ограничение таблицы job, SQL-запросы и метрики, поэтому они
// контракт, а не деталь реализации.
func TestStateValues(t *testing.T) {
	tests := []struct {
		name  string
		state State
		want  string
	}{
		{"ожидание взятия", StatePending, "pending"},
		{"взято и закреплено арендой", StateProcessing, "processing"},
		{"ожидание повтора", StateFailed, "failed"},
		{"исполнено успешно", StateDone, "done"},
		{"попытки исчерпаны", StateDead, "dead"},
		{"отменено без второй «l»", StateCanceled, "canceled"},
	}

	seen := make(map[string]string, len(tests))
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := string(tc.state); got != tc.want {
				t.Fatalf("состояние = %q, ожидалось %q", got, tc.want)
			}
		})
		if prev, ok := seen[string(tc.state)]; ok {
			t.Fatalf("состояния %q и %q совпадают по значению", prev, tc.name)
		}
		seen[string(tc.state)] = tc.name
	}
}

// TestSignalErrorsIs — сигналы обработчика сравниваются через errors.Is и
// переживают обёртку fmt.Errorf("%w", …); чужие ошибки сигналами не
// считаются.
func TestSignalErrorsIs(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		target error
		want   bool
	}{
		{"сигнал отсрочки распознаётся напрямую", ErrMustDelay, ErrMustDelay, true},
		{"отсрочка с интервалом распознаётся как сигнал", MustDelay(5 * time.Second), ErrMustDelay, true},
		{"обёрнутая отсрочка переживает %w", fmt.Errorf("попытка: %w", MustDelay(2*time.Second)), ErrMustDelay, true},
		{"обёрнутый сигнал сохранения попытки", fmt.Errorf("валидация: %w", ErrMustPreserveAttempt), ErrMustPreserveAttempt, true},
		{"обёрнутый сигнал отмены", fmt.Errorf("домен: %w", ErrMustCancelTask), ErrMustCancelTask, true},
		{"сигналы не путаются между собой", ErrMustCancelTask, ErrMustDelay, false},
		{"сохранение попытки не является отменой", ErrMustPreserveAttempt, ErrMustCancelTask, false},
		{"обычная ошибка не является сигналом", errors.New("сбой"), ErrMustDelay, false},
		{"потеря аренды не является сигналом", ErrLeaseLost, ErrMustPreserveAttempt, false},
		{"nil не является сигналом", nil, ErrMustCancelTask, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := errors.Is(tc.err, tc.target); got != tc.want {
				t.Fatalf("errors.Is(%v, %v) = %v, ожидалось %v", tc.err, tc.target, got, tc.want)
			}
		})
	}
}

// TestDelayOf — интервал отсрочки достаётся через errors.As, в том числе
// из обёрнутой ошибки; у простых сигналов интервала нет.
func TestDelayOf(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantDelay time.Duration
		wantOK    bool
	}{
		{"интервал достаётся из сигнала", MustDelay(3 * time.Second), 3 * time.Second, true},
		{"интервал достаётся из обёрнутой ошибки", fmt.Errorf("обработчик: %w", MustDelay(time.Minute)), time.Minute, true},
		{"нулевая отсрочка допустима", MustDelay(0), 0, true},
		{"отрицательная отсрочка не теряется", MustDelay(-time.Second), -time.Second, true},
		{"простой сигнал отсрочки интервала не несёт", ErrMustDelay, 0, false},
		{"обычная ошибка интервала не несёт", errors.New("сбой"), 0, false},
		{"nil интервала не несёт", nil, 0, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotDelay, gotOK := DelayOf(tc.err)
			if gotOK != tc.wantOK {
				t.Fatalf("признак сигнала = %v, ожидалось %v", gotOK, tc.wantOK)
			}
			if gotDelay != tc.wantDelay {
				t.Fatalf("интервал = %s, ожидалось %s", gotDelay, tc.wantDelay)
			}
		})
	}
}

// TestClassifyCause — правила D15: истёкший дедлайн и потолок исполнения
// темы — сбой попытки с повтором (dead достигается только исчерпанием
// попыток); потеря аренды и отмена без явной
// причины — терминальный исход; причина читается через context.Cause и
// доходит из обёрнутого контекста.
func TestClassifyCause(t *testing.T) {
	type ctxKey string

	tests := []struct {
		name     string
		setup    func(t *testing.T) context.Context
		wantKind AbortKind
		// wantCause — причина, которую обязана вернуть классификация
		// (сравнение через errors.Is); nil — причины нет.
		wantCause error
	}{
		{
			name:      "живой контекст — прерывания не было",
			setup:     func(*testing.T) context.Context { return context.Background() },
			wantKind:  AbortNone,
			wantCause: nil,
		},
		{
			name: "истёкший дедлайн — сбой попытки с повтором",
			setup: func(t *testing.T) context.Context {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				t.Cleanup(cancel)
				return ctx
			},
			wantKind:  AbortRetryable,
			wantCause: context.DeadlineExceeded,
		},
		{
			name: "потеря аренды — терминальный исход",
			setup: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				t.Cleanup(func() { cancel(nil) })
				cancel(ErrLeaseLost)
				return ctx
			},
			wantKind:  AbortTerminal,
			wantCause: ErrLeaseLost,
		},
		{
			name: "потолок исполнения исчерпан — сбой попытки с повтором",
			setup: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				t.Cleanup(func() { cancel(nil) })
				cancel(ErrMaxExecutionExceeded)
				return ctx
			},
			wantKind:  AbortRetryable,
			wantCause: ErrMaxExecutionExceeded,
		},
		{
			name: "обёрнутый потолок исполнения остаётся сбоем попытки",
			setup: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				t.Cleanup(func() { cancel(nil) })
				cancel(fmt.Errorf("воркер w-1: %w", ErrMaxExecutionExceeded))
				return ctx
			},
			wantKind:  AbortRetryable,
			wantCause: ErrMaxExecutionExceeded,
		},
		{
			name: "обёрнутая причина аренды доходит через context.Cause",
			setup: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				t.Cleanup(func() { cancel(nil) })
				cancel(fmt.Errorf("воркер w-1: %w", ErrLeaseLost))
				return ctx
			},
			wantKind:  AbortTerminal,
			wantCause: ErrLeaseLost,
		},
		{
			name: "обёрнутый дедлайн остаётся сбоем попытки",
			setup: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				t.Cleanup(func() { cancel(nil) })
				cancel(fmt.Errorf("попытка: %w", context.DeadlineExceeded))
				return ctx
			},
			wantKind:  AbortRetryable,
			wantCause: context.DeadlineExceeded,
		},
		{
			name: "причина отмены доходит через обёрнутый контекст",
			setup: func(t *testing.T) context.Context {
				parent, cancel := context.WithCancelCause(context.Background())
				t.Cleanup(func() { cancel(nil) })
				child := context.WithValue(parent, ctxKey("задание"), "id-1")
				cancel(ErrLeaseLost)
				return child
			},
			wantKind:  AbortTerminal,
			wantCause: ErrLeaseLost,
		},
		{
			name: "отмена без явной причины — терминальный исход",
			setup: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				t.Cleanup(cancel)
				cancel()
				return ctx
			},
			wantKind:  AbortTerminal,
			wantCause: context.Canceled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.setup(t)

			gotKind, gotCause := ClassifyCause(ctx)
			if gotKind != tc.wantKind {
				t.Fatalf("исход = %s (%d), ожидалось %s (%d)",
					gotKind, gotKind, tc.wantKind, tc.wantKind)
			}
			if tc.wantCause == nil {
				if gotCause != nil {
					t.Fatalf("причина = %v, ожидалось отсутствие причины", gotCause)
				}
				return
			}
			if !errors.Is(gotCause, tc.wantCause) {
				t.Fatalf("причина = %v, ожидалась %v", gotCause, tc.wantCause)
			}
		})
	}
}

// TestAbortKindString — человекочитаемые имена исходов (логи и метрики);
// неизвестное значение не паникует и не выдаёт себя за известное.
func TestAbortKindString(t *testing.T) {
	tests := []struct {
		name string
		kind AbortKind
		want string
	}{
		{"прерывания не было", AbortNone, "none"},
		{"сбой попытки с повтором", AbortRetryable, "retryable"},
		{"терминальный исход", AbortTerminal, "terminal"},
		{"неизвестный исход", AbortKind(99), "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.kind.String(); got != tc.want {
				t.Fatalf("String() = %q, ожидалось %q", got, tc.want)
			}
		})
	}
}
