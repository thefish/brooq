package queue

import (
	"context"
	"errors"
)

// Причины прерывания попытки. Голый <-ctx.Done() не различает таймаут и
// отмену, а разница принципиальна: таймаут — сбой попытки с повтором,
// отмена — терминальный исход (D15). Поэтому причина передаётся через
// context.WithCancelCause и разбирается здесь, а не в обработчике.
var (
	// ErrLeaseLost — аренда потеряна: задание отменено административно
	// или перехвачено другим воркером. Терминальный исход: строку уже
	// перевёл отменяющий, воркер в неё не пишет (D14).
	ErrLeaseLost = errors.New("аренда задания потеряна")
	// ErrMaxExecutionExceeded — исчерпан жёсткий потолок исполнения темы
	// (MaxExecutionTimeout). Это сбой попытки с повтором (D15), а не
	// терминальный исход: dead достигается только исчерпанием попыток
	// (записанным сбоем), а не самим фактом потолка.
	ErrMaxExecutionExceeded = errors.New("потолок исполнения исчерпан")
)

// AbortKind — исход прерванной попытки, выведенный из причины контекста.
type AbortKind int

const (
	// AbortNone — прерывания не было: контекст жив.
	AbortNone AbortKind = iota
	// AbortRetryable — сбой попытки: задание уйдёт в failed и будет
	// взято снова после backoff.
	AbortRetryable
	// AbortTerminal — терминальный исход: задание не повторяется
	// (потеря аренды, отмена без явной причины).
	AbortTerminal
)

// String — человекочитаемое имя исхода (для логов и метрик).
func (k AbortKind) String() string {
	switch k {
	case AbortNone:
		return "none"
	case AbortRetryable:
		return "retryable"
	case AbortTerminal:
		return "terminal"
	default:
		return "unknown"
	}
}

// ClassifyCause разбирает причину прерывания контекста (context.Cause) и
// возвращает исход попытки вместе с самой причиной — её пишут в лог и в
// last_error.
//
// Правила (D15): DeadlineExceeded и ErrMaxExecutionExceeded — сбой попытки
// с повтором (failed + backoff, dead — только по исчерпанию попыток);
// ErrLeaseLost и отмена без явной причины — терминальный исход. Причина
// читается через context.Cause, поэтому доходит и через обёрнутый контекст,
// и в виде обёрнутой ошибки fmt.Errorf("%w", …).
func ClassifyCause(ctx context.Context) (AbortKind, error) {
	cause := context.Cause(ctx)
	if cause == nil {
		return AbortNone, nil
	}
	switch {
	case errors.Is(cause, ErrLeaseLost):
		return AbortTerminal, cause
	case errors.Is(cause, context.DeadlineExceeded), errors.Is(cause, ErrMaxExecutionExceeded):
		return AbortRetryable, cause
	default:
		// Отмена без явной причины (context.Canceled, остановка
		// процесса): исполнять попытку дальше нельзя, повтор не
		// назначается.
		return AbortTerminal, cause
	}
}
