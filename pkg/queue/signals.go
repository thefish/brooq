package queue

import (
	"errors"
	"fmt"
	"time"
)

// Сигналы обработчика: ими обработчик сообщает движку, что исход попытки
// не сводится к «успех/ошибка». Сравнивать их нужно через errors.Is —
// сигналы переживают обёртку fmt.Errorf("%w", …).
var (
	// ErrMustDelay — сигнал отсрочки: сбой записан, но следующая
	// попытка назначается через интервал из MustDelay. Счётчик попыток
	// при этом расходуется как при обычном сбое.
	ErrMustDelay = errors.New("обработчик просит отсрочку")
	// ErrMustPreserveAttempt — сигнал сохранить попытку: счётчик
	// попыток не увеличивается, задание возвращается в оборот.
	ErrMustPreserveAttempt = errors.New("обработчик просит сохранить попытку")
	// ErrMustCancelTask — сигнал отмены: задание становится canceled,
	// терминальный исход.
	ErrMustCancelTask = errors.New("обработчик просит отменить задание")
)

// DelayError — сигнал отсрочки вместе с интервалом до следующей
// попытки. Создаётся конструктором MustDelay; errors.Is(err,
// ErrMustDelay) истинно, интервал достаётся через DelayOf.
type DelayError struct {
	// Delay — интервал до следующей попытки.
	Delay time.Duration
}

// Error реализует error.
func (e *DelayError) Error() string {
	return fmt.Sprintf("%v: %s", ErrMustDelay, e.Delay)
}

// Unwrap делает сигнал совместимым с errors.Is(err, ErrMustDelay).
func (e *DelayError) Unwrap() error { return ErrMustDelay }

// MustDelay возвращает сигнал отсрочки на заданный интервал: обработчик
// вернул его вместо обычной ошибки, когда повтор нужен раньше, чем дал бы
// backoff, или позже.
func MustDelay(d time.Duration) error {
	return &DelayError{Delay: d}
}

// DelayOf достаёт интервал отсрочки из ошибки обработчика, в том числе
// обёрнутой через fmt.Errorf("%w"). Второй результат — был ли это вообще
// сигнал отсрочки.
func DelayOf(err error) (time.Duration, bool) {
	var target *DelayError
	if errors.As(err, &target) {
		return target.Delay, true
	}
	return 0, false
}
