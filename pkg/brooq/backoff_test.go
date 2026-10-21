package brooq

import (
	"math"
	"testing"
	"time"
)

// TestBackoffCeiling — детерминированная часть формулы D6:
// min(base * factor^(attempts-1), ceiling), включая вырожденные параметры
// и защиту от +Inf при большом номере попытки.
func TestBackoffCeiling(t *testing.T) {
	tests := []struct {
		name     string
		base     time.Duration
		ceiling  time.Duration
		factor   float64
		attempts int
		want     time.Duration
	}{
		{"первая попытка равна базе", time.Second, time.Minute, 2, 1, time.Second},
		{"вторая попытка удваивает базу", time.Second, time.Minute, 2, 2, 2 * time.Second},
		{"третья попытка — четыре базы", time.Second, time.Minute, 2, 3, 4 * time.Second},
		{"шестая попытка ещё ниже потолка", time.Second, time.Minute, 2, 6, 32 * time.Second},
		{"рост упирается в потолок", time.Second, time.Minute, 2, 7, time.Minute},
		{"потолок удерживается дальше", time.Second, time.Minute, 2, 10, time.Minute},
		{"огромный номер попытки не даёт Inf", time.Second, time.Minute, 2, math.MaxInt32, time.Minute},
		{"нулевая база — отсрочки нет", 0, time.Minute, 2, 3, 0},
		{"отрицательная база — отсрочки нет", -time.Second, time.Minute, 2, 3, 0},
		{"номер попытки меньше единицы считается первым", time.Second, time.Minute, 2, 0, time.Second},
		{"множитель 1 — роста нет", time.Second, time.Minute, 1, 5, time.Second},
		{"множитель меньше 1 — роста нет", time.Second, time.Minute, 0.5, 5, time.Second},
		{"база не меньше потолка — сразу потолок", 2 * time.Minute, time.Minute, 2, 3, time.Minute},
		{"потолок не задан — рост ограничен разрядностью", time.Second, 0, 2, 100, time.Duration(math.MaxInt64)},
		{"отрицательный потолок — тоже не задан", time.Second, -time.Second, 2, 100, time.Duration(math.MaxInt64)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := backoffCeiling(tc.base, tc.ceiling, tc.factor, tc.attempts)
			if got != tc.want {
				t.Fatalf("backoffCeiling(%s, %s, %v, %d) = %s, ожидалось %s",
					tc.base, tc.ceiling, tc.factor, tc.attempts, got, tc.want)
			}
			// time.Duration — целое, поэтому NaN/Inf в нём невозможны. Реальный
			// риск — переполнение при вычислении: оно завернулось бы в
			// отрицательное значение вместо потолка.
			if got < 0 {
				t.Fatalf("отсрочка переполнилась: получено %s", got)
			}
		})
	}
}

// TestBackoffCeilingMonotonic — интервал монотонно растёт с номером
// попытки и остаётся на потолке после его достижения.
func TestBackoffCeilingMonotonic(t *testing.T) {
	tests := []struct {
		name        string
		base        time.Duration
		ceiling     time.Duration
		factor      float64
		maxAttempts int
	}{
		{"рост до минутного потолка", time.Second, time.Minute, 2, 12},
		{"рост без потолка", time.Second, 0, 2, 5},
		{"рост с дробным множителем", 100 * time.Millisecond, time.Minute, 1.5, 20},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			prev := time.Duration(0)
			for attempt := 1; attempt <= tc.maxAttempts; attempt++ {
				got := backoffCeiling(tc.base, tc.ceiling, tc.factor, attempt)
				if got < prev {
					t.Fatalf("попытка %d: отсрочка %s меньше предыдущей %s", attempt, got, prev)
				}
				prev = got
			}
			if tc.ceiling > 0 {
				if last := backoffCeiling(tc.base, tc.ceiling, tc.factor, tc.maxAttempts); last != tc.ceiling {
					t.Fatalf("на попытке %d отсрочка %s, ожидался потолок %s",
						tc.maxAttempts, last, tc.ceiling)
				}
			}
		})
	}
}

// TestBackoffDelayJitter — equal-jitter укладывает результат в [d/2, d],
// где d — детерминированная база backoffCeiling; вырожденные случаи
// (нулевая база, отсрочка в 1 нс) не выходят за границы и не паникуют.
func TestBackoffDelayJitter(t *testing.T) {
	tests := []struct {
		name     string
		base     time.Duration
		ceiling  time.Duration
		factor   float64
		attempts int
	}{
		{"первая попытка", time.Second, time.Minute, 2, 1},
		{"третья попытка", time.Second, time.Minute, 2, 3},
		{"попытка на потолке", time.Second, time.Minute, 2, 20},
		{"вырожденная отсрочка в один наносекунд", time.Nanosecond, time.Minute, 2, 1},
		{"нулевая база — отсрочки нет", 0, time.Minute, 2, 1},
	}

	const samples = 500
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := backoffCeiling(tc.base, tc.ceiling, tc.factor, tc.attempts)
			lower := d / 2
			distinct := make(map[time.Duration]struct{}, samples)
			for i := 0; i < samples; i++ {
				got := backoffDelay(tc.base, tc.ceiling, tc.factor, tc.attempts)
				if got < lower || got > d {
					t.Fatalf("джиттер %s вне отрезка [%s, %s]", got, lower, d)
				}
				distinct[got] = struct{}{}
			}
			// На заметной отсрочке джиттер обязан различаться, иначе
			// повторяемость выдаёт отсутствие рандомизации.
			if d >= time.Second && len(distinct) < 2 {
				t.Fatalf("джиттер вырожден: все %d значений равны %s", samples, d)
			}
		})
	}
}
