// Package brooq — оркестрация очереди: политика повторов, цикл поллинга,
// воркеры. SQL сюда не попадает: пакет работает с готовыми
// значениями и передаёт их в repository.
package brooq

import (
	"math"
	"math/rand/v2" //nolint:gosec // джиттер не криптографический: G404 здесь не применим
	"time"
)

// backoffDelay возвращает отсрочку до следующей попытки: базовая формула
// d = min(base * factor^(attempts-1), ceiling), затем equal-jitter —
// результат укладывается в [d/2, d] (D6).
//
// Параметры: base — базовый интервал (BackoffBase), ceiling — верхняя
// граница (BackoffMax), factor — множитель роста (обычно 2), attempts —
// номер состоявшейся попытки, начиная с 1.
//
// Jitter равномерный в правой половине отрезка, а не full-jitter: полный
// джиттер может дать почти нулевую паузу и мгновенный повтор при
// недоступном внешнем сервисе.
func backoffDelay(base, ceiling time.Duration, factor float64, attempts int) time.Duration {
	d := backoffCeiling(base, ceiling, factor, attempts)
	if d <= 0 {
		return d
	}
	// lower = d/2 (для нечётного d — округление вниз), верх — ровно d.
	lower := d / 2
	// rand.N требует строго положительный аргумент; для d = 1 нс
	// отрезок вырожден в {0, 1}, что по-прежнему лежит в [d/2, d].
	return lower + rand.N(d-lower+1) //nolint:gosec // джиттер задержки, не криптография: math/rand здесь уместен
}

// backoffCeiling — детерминированная часть формулы без джиттера:
// min(base * factor^(attempts-1), ceiling).
//
// Защита от переполнения выполняется ДО возведения в степень (D6):
// сначала показатель сравнивается с порогом достижения потолка
// attempts-1 >= log(ceiling/base)/log(factor). Без этой проверки
// math.Pow при больших attempts улетает в +Inf, и интервал становится
// мусором.
//
// Договорённости: base <= 0 — отсрочки нет (0). factor <= 1 — роста нет,
// возвращается base (с учётом потолка). ceiling <= 0 — потолок не задан,
// результат ограничен только разрядностью time.Duration, но никогда не
// Inf/NaN.
func backoffCeiling(base, ceiling time.Duration, factor float64, attempts int) time.Duration {
	if base <= 0 {
		return 0
	}
	if attempts < 1 {
		attempts = 1
	}
	if factor <= 1 || attempts == 1 {
		return clampDelay(base, ceiling)
	}
	// Потолок не выше базы: расти некуда.
	if ceiling > 0 && base >= ceiling {
		return ceiling
	}

	// Порог достижения потолка — сравнение ДО math.Pow.
	if ceiling > 0 {
		limit := math.Log(float64(ceiling) / float64(base))
		if float64(attempts-1)*math.Log(factor) >= limit {
			return ceiling
		}
	}

	d := float64(base) * math.Pow(factor, float64(attempts-1))
	if math.IsNaN(d) || math.IsInf(d, 0) || d >= float64(math.MaxInt64) {
		// Недостижимо при корректном ceiling, но страхует от мусора
		// вместо интервала.
		if ceiling > 0 {
			return ceiling
		}
		return time.Duration(math.MaxInt64)
	}

	return clampDelay(time.Duration(d), ceiling)
}

// clampDelay ограничивает отсрочку потолком; ceiling <= 0 — потолок не
// задан.
func clampDelay(d, ceiling time.Duration) time.Duration {
	if ceiling > 0 && d > ceiling {
		return ceiling
	}
	return d
}
