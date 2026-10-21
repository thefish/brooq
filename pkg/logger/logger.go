// Package logger предоставляет обёртку над стандартным log/slog:
// выбор формата вывода (JSON или текст) и уровня логирования,
// привязка имени компонента приложения.
//
// Пакет ничего не знает о домене: только slog-обёртка, которую
// подключают и юнит, и тесты.
package logger

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Форматы вывода логов.
const (
	// FormatText — человекочитаемый текстовый формат (для разработки).
	FormatText = "text"
	// FormatJSON — структурированный JSON (для продакшена).
	FormatJSON = "json"
)

// ParseLevel преобразует строковое представление уровня лога
// (debug, info, warn, error) в slog.Level. Неизвестное значение
// приводит к ошибке.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("неизвестный уровень лога %q (ожидаются: debug, info, warn, error)", s)
	}
}

// New создаёт *slog.Logger с заданным уровнем, форматом вывода
// и привязкой имени компонента (например, "api", "ingress" или "worker").
// Формат: "json" — JSON-вывод, любой другой — текстовый.
func New(level, format, component string) (*slog.Logger, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case FormatJSON:
		handler = slog.NewJSONHandler(os.Stdout, opts)
	default:
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	log := slog.New(handler)
	if component != "" {
		log = log.With("component", component)
	}
	return log, nil
}
