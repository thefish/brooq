package logger

import (
	"log/slog"
	"testing"
)

func TestNewLevels(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			log, err := New(tt.level, FormatText, "test")
			if err != nil {
				t.Fatalf("New() вернул ошибку: %v", err)
			}
			if log == nil {
				t.Fatal("New() вернул nil-логгер")
			}
		})
	}
}

func TestNewInvalidLevel(t *testing.T) {
	_, err := New("verbose", FormatText, "test")
	if err == nil {
		t.Fatal("New() не вернул ошибку для неизвестного уровня")
	}
}

func TestNewFormats(t *testing.T) {
	for _, format := range []string{FormatJSON, FormatText, "text"} {
		t.Run(format, func(t *testing.T) {
			log, err := New("info", format, "server")
			if err != nil {
				t.Fatalf("New() вернул ошибку: %v", err)
			}
			if log == nil {
				t.Fatal("New() вернул nil-логгер")
			}
		})
	}
}

func TestNewEmptyComponent(t *testing.T) {
	// Пустое имя компонента не должно приводить к ошибке.
	log, err := New("info", FormatText, "")
	if err != nil {
		t.Fatalf("New() вернул ошибку: %v", err)
	}
	if log == nil {
		t.Fatal("New() вернул nil-логгер")
	}
}

func TestParseLevel(t *testing.T) {
	if got, err := ParseLevel("debug"); err != nil || got != slog.LevelDebug {
		t.Errorf("ParseLevel(debug) = %v, %v", got, err)
	}
	if _, err := ParseLevel("trace"); err == nil {
		t.Error("ParseLevel(trace) не вернул ошибку")
	}
}
