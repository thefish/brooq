package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeDotenv пишет временный .env и возвращает путь к нему.
func writeDotenv(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), ".env")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

// noDotenv — путь к несуществующему файлу: тест не зависит от .env
// разработчика (приоритет источников проверяется отдельным тестом).
func noDotenv(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "нет-файла")
}

func TestDefaults(t *testing.T) {
	t.Setenv("HTTP_ADDR", "")
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("LOG_FORMAT", "")
	t.Setenv("DATABASE_URL", "postgres://localhost/brooq")

	cfg, err := Load(Options{DotenvPath: noDotenv(t)})
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:8090", cfg.HTTPAddr)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "text", cfg.LogFormat)
}

func TestEnvOverridesDotenv(t *testing.T) {
	path := writeDotenv(t, "DATABASE_URL=postgres://dotenv/db\nHTTP_ADDR=127.0.0.1:1111\n")
	t.Setenv("DATABASE_URL", "postgres://env/db")

	cfg, err := Load(Options{DotenvPath: path})
	require.NoError(t, err)
	assert.Equal(t, "postgres://env/db", cfg.DatabaseURL, "окружение приоритетнее .env")
	assert.Equal(t, "127.0.0.1:1111", cfg.HTTPAddr, "значение из .env подхвачено")
}

func TestMissingDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")

	_, err := Load(Options{DotenvPath: noDotenv(t)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DATABASE_URL")
}

// queueEnvKeys — все ключи настроек движка: перед прогоном кейса они
// очищаются, чтобы результат не зависел от окружения разработчика.
var queueEnvKeys = []string{
	"QUEUE_WORKERS",
	"QUEUE_POLL_INTERVAL",
	"QUEUE_LEASE",
	"QUEUE_ATTEMPT_TIMEOUT",
	"QUEUE_MAX_EXECUTION_TIMEOUT",
	"QUEUE_BACKOFF_BASE",
	"QUEUE_BACKOFF_MAX",
	"QUEUE_MAX_ATTEMPTS",
	"QUEUE_WATCHDOG_GRACE",
}

// TestQueueDefaults — значения по умолчанию настроек движка (расчёт на
// один VPS) и их согласованность с Validate.
func TestQueueDefaults(t *testing.T) {
	// DSN задаётся явно: дефолтов у обязательного поля нет, а без него
	// Validate падает на DATABASE_URL раньше проверки настроек движка.
	cfg := DefaultConfig()
	cfg.DatabaseURL = "postgres://localhost/brooq"
	tests := []struct {
		name  string
		check func(t *testing.T, cfg Config)
	}{
		{"воркеров по умолчанию один", func(t *testing.T, cfg Config) {
			assert.Equal(t, 1, cfg.QueueWorkers)
		}},
		{"период поллинга — секунда", func(t *testing.T, cfg Config) {
			assert.Equal(t, time.Second, cfg.QueuePollInterval)
		}},
		{"срок аренды — две минуты", func(t *testing.T, cfg Config) {
			assert.Equal(t, 2*time.Minute, cfg.QueueLease,
				"без heartbeat аренда обязана вмещать всю попытку (D13)")
		}},
		{"время попытки — минута", func(t *testing.T, cfg Config) {
			assert.Equal(t, time.Minute, cfg.QueueAttemptTimeout)
		}},
		{"аренда длиннее времени попытки", func(t *testing.T, cfg Config) {
			assert.GreaterOrEqual(t, cfg.QueueLease, cfg.QueueAttemptTimeout,
				"иначе задание живёт в processing с истёкшей арендой (D13)")
		}},
		{"потолок исполнения — пять минут", func(t *testing.T, cfg Config) {
			assert.Equal(t, 5*time.Minute, cfg.QueueMaxExecutionTimeout)
		}},
		{"база отсрочки — секунда", func(t *testing.T, cfg Config) {
			assert.Equal(t, time.Second, cfg.QueueBackoffBase)
		}},
		{"потолок отсрочки — минута", func(t *testing.T, cfg Config) {
			assert.Equal(t, time.Minute, cfg.QueueBackoffMax)
		}},
		{"потолок попыток — пять", func(t *testing.T, cfg Config) {
			assert.Equal(t, 5, cfg.QueueMaxAttempts)
		}},
		{"дефолты проходят собственную валидацию", func(t *testing.T, cfg Config) {
			require.NoError(t, cfg.Validate())
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, cfg)
		})
	}
}

// TestQueueEnvOverrides — каждая настройка движка читается из окружения и
// перекрывает значение по умолчанию.
func TestQueueEnvOverrides(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		check func(t *testing.T, cfg *Config)
	}{
		{"число воркеров", map[string]string{"QUEUE_WORKERS": "4"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 4, cfg.QueueWorkers)
		}},
		{"период поллинга", map[string]string{"QUEUE_POLL_INTERVAL": "250ms"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 250*time.Millisecond, cfg.QueuePollInterval)
		}},
		{"срок аренды", map[string]string{"QUEUE_LEASE": "45s", "QUEUE_ATTEMPT_TIMEOUT": "30s"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 45*time.Second, cfg.QueueLease)
			// Время попытки задаётся вместе с арендой: дефолтная минута не
			// влезла бы в 45s, и валидация отклонила бы пару (D13).
			assert.Equal(t, 30*time.Second, cfg.QueueAttemptTimeout)
		}},
		{"время попытки", map[string]string{"QUEUE_ATTEMPT_TIMEOUT": "90s"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 90*time.Second, cfg.QueueAttemptTimeout)
		}},
		{"потолок исполнения", map[string]string{"QUEUE_MAX_EXECUTION_TIMEOUT": "10m"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 10*time.Minute, cfg.QueueMaxExecutionTimeout)
		}},
		{"база отсрочки", map[string]string{"QUEUE_BACKOFF_BASE": "2s"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 2*time.Second, cfg.QueueBackoffBase)
		}},
		{"потолок отсрочки", map[string]string{"QUEUE_BACKOFF_MAX": "3m"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 3*time.Minute, cfg.QueueBackoffMax)
		}},
		{"потолок попыток", map[string]string{"QUEUE_MAX_ATTEMPTS": "9"}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 9, cfg.QueueMaxAttempts)
		}},
		{"пустое значение считается незаданным", map[string]string{"QUEUE_MAX_ATTEMPTS": ""}, func(t *testing.T, cfg *Config) {
			assert.Equal(t, 5, cfg.QueueMaxAttempts, "пустая переменная не должна обнулять дефолт")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range queueEnvKeys {
				t.Setenv(key, "")
			}
			t.Setenv("DATABASE_URL", "postgres://localhost/brooq")
			for key, value := range tc.env {
				t.Setenv(key, value)
			}

			cfg, err := Load(Options{DotenvPath: noDotenv(t)})
			require.NoError(t, err)
			tc.check(t, cfg)
		})
	}
}

// TestValidateQueueSettings — некорректные настройки движка отклоняются
// на старте (fail-fast), а не подменяются дефолтом молча.
func TestValidateQueueSettings(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(cfg *Config)
		wantErr []string
	}{
		{"воркеров ноль отклоняется", func(cfg *Config) {
			cfg.QueueWorkers = 0
		}, []string{"QUEUE_WORKERS"}},
		{"отрицательное число воркеров отклоняется", func(cfg *Config) {
			cfg.QueueWorkers = -3
		}, []string{"QUEUE_WORKERS"}},
		{"потолок попыток ноль отклоняется", func(cfg *Config) {
			cfg.QueueMaxAttempts = 0
		}, []string{"QUEUE_MAX_ATTEMPTS"}},
		{"нулевой период поллинга отклоняется", func(cfg *Config) {
			cfg.QueuePollInterval = 0
		}, []string{"QUEUE_POLL_INTERVAL"}},
		{"отрицательный срок аренды отклоняется", func(cfg *Config) {
			cfg.QueueLease = -time.Second
		}, []string{"QUEUE_LEASE"}},
		{"нулевое время попытки отклоняется", func(cfg *Config) {
			cfg.QueueAttemptTimeout = 0
		}, []string{"QUEUE_ATTEMPT_TIMEOUT"}},
		{"отрицательный потолок исполнения отклоняется", func(cfg *Config) {
			cfg.QueueMaxExecutionTimeout = -time.Minute
		}, []string{"QUEUE_MAX_EXECUTION_TIMEOUT"}},
		{"нулевая база отсрочки отклоняется", func(cfg *Config) {
			cfg.QueueBackoffBase = 0
		}, []string{"QUEUE_BACKOFF_BASE"}},
		{"отрицательный потолок отсрочки отклоняется", func(cfg *Config) {
			cfg.QueueBackoffMax = -time.Second
		}, []string{"QUEUE_BACKOFF_MAX"}},
		{"время попытки больше потолка исполнения отклоняется", func(cfg *Config) {
			cfg.QueueAttemptTimeout = 10 * time.Minute
		}, []string{"QUEUE_MAX_EXECUTION_TIMEOUT"}},
		{"время попытки больше аренды допустимо", func(cfg *Config) {
			// Ограничение AttemptTimeout <= QueueLease снято (D13): аренда
			// продлевается heartbeat-ом (4.6).
			cfg.QueueAttemptTimeout = cfg.QueueLease + time.Second
		}, nil},
		{"время попытки равно аренде допустимо", func(cfg *Config) {
			cfg.QueueAttemptTimeout = cfg.QueueLease
		}, nil},
		{"время попытки равно потолку исполнения допустимо", func(cfg *Config) {
			cfg.QueueAttemptTimeout = cfg.QueueMaxExecutionTimeout
		}, nil},
		{"несколько проблем перечисляются сразу", func(cfg *Config) {
			cfg.QueueWorkers = 0
			cfg.QueueLease = 0
		}, []string{"QUEUE_WORKERS", "QUEUE_LEASE"}},
		{"дефолты допустимы", func(*Config) {}, nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.DatabaseURL = "postgres://localhost/brooq"
			tc.mutate(&cfg)

			err := cfg.Validate()
			if len(tc.wantErr) == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tc.wantErr {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}
