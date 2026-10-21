// Package config загружает конфигурацию приложения из переменных
// окружения и файла .env через koanf. Значения по умолчанию заданы в
// DefaultConfig, приоритет значений: окружение > .env > дефолты.
//
// Обязательное поле одно — DATABASE_URL: очередь живёт в PostgreSQL,
// без базы процесс не стартует (fail-fast).
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/dotenv"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config — конфигурация юнита brooq.
type Config struct {
	// DatabaseURL — DSN подключения к PostgreSQL (обязательное поле).
	DatabaseURL string `koanf:"DATABASE_URL"`
	// HTTPAddr — адрес слушателя служебной поверхности (healthz, metrics).
	HTTPAddr string `koanf:"HTTP_ADDR"`
	// LogLevel — уровень логирования (debug, info, warn, error).
	LogLevel string `koanf:"LOG_LEVEL"`
	// LogFormat — формат лога (json, text).
	LogFormat string `koanf:"LOG_FORMAT"`

	// QueueWorkers — число воркеров (свободных мест) на каждую тему.
	QueueWorkers int `koanf:"QUEUE_WORKERS"`
	// QueuePollInterval — базовый период поллинга готовых заданий.
	QueuePollInterval time.Duration `koanf:"QUEUE_POLL_INTERVAL"`
	// QueueLease — срок, на который взятое задание закрепляется арендой.
	QueueLease time.Duration `koanf:"QUEUE_LEASE"`
	// QueueAttemptTimeout — время, которое даётся одной попытке;
	// истечение — сбой попытки с повтором.
	QueueAttemptTimeout time.Duration `koanf:"QUEUE_ATTEMPT_TIMEOUT"`
	// QueueMaxExecutionTimeout — жёсткий потолок исполнения темы:
	// контекст задания гасится безусловно, даже если обработчик жив.
	QueueMaxExecutionTimeout time.Duration `koanf:"QUEUE_MAX_EXECUTION_TIMEOUT"`
	// QueueBackoffBase — базовый интервал отсрочки до следующей попытки.
	QueueBackoffBase time.Duration `koanf:"QUEUE_BACKOFF_BASE"`
	// QueueBackoffMax — верхняя граница интервала отсрочки.
	QueueBackoffMax time.Duration `koanf:"QUEUE_BACKOFF_MAX"`
	// QueueMaxAttempts — потолок попыток темы: после него задание
	// становится мёртвым (dead).
	QueueMaxAttempts int `koanf:"QUEUE_MAX_ATTEMPTS"`
	// QueueReaperPeriod — период возврата истёкших аренд. Ноль —
	// вычислить как max(5s, Lease/4) по D8; больше нуля — явный период.
	QueueReaperPeriod time.Duration `koanf:"QUEUE_REAPER_PERIOD"`
	// QueueWatchdogGrace — грация после отмены контекста попытки, за
	// которую обработчик обязан выйти (D15); не вышел — ERROR в лог и
	// brooq_hung_workers_total. Ноль — взять QueueLease; отрицательная
	// — ошибка.
	QueueWatchdogGrace time.Duration `koanf:"QUEUE_WATCHDOG_GRACE"`
	// QueueStatsPeriod — период сбора статистики для gauge-метрик
	// brooq_jobs и brooq_ready_oldest_age_seconds (D11). Ноль — дефолт
	// 30s; отрицательный период — ошибка.
	QueueStatsPeriod time.Duration `koanf:"QUEUE_STATS_PERIOD"`
	// QueueRetentionTTL — сколько терминальное задание (done, dead,
	// canceled) живёт после перехода в терминальное состояние. Ноль —
	// уборка выключена (осознанный выключатель, а не ошибка);
	// отрицательный TTL — ошибка.
	QueueRetentionTTL time.Duration `koanf:"QUEUE_RETENTION_TTL"`
	// QueueRetentionBatch — размер одной пачки удаления уборки.
	// Меньше 1 — ошибка: пачкой нулевого размера убирать нечего.
	QueueRetentionBatch int `koanf:"QUEUE_RETENTION_BATCH"`
	// QueueRetentionPeriod — период уборки терминальных заданий. Ноль —
	// вычислить из TTL (сутки, зажатые в [1h, 24h]); отрицательный
	// период — ошибка.
	QueueRetentionPeriod time.Duration `koanf:"QUEUE_RETENTION_PERIOD"`
}

// Значения по умолчанию для настроек движка: расчёт на один VPS.
// Аренда (2m) коротка и продлевается heartbeat-ом (4.6,
// D13): она больше не обязана вмещать всю попытку, поэтому длинная
// работа не теряет аренду. Потолок исполнения заметно больше времени
// попытки.
const (
	defaultQueueWorkers             = 1
	defaultQueuePollInterval        = time.Second
	defaultQueueLease               = 2 * time.Minute
	defaultQueueAttemptTimeout      = time.Minute
	defaultQueueMaxExecutionTimeout = 5 * time.Minute
	defaultQueueBackoffBase         = time.Second
	defaultQueueBackoffMax          = time.Minute
	defaultQueueMaxAttempts         = 5
	// defaultQueueReaperPeriod — 0 означает «вычислить из QUEUE_LEASE»:
	// период сборщика — max(5s, Lease/4) по D8, и заставлять
	// пользователя задавать его незачем.
	defaultQueueReaperPeriod = 0
	// defaultQueueWatchdogGrace — 0 означает «взять QUEUE_LEASE»: грация
	// watchdog не должна переживать саму аренду (D15).
	defaultQueueWatchdogGrace = 0
	// defaultQueueStatsPeriod — 0 означает «взять дефолт движка» (30s по
	// D11): сбор статистики — фоновая работа, и заставлять пользователя
	// задавать её период незачем.
	defaultQueueStatsPeriod = 0
	// defaultQueueRetentionTTL — 30 дней: столько терминальное задание
	// живёт после перехода в терминальное состояние. Ноль в этом поле
	// означает «уборка выключена», поэтому дефолт задан здесь, а не в
	// движке: движок ноль не подставляет.
	defaultQueueRetentionTTL = 720 * time.Hour
	// defaultQueueRetentionBatch — размер пачки удаления: одна пачка
	// держит блокировки недолго, а цикл добирает остаток.
	defaultQueueRetentionBatch = 1000
	// defaultQueueRetentionPeriod — 0 означает «вычислить из TTL»:
	// период уборки — сутки, зажатые в [1h, 24h], и задавать его
	// пользователю незачем.
	defaultQueueRetentionPeriod = 0
)

// Options — параметры загрузки конфигурации.
type Options struct {
	// DotenvPath — путь к файлу .env. Пустая строка означает
	// файл ".env" в текущей рабочей директории.
	DotenvPath string
}

// DefaultConfig возвращает конфигурацию со значениями по умолчанию.
func DefaultConfig() Config {
	return Config{
		HTTPAddr:                 "127.0.0.1:8090",
		LogLevel:                 "info",
		LogFormat:                "text",
		QueueWorkers:             defaultQueueWorkers,
		QueuePollInterval:        defaultQueuePollInterval,
		QueueLease:               defaultQueueLease,
		QueueAttemptTimeout:      defaultQueueAttemptTimeout,
		QueueMaxExecutionTimeout: defaultQueueMaxExecutionTimeout,
		QueueBackoffBase:         defaultQueueBackoffBase,
		QueueBackoffMax:          defaultQueueBackoffMax,
		QueueMaxAttempts:         defaultQueueMaxAttempts,
		QueueReaperPeriod:        defaultQueueReaperPeriod,
		QueueWatchdogGrace:       defaultQueueWatchdogGrace,
		QueueStatsPeriod:         defaultQueueStatsPeriod,
		QueueRetentionTTL:        defaultQueueRetentionTTL,
		QueueRetentionBatch:      defaultQueueRetentionBatch,
		QueueRetentionPeriod:     defaultQueueRetentionPeriod,
	}
}

// Load загружает конфигурацию: сначала из файла .env (если он
// существует), затем из переменных окружения, которые имеют приоритет.
// Обязательное поле DATABASE_URL проверяется функцией Validate.
func Load(opts Options) (*Config, error) {
	path := opts.DotenvPath
	if path == "" {
		path = ".env"
	}

	k := koanf.New(".")

	// Файл .env — значения по умолчанию из репозитория.
	if _, err := os.Stat(path); err == nil {
		if err := k.Load(file.Provider(path), dotenv.Parser()); err != nil {
			return nil, fmt.Errorf("загрузка файла %s: %w", path, err)
		}
	}

	// Переменные окружения имеют приоритет над .env. Пустое значение
	// считается незаданным (`KEY=` в .env или в systemd EnvironmentFile
	// означает «не настроено», а не «пустая строка вместо дефолта»).
	if err := k.Load(env.Provider("", ".", func(key string) string {
		if strings.TrimSpace(os.Getenv(key)) == "" {
			return ""
		}
		return key
	}), nil); err != nil {
		return nil, fmt.Errorf("загрузка переменных окружения: %w", err)
	}

	cfg := DefaultConfig()
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("разбор конфигурации: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate проверяет наличие обязательных переменных окружения и
// корректность настроек движка. Возвращает ошибку с перечислением
// недостающих переменных и отклонённых значений: ноль и отрицательные
// значения периодов — ошибка старта (fail-fast), а не повод молча взять
// дефолт.
func (c *Config) Validate() error {
	var missing []string
	if strings.TrimSpace(c.DatabaseURL) == "" {
		missing = append(missing, "DATABASE_URL")
	}
	if len(missing) > 0 {
		return fmt.Errorf(
			"отсутствуют обязательные переменные окружения: %s (задайте их в окружении или файле .env)",
			strings.Join(missing, ", "),
		)
	}

	var problems []string
	if c.QueueWorkers < 1 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_WORKERS должен быть не меньше 1, получено %d", c.QueueWorkers))
	}
	if c.QueueMaxAttempts < 1 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_MAX_ATTEMPTS должен быть не меньше 1, получено %d", c.QueueMaxAttempts))
	}
	periods := []struct {
		name  string
		value time.Duration
	}{
		{"QUEUE_POLL_INTERVAL", c.QueuePollInterval},
		{"QUEUE_LEASE", c.QueueLease},
		{"QUEUE_ATTEMPT_TIMEOUT", c.QueueAttemptTimeout},
		{"QUEUE_MAX_EXECUTION_TIMEOUT", c.QueueMaxExecutionTimeout},
		{"QUEUE_BACKOFF_BASE", c.QueueBackoffBase},
		{"QUEUE_BACKOFF_MAX", c.QueueBackoffMax},
	}
	for _, p := range periods {
		if p.value <= 0 {
			problems = append(problems, fmt.Sprintf(
				"%s должен быть больше нуля, получено %s", p.name, p.value))
		}
	}
	// Ноль в QUEUE_REAPER_PERIOD — не ошибка, а «вычислить из аренды»;
	// отрицательный период — ошибка.
	if c.QueueReaperPeriod < 0 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_REAPER_PERIOD должен быть больше нуля или не задан (0 — расчёт из QUEUE_LEASE), получено %s",
			c.QueueReaperPeriod))
	}
	// Ноль в QUEUE_WATCHDOG_GRACE — не ошибка, а «взять QUEUE_LEASE»;
	// отрицательная грация — ошибка (таймер с ней бессмысленен).
	if c.QueueWatchdogGrace < 0 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_WATCHDOG_GRACE не должен быть отрицательным (0 — грация равна QUEUE_LEASE), получено %s",
			c.QueueWatchdogGrace))
	}
	// Ноль в QUEUE_STATS_PERIOD — не ошибка, а «взять дефолт 30s» (D11);
	// отрицательный период — ошибка (таймер с ним бессмысленен).
	if c.QueueStatsPeriod < 0 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_STATS_PERIOD не должен быть отрицательным (0 — период 30s по умолчанию), получено %s",
			c.QueueStatsPeriod))
	}
	// Ноль в QUEUE_RETENTION_TTL — не ошибка, а выключатель уборки:
	// пользователь вправе держать терминальные строки вечно.
	// Отрицательный TTL — ошибка: с ним уборка бессмысленна.
	if c.QueueRetentionTTL < 0 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_RETENTION_TTL не должен быть отрицательным (0 — уборка выключена), получено %s",
			c.QueueRetentionTTL))
	}
	if c.QueueRetentionBatch < 1 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_RETENTION_BATCH должен быть не меньше 1, получено %d", c.QueueRetentionBatch))
	}
	// Ноль в QUEUE_RETENTION_PERIOD — не ошибка, а «вычислить из TTL»;
	// отрицательный период — ошибка (таймер с ним бессмысленен).
	if c.QueueRetentionPeriod < 0 {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_RETENTION_PERIOD не должен быть отрицательным (0 — расчёт из QUEUE_RETENTION_TTL), получено %s",
			c.QueueRetentionPeriod))
	}
	if c.QueueAttemptTimeout > 0 && c.QueueMaxExecutionTimeout > 0 &&
		c.QueueAttemptTimeout > c.QueueMaxExecutionTimeout {
		problems = append(problems, fmt.Sprintf(
			"QUEUE_ATTEMPT_TIMEOUT (%s) не должен превышать QUEUE_MAX_EXECUTION_TIMEOUT (%s)",
			c.QueueAttemptTimeout, c.QueueMaxExecutionTimeout))
	}
	// Ограничение QUEUE_ATTEMPT_TIMEOUT <= QUEUE_LEASE снято (D13): с
	// heartbeat-ом (4.6) аренда продлевается, и длинная работа больше не
	// теряет её. Единственный верхний ориентир попытки — потолок
	// исполнения темы.
	if len(problems) > 0 {
		return fmt.Errorf("некорректные настройки движка: %s", strings.Join(problems, "; "))
	}
	return nil
}
