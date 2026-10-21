// Движок очереди: жизненный цикл, регистрация тем и порт хранилища.
// SQL сюда не попадает: пакет работает с готовыми значениями
// и вызывает методы repository через порт Repo.
package brooq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/thefish/brooq/pkg/queue"
)

// Значения по умолчанию для незаполненных настроек движка. Валидация
// конфигурации на старте процесса — задача настроек (п. 6.1); здесь
// значения лишь страхуют от нулей, которые сломали бы цикл.
const (
	defaultWorkers      = 1
	defaultPollInterval = time.Second
	defaultLease        = 30 * time.Second
)

// errQueueStopped — причина отмены контекста при остановке движка.
// Она отделяет плановую остановку от отмены задания: при остановке
// исход в строку не пишется, задание остаётся в processing до истечения
// аренды, и Reaper возвращает его в оборот (D8).
var errQueueStopped = errors.New("движок останавливается")

// Repo — порт хранилища, который нужен движку. Методы дословно
// повторяют методы *postgres.Repository, поэтому хранилище подставлено
// без адаптера, а в тестах — без базы.
type Repo interface {
	// Claim берёт до limit заданий темы для воркера и закрепляет их
	// арендой; попытка списывается при взятии (D4, D5).
	Claim(ctx context.Context, tx queue.Tx, topic queue.Topic, worker string,
		lease time.Duration, limit int) ([]queue.Claim, error)
	// MarkDone записывает успех: done безусловно.
	MarkDone(ctx context.Context, tx queue.Tx, id, worker string) (int64, error)
	// MarkFailed записывает сбой с готовым моментом следующей попытки.
	MarkFailed(ctx context.Context, tx queue.Tx, id, worker, cause string, availableAt time.Time) (int64, error)
	// MarkFailedPreservingAttempt записывает сбой без расхода попытки.
	MarkFailedPreservingAttempt(ctx context.Context, tx queue.Tx, id, worker, cause string, availableAt time.Time) (int64, error)
	// MarkDead записывает сбой, исчерпавший попытки: терминальный исход.
	MarkDead(ctx context.Context, tx queue.Tx, id, worker, cause string) (int64, error)
	// MarkCanceled переводит задание в canceled, включая полёт.
	MarkCanceled(ctx context.Context, tx queue.Tx, id string) (int64, error)
	// RenewLease продлевает аренду задания, если оно всё ещё в
	// processing и аренду держит тот же воркер (D13). 0 строк —
	// аренда потеряна, а не ошибка.
	RenewLease(ctx context.Context, tx queue.Tx, id, worker string, lease time.Duration) (int64, error)
	// ReapExpired возвращает в pending задания с истёкшей арендой
	// (D8) и отдаёт их для журнала.
	ReapExpired(ctx context.Context, tx queue.Tx) ([]queue.ReapedJob, error)
	// DeleteTerminal удаляет до batch терминальных заданий (done, dead,
	// canceled), пролежавших терминальными дольше ttl, и отдаёт их для
	// журнала и метрики. Пустой результат — не ошибка; ttl <= 0 или
	// batch < 1 означают «уборка выключена» и к базе не обращаются.
	DeleteTerminal(ctx context.Context, tx queue.Tx, ttl time.Duration, batch int) ([]queue.DeletedJob, error)
	// Begin открывает транзакцию записи.
	Begin(ctx context.Context) (queue.Tx, error)
	// Enqueue ставит задание через пул.
	Enqueue(ctx context.Context, job queue.Job) error
	// EnqueueTx ставит задание в транзакции вызывающего.
	EnqueueTx(ctx context.Context, tx queue.Tx, job queue.Job) error
	// CountByState — чтение для метрик (D11): число заданий по каждой
	// паре «тема + состояние». Идёт через пул, а не через транзакцию;
	// пустая таблица — пустой результат без ошибки.
	CountByState(ctx context.Context) ([]queue.TopicStateCount, error)
	// OldestReadyAge — чтение для метрик (D11): возраст старейшего
	// готового задания по каждой теме. Тоже через пул;
	// темы без готовых заданий в результат не попадают.
	OldestReadyAge(ctx context.Context) ([]queue.TopicAge, error)
}

// Config — настройки движка, не зависящие от темы. Настройки темы
// (потолок попыток, таймауты, границы отсрочки) приходят в
// RegisterKind вместе с обработчиком.
type Config struct {
	// Workers — число горутин (свободных мест) на каждую тему.
	Workers int
	// PollInterval — базовый период поллинга; фактический тик —
	// PollInterval ±25 % (D7).
	PollInterval time.Duration
	// Lease — срок, на который взятое задание закрепляется арендой.
	Lease time.Duration
	// ReaperPeriod — период возврата истёкших аренд. 0 — вычислить как
	// max(5s, Lease/4) по D8; больше нуля — явный период.
	ReaperPeriod time.Duration
	// WatchdogGrace — грация после отмены контекста попытки, за которую
	// обработчик обязан выйти (D15 п.3). Не вышел — ERROR в лог и
	// brooq_hung_workers_total{topic}. 0 — взять Lease: столько же
	// живёт аренда, после чего задание возвращает Reaper.
	WatchdogGrace time.Duration
	// StatsPeriod — период сбора статистики для gauge-метрик
	// brooq_jobs и brooq_ready_oldest_age_seconds (D11). 0 — дефолт
	// 30s; отрицательный период приводится к дефолту на уровне
	// настроек процесса (QUEUE_STATS_PERIOD).
	StatsPeriod time.Duration
	// RetentionTTL — сколько терминальное задание (done, dead,
	// canceled) живёт после перехода в терминальное состояние. Ноль и
	// меньше — уборка выключена (осознанный выключатель), и горутина
	// уборки не запускается. Дефолта здесь нет: его подставляет
	// сборка процесса (QUEUE_RETENTION_TTL).
	RetentionTTL time.Duration
	// RetentionBatch — размер одной пачки удаления уборки. Меньше 1 —
	// 1000 (New подставляет дефолт).
	RetentionBatch int
	// RetentionPeriod — период уборки терминальных заданий. 0 —
	// вычислить из RetentionTTL (см. retentionPeriod); больше нуля —
	// явный период.
	RetentionPeriod time.Duration
	// Metrics — порт метрик движка. nil допустим: тогда счётчики не
	// ведутся (тесты, сборка без наблюдаемости).
	Metrics Metrics
	// WorkerID — идентификатор воркера для locked_by. Пустое значение —
	// сгенерировать из имени хоста и pid.
	WorkerID string
}

// kind — зарегистрированная тема: обработчик и его настройки.
type kind struct {
	handler queue.Handler
	opts    queue.KindOptions
}

// Queue — движок: набор зарегистрированных тем и горутины-воркеры.
// Нулевое значение непригодно; создавать через New.
type Queue struct {
	repo Repo
	log  *slog.Logger
	cfg  Config
	// worker — базовый идентификатор процесса; в locked_by попадает
	// идентификатор слота, см. workerID.
	worker string

	mu      sync.Mutex
	kinds   map[queue.Topic]kind
	started bool
	closed  bool

	runCtx    context.Context
	cancelRun context.CancelCauseFunc
	stopOnce  sync.Once
	workersWg sync.WaitGroup

	// busyMu и busy — занятые слоты воркеров по темам: источник gauge
	// brooq_worker_busy (D11). Порт метрик не умеет инкремент со
	// знаком, поэтому движок считает занятость сам и публикует
	// абсолютное значение после каждого изменения.
	busyMu sync.Mutex
	busy   map[queue.Topic]int64

	// statsMu и statsPublished — учёт наборов лейблов, опубликованных
	// тикером статистики в прошлый тик: имя gauge → ключ лейблов → сами
	// лейблы. Gauge обязан отражать текущее состояние, поэтому серии,
	// которых в новом тике нет, публикуются нулём: иначе опустевшая
	// тема навсегда оставляет старый возраст старейшего задания и
	// фантомное «processing». Карту двигает горутина статистики, но
	// мьютекс нужен: Set в реестре потокобезопасен, а карта — нет.
	statsMu        sync.Mutex
	statsPublished map[string]map[string]map[string]string
}

// New создаёт движок поверх хранилища. Логгер обязателен (nil
// заменяется slog.Default()), нулевые настройки заменяются значениями по
// умолчанию. Побочных эффектов нет: горутины запускает Start.
func New(repo Repo, log *slog.Logger, cfg Config) *Queue {
	if log == nil {
		log = slog.Default()
	}
	if cfg.Workers < 1 {
		cfg.Workers = defaultWorkers
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.Lease <= 0 {
		cfg.Lease = defaultLease
	}
	if cfg.WatchdogGrace <= 0 {
		// Грация по умолчанию — Lease (D15): столько же живёт аренда,
		// после чего Reaper вправе вернуть задание в оборот.
		cfg.WatchdogGrace = cfg.Lease
	}
	if cfg.RetentionBatch < 1 {
		// Размер пачки — не выключатель: уборка выключается TTL. Ноль
		// здесь — незаданное значение, а не «удалять по нулю».
		cfg.RetentionBatch = defaultRetentionBatch
	}
	worker := cfg.WorkerID
	if worker == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "brooq"
		}
		worker = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
	return &Queue{
		repo:           repo,
		log:            log,
		cfg:            cfg,
		worker:         worker,
		kinds:          make(map[queue.Topic]kind),
		busy:           make(map[queue.Topic]int64),
		statsPublished: make(map[string]map[string]map[string]string),
	}
}

// workerID возвращает идентификатор держателя аренды для слота slot.
// locked_by идентифицирует именно держателя аренды, а не процесс: при
// нескольких слотах один процесс мог бы держать несколько аренд, и guard
// locked_by = $me перестал бы отличать попытку от попытки внутри него
// (слот A просрочил аренду, Reaper вернул задание, слот B его взял —
// heartbeat и Mark* слота A прошли бы по чужому исполнению, D13).
//
// При Workers <= 1 суффикс не добавляется: вид locked_by и строки в БД
// остаются прежними (совместимость с существующими сценариями и тестами).
func (q *Queue) workerID(slot int) string {
	if q.cfg.Workers <= 1 {
		return q.worker
	}
	return fmt.Sprintf("%s-%d", q.worker, slot)
}

// RegisterKind регистрирует обработчик темы вместе с её настройками.
// Вызывается до Start: повторная регистрация темы, пустая тема, nil-овый
// обработчик и MaxAttempts меньше 1 — ошибка на регистрации, а не при
// исполнении (D9).
func (q *Queue) RegisterKind(topic queue.Topic, h queue.Handler, opts queue.KindOptions) error {
	if topic == "" {
		return errors.New("регистрация темы: пустая тема")
	}
	if h == nil {
		return fmt.Errorf("регистрация темы %s: обработчик не задан", topic)
	}
	if opts.MaxAttempts < 1 {
		return fmt.Errorf("регистрация темы %s: потолок попыток должен быть не меньше 1, получено %d",
			topic, opts.MaxAttempts)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started {
		return fmt.Errorf("регистрация темы %s: движок уже запущен", topic)
	}
	if q.closed {
		return fmt.Errorf("регистрация темы %s: движок закрыт", topic)
	}
	if _, ok := q.kinds[topic]; ok {
		return fmt.Errorf("регистрация темы %s: тема уже зарегистрирована", topic)
	}
	q.kinds[topic] = kind{handler: h, opts: opts}
	return nil
}

// Start запускает по Workers горутин на каждую зарегистрированную тему,
// одну горутину сборщика истёкших аренд (D8), сборщик статистики и —
// если задан RetentionTTL — уборку терминальных заданий. Повторный
// вызов и вызов после Close ничего не делают.
func (q *Queue) Start() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.started || q.closed {
		return
	}
	q.started = true
	q.runCtx, q.cancelRun = context.WithCancelCause(context.Background())

	for topic, k := range q.kinds {
		for slot := 0; slot < q.cfg.Workers; slot++ {
			q.workersWg.Add(1)
			go func(topic queue.Topic, k kind, slot int) {
				defer q.workersWg.Done()
				q.runWorker(q.runCtx, topic, slot, k)
			}(topic, k, slot)
		}
	}

	// Сборщик истёкших аренд — один на движок, а не на тему: SQL по теме
	// не фильтрует (D8). Учитывается в том же WaitGroup, поэтому Close
	// дожидается и его.
	q.workersWg.Add(1)
	go func() {
		defer q.workersWg.Done()
		q.runReaper(q.runCtx)
	}()

	// Сборщик статистики — один на движок (D11): он читает все темы
	// сразу и публикует gauge brooq_jobs и
	// brooq_ready_oldest_age_seconds. Гибнет по контексту движка, как
	// воркеры, и учитывается в том же WaitGroup.
	q.workersWg.Add(1)
	go func() {
		defer q.workersWg.Done()
		q.runStats(q.runCtx)
	}()

	// Уборка терминальных заданий — один на движок, как сборщик: SQL по
	// теме не фильтрует. Запускается только когда TTL задан: ноль в
	// RetentionTTL — осознанный выключатель уборки.
	if q.cfg.RetentionTTL > 0 {
		q.workersWg.Add(1)
		go func() {
			defer q.workersWg.Done()
			q.runRetention(q.runCtx)
		}()
	}
}

// Stop останавливает воркеры и дожидается их выхода. Вызов идемпотентен:
// повторный Stop не паникует и снова дожидается горутин. Задания,
// исполнявшиеся в момент остановки, исход не получают — они остаются в
// processing до истечения аренды, дальше их возвращает Reaper (D8).
func (q *Queue) Stop() {
	q.mu.Lock()
	cancel := q.cancelRun
	q.mu.Unlock()
	if cancel == nil {
		return
	}
	q.stopOnce.Do(func() {
		cancel(errQueueStopped)
	})
	q.workersWg.Wait()
}

// Enqueue ставит задание в очередь через пул соединений. Это фасад над
// хранилищем: потребителю не нужно трогать SQL-слой (D9). Задание
// получает состояние pending и становится доступно воркерам.
func (q *Queue) Enqueue(ctx context.Context, job queue.Job) error {
	return q.repo.Enqueue(ctx, job)
}

// EnqueueTx ставит задание в транзакции вызывающего: задание и его
// бизнес-изменения фиксируются вместе — либо всё, либо ничего.
// Транзакцию открывает потребитель (см. Begin) и сам же коммитит.
func (q *Queue) EnqueueTx(ctx context.Context, tx queue.Tx, job queue.Job) error {
	return q.repo.EnqueueTx(ctx, tx, job)
}

// Begin открывает транзакцию записи для постановки задания вместе с
// изменениями потребителя. Закрывать её (Commit/Rollback) обязан
// вызывающий: движок транзакциями потребителя не владеет.
func (q *Queue) Begin(ctx context.Context) (queue.Tx, error) {
	return q.repo.Begin(ctx)
}

// Cancel переводит задание в терминальное состояние canceled (D9, D14):
// открывает транзакцию, помечает задание отменённым и коммитит. Если
// строк не обновлено, задание уже терминальное (done, dead или canceled)
// — возвращается ошибка, а не тихий успех.
//
// Задание, уже взятое воркером, останавливается через потерю аренды:
// heartbeat продлевает аренду только живому заданию, а отменённая строка
// перестаёт её продлевать — продление получает 0 строк, обработчик
// теряет аренду не позднее чем через Lease/3 и отменяется. Исход отмены
// воркер не перезаписывает: его запись идёт с условием по state/аренде,
// строк не обновляется, и задание остаётся canceled (D13, D14).
func (q *Queue) Cancel(ctx context.Context, id string) error {
	tx, err := q.repo.Begin(ctx)
	if err != nil {
		return fmt.Errorf("отмена задания %s: открытие транзакции: %w", id, err)
	}
	n, err := q.repo.MarkCanceled(ctx, tx, id)
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("отмена задания %s: %w", id, err)
	}
	if n == 0 {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("отмена задания %s: задание уже терминальное (done, dead или canceled)", id)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("отмена задания %s: фиксация: %w", id, err)
	}
	return nil
}

// Close останавливает движок и помечает его закрытым: после Close
// работающих горутин не остаётся, а регистрация и запуск новых
// отклоняются. Хранилище движку не принадлежит, поэтому освобождать
// здесь нечего — пул закрывает тот, кто его открыл.
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.mu.Unlock()

	q.Stop()
	return nil
}
