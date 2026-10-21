# brooq — движок очереди: как пользоваться

Практическое руководство по движку. Устройство, гарантии и ограничения —
в [`architecture.md`](architecture.md).

## 1. Быстрый старт

Пять шагов от пустой базы до пойманного задания.

### Шаг 1. Накатить миграции

```bash
make test-db-up                    # локальная PostgreSQL для проб
export DATABASE_URL='postgres://brooq:brooq@localhost:55433/brooq_test?sslmode=disable'
make migrate-up
make migrate-status
```

Ожидаемый вывод: применены `00001_job` и `00002_job_idempotency`.
Повторный `make migrate-up` — no-op. Миграции накатываются **шагом
деплоя**, никогда из бинарника при старте.

### Шаг 2. Задать настройки

Всё через окружение или `.env`, дефолты уже рабочие:

```dotenv
QUEUE_WORKERS=2
QUEUE_POLL_INTERVAL=1s
QUEUE_LEASE=2m
QUEUE_ATTEMPT_TIMEOUT=1m
QUEUE_MAX_EXECUTION_TIMEOUT=5m
QUEUE_BACKOFF_BASE=1s
QUEUE_BACKOFF_MAX=1m
QUEUE_MAX_ATTEMPTS=5
```

Невалидные значения (ноль, отрицательное, `AttemptTimeout > Lease`)
отклоняются **на старте** с перечислением всех проблем сразу.

### Шаг 3. Написать обработчик

```go
package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/thefish/brooq/pkg/queue"
)

// notification — тело задания; схему знает только потребитель.
type notification struct {
	UserID string `json:"user_id"`
	Text   string `json:"text"`
}

type notifyHandler struct {
	log *slog.Logger
}

// Handle исполняет одно задание. Обработчик обязан быть идемпотентным:
// исполнение может повториться (аренда истекла, запись исхода не прошла).
func (h notifyHandler) Handle(ctx context.Context, job queue.Claim) error {
	var msg notification
	if err := json.Unmarshal(job.Payload, &msg); err != nil {
		// Постоянная ошибка: повтор не поможет, но и отменить задание
		// нельзя — пусть попытки исчерпаются и задание станет dead.
		return err
	}

	if err := send(ctx, msg); err != nil {
		// Временная ошибка внешнего API: повторить позже, не расходуя
		// попытку (попытка уже списана при взятии — вернём её).
		return queue.ErrMustPreserveAttempt
	}

	h.log.Info("уведомление отправлено", "job", job.ID, "attempt", job.Attempt)
	return nil
}
```

### Шаг 4. Собрать движок

```go
// Хранилище — публичный SQL-адаптер; пул открывает потребитель
// (pkg/db — удобный помощник, но подойдёт любой *pgxpool.Pool).
store := postgres.New(pool)

// Движок собирается из хранилища, логгера и настроек.
engine := brooq.New(store, log, brooq.Config{
	Workers:      2,
	PollInterval: time.Second,
	Lease:        2 * time.Minute,
})

// Регистрация темы — до Start: ошибки настроек видны на старте.
if err := engine.RegisterKind(queue.Topic("notification"), notifyHandler{log}, queue.KindOptions{
	MaxAttempts:         5,
	AttemptTimeout:      time.Minute,
	MaxExecutionTimeout: 5 * time.Minute,
	BackoffBase:         time.Second,
	BackoffMax:          time.Minute,
}); err != nil {
	return err
}

engine.Start()
defer engine.Close()
```

### Шаг 5. Положить задание и увидеть исход

```go
// Задание ставится через фасад — SQL-адаптер трогать не нужно.
err := engine.Enqueue(ctx, queue.Job{
	Topic:   queue.Topic("notification"),
	Payload: []byte(`{"user_id":"u-42","text":"привет"}`),
	// Ключ идемпотентности необязателен: с ним повторная постановка
	// не создаст второе задание.
	IdempotencyKey: "notify-u-42-2026-10-20",
})
```

Воркер заберёт задание и запишет исход в ту же строку; `attempts`
станет `1`, `state` — `done`.

Импорты потребителя — только публичные пакеты:

```go
import (
	"github.com/thefish/brooq/pkg/brooq"    // движок: New, Config, Queue
	"github.com/thefish/brooq/pkg/postgres" // SQL-адаптер для PostgreSQL
	"github.com/thefish/brooq/pkg/queue"    // словарь: Job, Claim, Handler, сигналы
)
```

Ни один `internal/*` не нужен: это и есть смысл публичного фасада.

Готовый пример целиком — `cmd/brooq/main.go` (там тема `demo` и
демонстрационный обработчик).

Проверка вживую:

```bash
make build
DATABASE_URL='postgres://brooq:brooq@localhost:55433/brooq_test?sslmode=disable' ./bin/brooq &
sleep 2
psql "$DATABASE_URL" -c "INSERT INTO job (id, topic, payload, idempotency_key)
  VALUES (gen_random_uuid(), 'demo', '{\"msg\":\"привет\"}'::jsonb, 'demo-1')"
sleep 2
psql "$DATABASE_URL" -c "SELECT state, attempts FROM job WHERE idempotency_key='demo-1'"
#  state | attempts
# -------+----------
#  done  |        1
```

---

## 2. Публичная поверхность

Три публичных пакета — и больше ничего не нужно потребителю:

| Пакет | Что даёт | Импортирует |
|---|---|---|
| `pkg/queue` | словарь: `Job`, `Claim`, `Handler`, `Tx`, `State`, сигналы | только stdlib |
| `pkg/brooq` | движок: `New`, `Config`, `Queue`, `RegisterKind`, `Start/Stop/Close` | `pkg/queue` |
| `pkg/postgres` | SQL-адаптер: `New(pool)`, `Repository` | `pkg/queue`, `pgx` |

Направление импортов — только вниз: `pkg/brooq` не знает
PostgreSQL (работает через порт `Repo`), `pkg/queue` не знает ни того,
ни другого. Поэтому движок тестируется без базы, а хранилище
подменяется.

Фасад движка:

```go
type Queue struct{ /* ... */ }

func New(repo Repo, log *slog.Logger, cfg Config) *Queue
func (q *Queue) RegisterKind(topic queue.Topic, h queue.Handler, opts queue.KindOptions) error

// Постановка — не нужно трогать SQL-адаптер напрямую.
func (q *Queue) Enqueue(ctx context.Context, job queue.Job) error
func (q *Queue) EnqueueTx(ctx context.Context, tx queue.Tx, job queue.Job) error
func (q *Queue) Begin(ctx context.Context) (queue.Tx, error)

// Административная отмена (D9, D14).
func (q *Queue) Cancel(ctx context.Context, id string) error

func (q *Queue) Start()
func (q *Queue) Stop()
func (q *Queue) Close() error
```

Словарь (`pkg/queue`) — ниже.

```go
// Задание для постановки.
type Job struct {
	Topic          Topic
	Payload        Payload         // []byte, непрозрачный JSON
	IdempotencyKey string          // "" = ключа нет
	AvailableAt    time.Time       // нулевое = сейчас
}

// Взятое задание, которое видит обработчик.
type Claim struct {
	ID      string
	Topic   Topic
	Payload Payload
	Attempt int                    // номер состоявшейся попытки, с 1
	Lease   Lease                  // кто держит и до какого момента
	// OnSucceeded — колбэк для постановки следующего задания в той же
	// транзакции. ВНИМАНИЕ: движок его пока не заполняет (4.4), поле
	// всегда nil.
	OnSucceeded func(ctx context.Context, tx Tx) error
}

type Handler interface {
	Handle(ctx context.Context, job Claim) error
}

// Настройки темы: задаются в коде, а не колонкой таблицы.
type KindOptions struct {
	MaxAttempts         int
	AttemptTimeout      time.Duration
	MaxExecutionTimeout time.Duration
	BackoffBase         time.Duration
	BackoffMax          time.Duration
}
```

---

## 3. Состояния

| Состояние | Что значит |
|---|---|
| `pending` | ждёт взятия; возможно, до `available_at` |
| `processing` | взято и закреплено арендой |
| `failed` | **ожидание повтора**: сбой записан, попытки не исчерпаны |
| `done` | исполнено успешно; терминальное |
| `dead` | попытки исчерпаны; терминальное |
| `canceled` | отменено; терминальное |

```
                   ┌──────────── done  (успех, безусловно)
                   │
pending ──взятие──▶ processing ──┬──────── dead  (сбой на последней попытке)
   ▲                   │         │
   │                   │         └──────── canceled (сигнал отмены / внешняя отмена)
   │                   │
   └── Reaper ◀────────┤ аренда истекла  → pending (§ 10)
                       │
                       └── failed ──(available_at)──▶ берётся снова
```

Ключевое: `failed` — это **очередь ожидания**, а не приговор. Задание
берётся из `pending` **и** `failed`, когда наступил `available_at`.

Терминальные состояния не перезаписываются: все записи исхода идут
условным `UPDATE` с guard-ом `WHERE state = 'processing' AND locked_by = $me`,
и «ноль изменённых строк» — штатный результат, а не ошибка.

---

## 4. Политика повторов

### Попытка списывается при взятии

`attempts` увеличивается в том же `UPDATE`, который закрепляет аренду.
Значит `Claim.Attempt` — уже номер **состоявшейся** попытки, начиная с 1.

### Отсрочка

```
d = min(BackoffBase · 2^(attempts-1), BackoffMax)
фактическая отсрочка = равномерно из [d/2, d]
```

Джиттер разводит воркеров, которые иначе ударили бы по внешнему API
одновременно. Защита от переполнения стоит **до** возведения в степень:
при большом номере попытки результат равен потолку, без `Inf`.

Пример при `BackoffBase=1s`, `BackoffMax=1m`:

| Попытка | `d` | Фактически |
|---|---|---|
| 1 | 1s | 0.5s … 1s |
| 2 | 2s | 1s … 2s |
| 3 | 4s | 2s … 4s |
| 7 | 64s → потолок 1m | 30s … 1m |

### Три сигнала обработчика

```go
import "github.com/thefish/brooq/pkg/queue"

// 1. «Повтори позже, вот через столько» — не расходует попытку сверх
//    уже списанной и не уменьшает счётчик. Отсрочка клампится к
//    BackoffMax. На последней попытке задание всё равно станет dead.
return queue.MustDelay(30 * time.Second)

// 2. «Сбой не должен расходовать попытку» — задание вернётся в failed,
//    attempts уменьшится обратно (но не ниже нуля), dead недостижим.
return queue.ErrMustPreserveAttempt

// 3. «Задание стало неактуальным» — canceled независимо от остатка
//    попыток, обработка прекращается.
return queue.ErrMustCancelTask
```

Ошибки оборачиваемы — `fmt.Errorf("отправка: %w", queue.ErrMustCancelTask)`
работает так же. Проверить отсрочку в своём коде:

```go
if d, ok := queue.DelayOf(err); ok {
	log.Info("обработчик попросил отсрочку", "delay", d)
}
```

### Обычная ошибка

Возврат любой другой ошибки — сбой попытки: `failed` + backoff, либо
`dead`, если попытки исчерпаны. Причина пишется в `last_error`
(без секретов — это ваша ответственность).

### Паника

Паника обработчика перехватывается: задание получает исход сбоя,
воркер продолжает работать, процесс не завершается. Стек пишется в лог
уровнем `ERROR`.

---

## 5. Причины прерывания контекста

Движок различает таймаут и отмену через `context.WithCancelCause`
(D15). Обработчику **не нужно** разбирать причину — достаточно
возвращать `ctx.Err()`.

| Причина | Что делает движок |
|---|---|
| `context.DeadlineExceeded` (истёк `AttemptTimeout`) | `failed` + backoff |
| `queue.ErrMaxExecutionExceeded` (потолок темы) | `failed` + backoff |
| `queue.ErrLeaseLost` (аренда потеряна) | ничего не пишет — строку уже изменил отменяющий |

```go
func (h handler) Handle(ctx context.Context, job queue.Claim) error {
	if err := h.doWork(ctx); err != nil {
		if ctx.Err() != nil {
			// Отмена или таймаут: исход определит движок по context.Cause.
			return ctx.Err()
		}
		return err
	}
	return nil
}
```

`ErrMaxExecutionExceeded` **не** делает задание мёртвым: `dead`
достигается только исчерпанием попыток.

---

## 6. Производитель

### Обычная постановка

```go
err := repo.Enqueue(ctx, queue.Job{
	Topic:   queue.Topic("notification"),
	Payload: payload,
})
```

### В транзакции производителя (главный смысл «библиотеки»)

Задание фиксируется вместе с вашими изменениями — атомарно:

```go
tx, err := repo.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx) // безопасно: после Commit — no-op

// 1. Меняем своё состояние.
if _, err := tx.Exec(ctx, `UPDATE account SET notified = true WHERE id = $1`, id); err != nil {
	return err
}

// 2. Кладём задание в той же транзакции.
if err := repo.EnqueueTx(ctx, tx, queue.Job{
	Topic:          queue.Topic("notification"),
	Payload:        payload,
	IdempotencyKey: "welcome-" + id,
}); err != nil {
	return err
}

// 3. Фиксируем: либо оба изменения видны, либо ни одного.
return tx.Commit(ctx)
```

Если транзакция откатится — задания не будет. Если зафиксируется —
воркер его увидит.

### Идемпотентность

```go
// Повтор с тем же ключом → второго задания нет (ON CONFLICT DO NOTHING).
repo.Enqueue(ctx, queue.Job{Topic: "notification", Payload: p, IdempotencyKey: "order-42-paid"})
repo.Enqueue(ctx, queue.Job{Topic: "notification", Payload: p, IdempotencyKey: "order-42-paid"})
// В таблице одна строка.
```

Правила:

- ключ уникален **внутри темы**: одинаковый ключ у разных тем — два
  разных задания;
- пустая строка `""` означает «ключа нет» (превращается в `NULL`), и
  такие задания **не** дедуплицируются;
- ключ из пробелов `"   "` — настоящий ключ;
- ключ **не освобождается** после терминального состояния: повтор
  через месяц с тем же ключом не создаст задание. Ключ описывает
  конкретную попытку доставки — для новой доставки нужен новый ключ.

---

## 7. Отмена задания

```go
// Отменяет и ждущее, и исполняющееся задание.
err := engine.Cancel(ctx, jobID)
```

Из `psql` — то же самое:

```sql
UPDATE job SET state = 'canceled', updated_at = now()
WHERE id = '...' AND state IN ('pending', 'failed', 'processing');
```

### Как отмена доходит до работающего обработчика

Механизм — потеря аренды, без `LISTEN/NOTIFY` (D14):

1. отменяющий переводит строку в `canceled`;
2. следующий heartbeat воркера не проходит guard `state='processing'`
   и возвращает **0 строк**;
3. воркер вызывает `cancelCause(ErrLeaseLost)`, обработчик получает
   `ctx.Done()`, воркер **не пишет в строку ничего** — исход уже записан
   отменяющим.

Задержка реакции — **не дольше `Lease/3`** (период продления).

```
отмена ──▶ строка canceled ──▶ heartbeat: 0 строк ──▶ ErrLeaseLost ──▶ ctx.Done()
                                      (≤ Lease/3)                        │
                                                                         ▼
                                                     воркер НЕ пишет исход
```

Обработчик, уважающий `ctx`, завершится сам. Обработчик, игнорирующий
`ctx`, продолжит работу (горутину в Go убить нельзя) — тогда сработает
watchdog: `ERROR` в лог и `brooq_hung_workers_total`. Его запись исхода
получит 0 строк и не перезапишет отмену.

---

## 8. Настройки

| Переменная | Поле | Дефолт | Смысл |
|---|---|---|---|
| `QUEUE_WORKERS` | `QueueWorkers` | `1` | горутин (свободных мест) на тему |
| `QUEUE_POLL_INTERVAL` | `QueuePollInterval` | `1s` | базовый период поллинга, фактический ±25 % |
| `QUEUE_LEASE` | `QueueLease` | `2m` | срок аренды взятого задания |
| `QUEUE_ATTEMPT_TIMEOUT` | `QueueAttemptTimeout` | `1m` | сколько даётся одной попытке |
| `QUEUE_MAX_EXECUTION_TIMEOUT` | `QueueMaxExecutionTimeout` | `5m` | жёсткий потолок темы |
| `QUEUE_BACKOFF_BASE` | `QueueBackoffBase` | `1s` | база отсрочки |
| `QUEUE_BACKOFF_MAX` | `QueueBackoffMax` | `1m` | потолок отсрочки |
| `QUEUE_MAX_ATTEMPTS` | `QueueMaxAttempts` | `5` | потолок попыток |
| `QUEUE_REAPER_PERIOD` | `QueueReaperPeriod` | `0` (расчёт) | период сборщика; `0` = `max(5s, Lease/4)` |
| `QUEUE_WATCHDOG_GRACE` | `QueueWatchdogGrace` | `0` (= `Lease`) | грация watchdog после отмены |
| `QUEUE_STATS_PERIOD` | `QueueStatsPeriod` | `0` (= 30s) | период сбора статистики для gauge |
| `QUEUE_RETENTION_TTL` | `QueueRetentionTTL` | `720h` (30 дней) | сколько терминальное задание живёт после перехода в терминальное; `0` — уборка выключена |
| `QUEUE_RETENTION_BATCH` | `QueueRetentionBatch` | `1000` | размер пачки удаления |
| `QUEUE_RETENTION_PERIOD` | `QueueRetentionPeriod` | `0` (расчёт) | период уборки; `0` = сутки, зажатые в `[1h, 24h]` |

Валидация на старте отклоняет:

- `QUEUE_WORKERS < 1`, `QUEUE_MAX_ATTEMPTS < 1`, `QUEUE_RETENTION_BATCH < 1`;
- любой период `<= 0` — кроме `QUEUE_RETENTION_PERIOD`, где `0` значит «расчёт»;
- `QUEUE_RETENTION_TTL` отрицательный; **ноль допустим** и означает
  «уборка выключена»;
- `AttemptTimeout > MaxExecutionTimeout`;
- любой период `<= 0`;
- `AttemptTimeout > MaxExecutionTimeout`;
- ~~`AttemptTimeout > Lease`~~ — **ограничение снято**: аренда
  продлевается heartbeat-ом, поэтому длинная работа больше не теряет
  аренду. Следствие для эксплуатации: чем короче `Lease`, тем быстрее
  `Reaper` вернёт задание упавшего воркера, но тем чаще продления.

Голые числа без единиц (`QUEUE_LEASE=120`) отклоняются: `time.Duration`
требует суффикс (`120s`).

### Как выбирать `Lease`

`Lease` больше не ограничивает длительность работы: пока обработчик жив,
воркер продлевает аренду каждые `Lease/3`. Поэтому `Lease` — это
**скорость восстановления**:

- короткий `Lease` (10–30s) — задание упавшего воркера вернётся быстро,
  но продления идут чаще (больше запросов к БД);
- длинный `Lease` (2–5m) — меньше нагрузки, но падение стоит до `Lease`
  простоя задания.

`WatchdogGrace` — сколько ждать выхода обработчика после отмены, прежде
чем зафиксировать зависание. По умолчанию равен `Lease`.

---

## 9. Схема и наблюдение

### Колонки

| Колонка | Смысл |
|---|---|
| `id` | UUID задания |
| `topic` | тема (маршрутизация) |
| `payload` | тело, `JSONB`, непрозрачно для очереди |
| `state` | одно из шести состояний, `CHECK job_state_check` |
| `attempts` | сколько раз задание уже брали |
| `available_at` | раньше этого времени не брать |
| `locked_by` | кто держит аренду (id воркера) |
| `locked_until` | конец аренды |
| `last_error` | причина последнего сбоя (без секретов) |
| `idempotency_key` | ключ идемпотентности, `NULL` = нет |
| `created_at` / `updated_at` | время постановки / последнего изменения |

### Индексы

| Индекс | Зачем |
|---|---|
| `job_ready_idx` | выборка готовых без фильтра по теме |
| `job_ready_topic_idx` | `(topic, available_at, id) WHERE state IN ('pending','failed')` — hot path воркера |
| `job_lease_idx` | истёкшие аренды (`WHERE state='processing'`) |
| `job_topic_idx` | счётчики по теме |
| `job_idempotency_idx` | уникальность `(topic, idempotency_key)` |

### Полезные запросы

```sql
-- Судьба задания одним запросом — главное обещание очереди.
SELECT state, attempts, last_error, available_at, locked_by, locked_until
FROM job WHERE id = '...';

-- Что сейчас в полёте и кто держит.
SELECT id, topic, locked_by, locked_until, attempts,
       locked_until < now() AS lease_expired
FROM job WHERE state = 'processing' ORDER BY locked_until;

-- Глубина очереди по темам.
SELECT topic, state, count(*) FROM job GROUP BY topic, state ORDER BY 1, 2;

-- Возраст старейшего готового задания (лаг очереди).
SELECT topic, now() - min(available_at) AS oldest_ready
FROM job WHERE state IN ('pending','failed') AND available_at <= now()
GROUP BY topic;

-- Мёртвые задания: разбирать руками.
SELECT id, topic, attempts, last_error, updated_at
FROM job WHERE state = 'dead' ORDER BY updated_at DESC;
```

### `Reaper`: что происходит с зависшими заданиями

`Reaper` работает внутри каждого процесса: одна горутина, период
`max(5s, Lease/4)`, переопределяется `QUEUE_REAPER_PERIOD`. Он делает
ровно то, что описано в D8:

```sql
UPDATE job
SET state = 'pending', locked_by = NULL, locked_until = NULL,
    available_at = now(), updated_at = now()
WHERE state = 'processing' AND locked_until < now()
RETURNING id, topic;
```

Почему именно так:

- **`attempts` не меняется**: истечение аренды — признак упавшего
  процесса, а не записанного сбоя (инвариант 7). Поэтому падение не
  приближает задание к `dead`;
- **`available_at = now()`, без дополнительной отсрочки**: сама аренда
  уже была ожиданием — задание не могло вернуться раньше срока. Вторая
  задержка была бы двойной;
- **guard `state = 'processing'`** делает возврат идемпотентным и не
  даёт воскресить терминальные строки (у них тоже остался
  `locked_until` с прошлой аренды);
- **несколько реплик безопасны**: таблицы-мьютекса не нужно, второй
  одновременный сборщик увидит строку уже в `pending` и ничего не
  вернёт повторно.

Смотреть, что именно вернулось: в логе `Info` с `count` и `topics`.
Сколько заданий прямо сейчас исполняется и кто их держит — запрос
«что сейчас в полёте» выше.

Если всё же нужно вернуть задание вручную (например, сборщик отключён):

```sql
-- ВНИМАНИЕ: безопасно только если аренда действительно истекла.
-- attempts не меняется — истечение аренды попыткой не считается.
UPDATE job
SET state = 'pending', locked_by = NULL, locked_until = NULL, updated_at = now()
WHERE state = 'processing' AND locked_until < now();
```

Не запускайте это в цикле с малым периодом: задание, чья аренда только
что истекла, может исполняться живым, но медленным воркером. Его
запись исхода после возврата в оборот получит 0 строк (guard по
`locked_by`), то есть работа не будет засчитана — задание исполнится
повторно. Это ровно то окно, ради устранения которого нужен heartbeat
(4.6).
---

### Уборка терминальных строк (ретенция)

Терминальные задания (`done`, `dead`, `canceled`) удаляются по TTL:
по умолчанию через 30 дней после перехода в терминальное состояние.
Зачем: без уборки таблица и её индексы растут бесконечно, а ключ
идемпотентности никогда не освобождается.

```sql
DELETE FROM job
WHERE id IN (
    SELECT id
    FROM job
    WHERE state IN ('done', 'dead', 'canceled')
      AND updated_at < now() - $1::interval   -- TTL
    ORDER BY updated_at
    LIMIT $2                                  -- размер пачки
    FOR UPDATE SKIP LOCKED
)
RETURNING id, topic;
```

Почему так:

- **`updated_at` как возраст.** Каждый оператор исхода выставляет его,
  поэтому «старше TTL» и «терминальное дольше TTL» — одно и то же;
- **пачками, а не всё сразу.** Один большой `DELETE` держит блокировки и
  даёт пик мёртвых строк. Горутина удаляет до `QUEUE_RETENTION_BATCH`
  строк в одной транзакции и повторяет, пока пачка приходит полной;
- **`FOR UPDATE SKIP LOCKED`.** Несколько процессов или реплик убирают
  разные строки, а не ждут друг друга;
- **`ORDER BY updated_at`.** Сначала самое старое, чтобы отставшая
  уборка не копила хвост;
- **активные строки не трогаются** — в выборке только терминальные
  состояния, а `pending`/`processing`/`failed` не удаляются никогда,
  даже с очень старым `updated_at`.

**Выключение.** `QUEUE_RETENTION_TTL=0` отключает уборку целиком:
горутина не запускается. Это защита от случайного `DELETE` терминальной
части таблицы — ноль в TTL никогда не означает «удалить всё».

**Цикл ограничен** сотней пачек за тик. Если накопление больше, остаток
уйдёт следующим тиком, а в лог придёт `Warn` — лучше, чем один
бесконечный проход, который незаметно держит нагрузку.

**Autovacuum.** Миграция `00003` понижает порог для таблицы `job`
(`autovacuum_vacuum_scale_factor = 0.02`, `threshold = 100`): при дефолтных
20 % мёртвых строк очередь с постоянным потоком вставок и удалений
получает редкие и очень долгие проходы. Частые короткие проходы дешевле.

Сколько удалено: метрика `brooq_retention_deleted_total{topic}` и запись
`Info` в логе при непустой уборке.

Проверить, что удаляется именно то, что нужно:

```sql
-- Кандидаты на удаление: столько строк уборка заберёт в ближайший тик.
SELECT state, count(*), min(updated_at) AS oldest
FROM job
WHERE state IN ('done', 'dead', 'canceled')
  AND updated_at < now() - interval '30 days'
GROUP BY state;
```

Удалить вручную (например, уборка выключена):

```sql
DELETE FROM job
WHERE state IN ('done', 'dead', 'canceled')
  AND updated_at < now() - interval '30 days';
```

Помните: ключ идемпотентности освобождается вместе со строкой. Если
потребитель полагался на то, что повтор с тем же ключом не создаст
задание, то после уборки такой повтор создаст новое — это ожидаемое
поведение TTL, а не дефект.

---

### Метрики

`/metrics` отдаёт десять метрик движка (плюс HTTP-метрики поверхности):

| Метрика | Тип | Что показывает |
|---|---|---|
| `brooq_jobs{topic,state}` | gauge | сколько заданий в каждом состоянии |
| `brooq_ready_oldest_age_seconds{topic}` | gauge | возраст старейшего готового — **главный SLO** |
| `brooq_claim_total{topic,outcome}` | counter | взятия: `claimed` / `empty` |
| `brooq_settle_total{topic,outcome}` | counter | исходы: `done` / `retry` / `dead` / `canceled` |
| `brooq_attempt_duration_seconds_sum{topic}` | counter | суммарная длительность обработчиков |
| `brooq_attempt_duration_seconds_count{topic}` | counter | число исполнений |
| `brooq_lease_expired_total{topic}` | counter | возвраты истёкших аренд (`Reaper`) |
| `brooq_lease_lost_total{topic}` | counter | потеря аренды (отмена/перехват) |
| `brooq_hung_workers_total{topic}` | counter | зависшие обработчики (watchdog) |
| `brooq_worker_busy{topic}` | gauge | занятые слоты |
| `brooq_retention_deleted_total{topic}` | counter | сколько терминальных строк удалено уборкой |

Средняя длительность попытки = `_sum / _count`. Гистограмм нет
сознательно: реестр без корзин, а пара sum/count покрывает среднее и
темп.

`brooq_ready_oldest_age_seconds` обнуляется, когда готовых заданий нет —
поэтому «растёт» однозначно означает «очередь не успевает», без ложных
срабатываний после опустошения темы.

Типовые алерты:

```promql
# Очередь встала: старейшее готовое ждёт больше 5 минут.
brooq_ready_oldest_age_seconds > 300

# Задания умирают: растут dead-исходы.
rate(brooq_settle_total{outcome="dead"}[5m]) > 0

# Обработчики зависают.
increase(brooq_hung_workers_total[15m]) > 0

# Все слоты заняты — очередь не разгружается воркерами.
brooq_worker_busy == on(topic) brooq_jobs{state="processing"}
```

---

### Цепочка заданий (`Chain`)

Обработчик может поставить следующее задание **в той же транзакции**,
которой закрывается текущее (D9). Это главный смысл формы «библиотека»:
если запись следующего задания не удастся, текущее не считается
закрытым — оба изменения откатятся вместе.

```go
func (h handler) Handle(ctx context.Context, job queue.Claim) error {
	if err := h.step(ctx, job); err != nil {
		return err
	}
	// Следующий шаг фиксируется вместе с закрытием текущего.
	job.Chain(queue.Job{
		Topic:          queue.Topic("email"),
		Payload:        nextPayload,
		IdempotencyKey: "step-2-" + job.ID,
	})
	return nil
}
```

Цепочка срабатывает только при успехе: при сбое или отмене следующее
задание не ставится.

---

## 10. Гарантии

Три обещания разной силы — их важно не смешивать:

1. **Взятие — «не более одного раза в каждый момент».** Ни в какой
   момент задание не закреплено за двумя воркерами. Обеспечено одной
   SQL-строкой с `FOR NO KEY UPDATE SKIP LOCKED` и guard-ом `locked_by`
   при записи исхода.
2. **Исполнение — «хотя бы один раз».** Обработчик может быть запущен
   повторно — в том числе без чьего-либо решения (аренда истекла,
   запись исхода не прошла). **Обработчик обязан быть идемпотентным.**
   «Ровно один раз» достижимо только для эффектов внутри той же БД, но
   не для внешних (письмо, HTTP).
3. **Запись исхода — «ровно один раз».** Терминальные состояния не
   перезаписываются; судьба задания читается одним SQL-запросом.

### Чего очередь не делает

- не гарантирует порядок между процессами (внутри темы — FIFO по
  `available_at`, затем `id`);
- не даёт приоритетов;
- не ретраит внутренние запросы к БД: сбой запроса — сбой операции;
- не удаляет терминальные строки: ретенция — отдельный change, поэтому
  ключ идемпотентности не освобождается, а таблица растёт;
- не запускает горутины и не открывает соединения без явного вызова.

---

## 11. Использование как библиотеки

brooq импортируется из чужого модуля: движок и адаптер публичны
(`pkg/brooq`, `pkg/postgres`), в `internal/` осталась только
инфраструктура самого юнита. Доказательство — тест в пакете
`brooq_test`, который собирает очередь снаружи и проводит задание до
`done`, не импортируя ни одного `internal/*`.

Минимальный потребитель:

```go
package main

import (
	"context"
	"log/slog"

	"github.com/thefish/brooq/pkg/brooq"
	"github.com/thefish/brooq/pkg/db"
	"github.com/thefish/brooq/pkg/postgres"
	"github.com/thefish/brooq/pkg/queue"
)

func main() {
	ctx := context.Background()
	pool, err := db.Connect(ctx, dsn, db.DefaultParams())
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	engine := brooq.New(postgres.New(pool), slog.Default(), brooq.Config{
		Workers: 4, Lease: 2 * time.Minute,
	})
	if err := engine.RegisterKind(queue.Topic("email"), handler{}, queue.KindOptions{
		MaxAttempts: 5, AttemptTimeout: time.Minute,
		MaxExecutionTimeout: 5 * time.Minute,
		BackoffBase: time.Second, BackoffMax: time.Minute,
	}); err != nil {
		panic(err)
	}

	engine.Start()
	defer engine.Close()
	select {} // или свой цикл/signal.NotifyContext
}
```

### Что от потребителя требуется

- **своя таблица `job`**: миграции — шаг деплоя, бинарник их
  не накатывает. Файлы `migrations/00001`, `00002` — часть контракта
  схемы, а не деталь реализации;
- **идемпотентный обработчик**: исполнение «хотя бы один раз» — не
  лозунг, а следствие аренды и повторов (§ 11);
- **своё соединение с PostgreSQL**: пул открывает потребитель,
  движок его не создаёт и не закрывает.

### Чего в фасаде нет

- HTTP-API очереди — отмена есть (`Queue.Cancel`), но эндпоинта нет;
- приоритетов, шардирования, изоляции по версии приложения;
- управления схемой: ретенция и партиционирование — отдельные задачи.

`pkg/queue` при этом остаётся замороженным контрактом: переименование
поля в `Job` или `Claim` ломает потребителя.



---

## 12. Тесты

```bash
make test                      # без базы: интеграционные и e2e — SKIP
make test-db-up
TEST_DATABASE_URL='postgres://brooq:brooq@localhost:55433/brooq_test?sslmode=disable' make test
```

**Зелёный `make test` без `TEST_DATABASE_URL` не доказывает, что
миграции применяются** — `pkg/postgres` и `pkg/brooq`
пропускаются. С базой тесты создают изолированную схему, накатывают
goose и убирают её.

Что покрыто:

| Пакет | Что проверяется |
|---|---|
| `pkg/queue` | словарь и состояния, `errors.Is/As` сигналов, разбор `context.Cause` |
| `pkg/brooq` | маппинг исходов (16 кейсов), паника, `OnSucceeded` в одной транзакции, пустая очередь, идемпотентный `Stop`/`Close`, отсутствие утечки горутин, отсутствие наложения итераций, backoff, фасад (`Enqueue`/`EnqueueTx`/`Begin`/`Cancel`), сборщик на уровне движка (возврат при работающем движке, живая аренда, два движка, многократное истечение) |
| `pkg/postgres` | схема и индексы, дедупликация, guard терминальности и владения, контракт взятия (тема, `available_at`, `failed`, порядок, `limit`, аренда, `SKIP LOCKED`), откат транзакции, `Reaper` |
| `pkg/brooq` (e2e) | **рейс**: два движка на одно задание → ровно один вызов; **happy path**: постановка в транзакции → взятие → исход одним `SELECT`; **библиотечный тест** из `brooq_test` — сборка очереди без `internal/*` |

Два e2e — по решению владельца: гонка взятия и happy path. Остальное —
unit-тесты в табличном стиле, без базы.

Полный гейт перед завершением:

```bash
make build && make test && make lint && make spec-validate
```

---

## 13. Что дальше

Движок закрыт: все задачи change'а `01-queue-engine` выполнены, ретенция
терминальных строк тоже. Осталось осознанно отложенное:

1. **HTTP-API очереди** — отмена есть (`Queue.Cancel`), но эндпоинта и
   авторизации нет; отмена из `psql` или из кода потребителя.
2. **Приоритеты** — порядок строго FIFO по `available_at, id`.
3. **Партиционирование таблицы** — уборка по TTL решает проблему
   роста, но при очень больших объёмах понадобится разбиение по
   времени: тогда старые партиции отбрасываются целиком, без `DELETE`.
4. **Шардирование и изоляция по версии приложения** — не нужны на
   текущем масштабе.

Что стоит знать про эксплуатацию:

- `Lease` теперь задаёт не «сколько может длиться работа» (аренда
  продлевается), а **скорость восстановления** после падения воркера:
  задание упавшего процесса вернётся через `Lease`;
- зависший обработчик, игнорирующий `ctx`, удерживает слот до
  перезапуска процесса — горутину в Go убить нельзя.
  Наблюдается через `brooq_hung_workers_total`;
- `brooq_ready_oldest_age_seconds` — главный SLO: рост означает, что
  очередь не успевает. Значение обнуляется, когда готовых заданий нет;
- **TTL задаёт окно идемпотентности**: после удаления строки ключ
  освобождается, и повторная постановка с тем же ключом создаст новое
  задание. Если повтор должен отсекаться дольше, увеличьте
  `QUEUE_RETENTION_TTL` или держите окно в самом потребителе.

