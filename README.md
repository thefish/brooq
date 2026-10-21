# brooq — очередь заданий на PostgreSQL

## Что это

Очередь заданий без внешнего брокера: задания лежат в PostgreSQL, воркеры
берут их конкурентно (`FOR UPDATE SKIP LOCKED`), исход пишется в ту же
строку. Один бинарник, один VPS, наблюдаемость с первого дня.

brooq — **библиотека**: она компилируется в процесс потребителя и делит
с ним транзакцию и пул соединений. Отдельный
процесс-воркер — следствие, а не суть: `cmd/brooq` использует ту же
библиотеку.

Три обещания разной силы:

- **взятие** — не более одного раза в каждый момент;
- **исполнение** — хотя бы один раз (обработчик обязан быть
  идемпотентным);
- **запись исхода** — ровно один раз; судьба задания читается одним
  `SELECT`.

## Зачем

Уведомления и фоновые действия в моём личном проекте шли через
push-контур «api → notification-service → транспорт»: состояние задания
жило в PostgreSQL, а исполнение — в памяти процесса, из-за чего доставка
была плохо отслеживаемой и необслуживаемой. brooq выносит очередь в
отдельный проект: Postgres — точка правды, воркеры берут строки сами,
всё видно SQL-запросом и метриками.

## Быстрый старт

```sh
make bin-deps                 # go mod download + зафиксированные версии инструментов
make test-db-up               # локальная PostgreSQL для интеграционных тестов (порт 55433)
TEST_DATABASE_URL='postgres://brooq:brooq@localhost:55433/brooq_test?sslmode=disable' make test
make build                    # bin/brooq
make lint
make spec-validate            # openspec validate --all --strict
```

Зелёный `make test` **без** `TEST_DATABASE_URL` не доказывает, что
миграции применяются: интеграционные тесты и e2e тогда пропускаются.

Локальный запуск юнита: скопируйте `.env.example` в `.env`, задайте
`DATABASE_URL`, затем `make migrate-up && make run`. Служебная
поверхность — `127.0.0.1:8090` (`/healthz`, `/metrics`).

### Как библиотека

```go
import (
	"github.com/thefish/brooq/pkg/brooq"    // движок
	"github.com/thefish/brooq/pkg/postgres" // SQL-адаптер
	"github.com/thefish/brooq/pkg/queue"    // словарь: Job, Claim, Handler
)

store := postgres.New(pool)          // пул открывает потребитель
engine := brooq.New(store, log, brooq.Config{Workers: 4, Lease: 30 * time.Second})
_ = engine.RegisterKind(queue.Topic("email"), handler{}, queue.KindOptions{
	MaxAttempts: 5, AttemptTimeout: time.Minute, MaxExecutionTimeout: 5 * time.Minute,
	BackoffBase: time.Second, BackoffMax: time.Minute,
})
engine.Start()
defer engine.Close()

// Задание — в том числе в своей транзакции:
err := engine.Enqueue(ctx, queue.Job{Topic: "email", Payload: payload, IdempotencyKey: "order-42"})
```

Подробное руководство с примерами — `docs/queue-engine.md`.

## Документы

| Файл | Что внутри |
|------|------------|
| `docs/architecture.md` | архитектура: устройство, гарантии, ограничения |
| `docs/queue-engine.md` | руководство по движку: примеры, состояния, эксплуатация |
| `deploy/README.md` | деплой: юнит systemd, переменные, миграции |

## Состояние

Движок работает целиком: постановка, взятие, запись исхода, повтор с
backoff, `Reaper`, heartbeat, отмена в полёте, watchdog и девять метрик
`brooq_*`.

Чего пока нет:

- **HTTP-API очереди** — отмена есть (`Queue.Cancel`), эндпоинта нет;
- **приоритеты** — порядок строго FIFO по `available_at, id`;
- **партиционирования** — при очень больших объёмах понадобится
  разбиение по времени вместо удаления строками.

Терминальные задания (`done`, `dead`, `canceled`) удаляются по TTL —
по умолчанию через 30 дней (`QUEUE_RETENTION_TTL`), пачками, с
настроенным под это autovacuum.

- **ретенция терминальных строк**: ключ идемпотентности не
  освобождается, таблица растёт;
- **HTTP-API очереди** — отмена есть (`Queue.Cancel`), эндпоинта нет;
- **приоритеты** — порядок строго FIFO по `available_at, id`.

Устройство пакетов и правила импортов — `docs/architecture.md`:

```
pkg/queue      словарь (только stdlib)
pkg/brooq      движок: воркеры, Reaper, heartbeat, метрики
pkg/postgres   SQL-адаптер (единственное место, где живёт PostgreSQL)
cmd/brooq      композиция: собрать зависимости, запустить
```

Схема — только миграции `migrations/`, накатываются шагом
деплоя, никогда из бинарника при старте.
