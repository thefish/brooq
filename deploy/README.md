# Деплой brooq

Один VPS, один systemd-юнит, PostgreSQL на том же хосте.
Миграции — отдельный шаг деплоя, не часть старта юнита.

## Раскладка на сервере

```
/opt/brooq/
├── bin/brooq            # статический бинарник (CGO_ENABLED=0)
├── migrations/          # каталог goose (rsync с деплоем)
├── units/brooq.service  # копируется в /etc/systemd/system/
└── .env                 # несекретные настройки (chmod 600)
/etc/brooq/credentials/  # секреты для LoadCredential (вне репозитория)
```

## Юнит

| Юнит | Бинарник | Слушает | Зависит от | Публичный |
|------|----------|---------|------------|-----------|
| `brooq` | `bin/brooq` | `127.0.0.1:8090` (`HTTP_ADDR`) — `/healthz`, `/metrics` | postgresql | нет (только loopback) |

`brooq.service` ужесточён: `NoNewPrivileges`, `PrivateTmp`,
`ProtectSystem=strict`, `ProtectHome`. Секреты — только через
`LoadCredential=`, не через `EnvironmentFile`.

## Переменные

| Переменная | Обязательна | Назначение |
|------------|-------------|------------|
| `DATABASE_URL` | да | DSN PostgreSQL; без него юнит не стартует |
| `HTTP_ADDR` | нет | адрес служебной поверхности (по умолчанию `127.0.0.1:8090`) |
| `LOG_LEVEL` | нет | `debug`/`info`/`warn`/`error` |
| `LOG_FORMAT` | нет | `json` (прод) / `text` |

Полный список с комментариями — `.env.example`.

## Порядок деплоя

```sh
make deploy     # build → rsync bin/+migrations/+units → install units
                # → goose up → systemctl restart brooq
```

Шаги по отдельности: `make build`, `make deploy-units`,
`make deploy-migrate`, `make deploy-restart`.

Параметры подключения — `VPS_HOST`, `VPS_USER`, `VPS_DIR`, `VPS_UNITS`.

Первый раз скопируйте шаблон и подставьте свои значения:

```sh
cp deploy/vars.mk.example deploy/vars.mk
```

`deploy/vars.mk` не версионируется (в `.gitignore`): это локальные
параметры конкретного хоста. Значения можно задать и через `.env` или
окружение — они подключаются раньше и перекрывают файл:
`VPS_HOST=host make deploy`.

Без `vars.mk` проект собирается и тестируется как обычно; `deploy`
потребует задать `VPS_HOST`/`VPS_USER`/`VPS_DIR`.

## Откат

1. Вернуть предыдущий `bin/brooq` (rsync из локальной сборки нужного тега).
2. `sudo systemctl restart brooq`.
3. Откат схемы — только если новая миграция несовместима:
   `goose -dir migrations postgres "$DATABASE_URL" down` (по одной версии,
   осознанно — данные при этом теряются).

## Проверка после деплоя

```sh
ssh <host> 'systemctl status brooq --no-pager'
ssh <host> 'curl -fsS http://127.0.0.1:8090/healthz'      # {"status":"ok"}
ssh <host> 'curl -fsS http://127.0.0.1:8090/metrics | head'
```

`/healthz` отвечает 503, если БД недоступна: это liveness-проба, она не
должна врать «всё хорошо» при мёртвой базе.
