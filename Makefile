# Makefile для проекта brooq (generic очередь на Go + PostgreSQL).
# Список целей: make help
#
# Договор сборки: Go и версии инструментов зафиксированы (go.mod +
# deploy/vars.mk), миграции — единственный источник схемы и накатываются
# только шагом деплоя (ADR-0002), деплой — один VPS (ADR-0006).

PROJECT_NAME := "brooq"
PKG := "github.com/thefish/$(PROJECT_NAME)"

GO := $(shell which go)
CWD := $(shell pwd)
CURRENT_DATE := $(shell date +%FT%T%Z)
PKG_VER := $(shell git describe --tags 2>/dev/null || echo dev)

LDFLAGS := "-X \"main.Version=$(PKG_VER) built at $(CURRENT_DATE) on $(shell hostname) with $(shell go version)\""

# Подгрузка переменных из .env, если файл существует (DATABASE_URL).
-include .env
export

# Параметры деплоя (SSH-хост, пути, имена юнитов). Файл локальный и не
# версионируется: скопируйте deploy/vars.mk.example в deploy/vars.mk и
# подставьте свои значения. Отсутствие файла — не ошибка: make соберёт и
# протестирует проект, а deploy потребует VPS_HOST/VPS_USER/VPS_DIR.
-include deploy/vars.mk

# Значения по умолчанию, если deploy/vars.mk нет. ?= не перетирает ни
# переменные из .env (он подключается выше), ни окружение.
VPS_UNITS ?= brooq

MIGRATIONS_DIR := migrations
GOOSE := goose

GOLANGCI_LINT ?= golangci-lint
GOVULNCHECK ?= govulncheck
GOVULNCHECK_OPTS ?= -show color

.PHONY: help bin-deps migrate-up migrate-down migrate-status test lint go-check build spec-validate deploy deploy-units deploy-migrate deploy-restart run test-db-up test-db-down

## help: показать список доступных команд
help:
	@echo "Доступные команды проекта brooq:"
	@echo
	@grep -E '^## ' Makefile | sed 's/^## //' | column -t -s ':'

## bin-deps: инструменты и зависимости (goose, golangci-lint, govulncheck — версии зафиксированы)
# Пин сверяется по версии, а не по наличию в PATH: иначе уже установленный
# инструмент другой версии молча остаётся и «зафиксированная версия» врёт.
bin-deps:
	$(GO) mod download
	$(GO) mod tidy
	@$(GOLANGCI_LINT) version 2>/dev/null | grep -q '$(GOLANGCI_LINT_VERSION)' || $(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@$(GOOSE) -version 2>/dev/null | grep -q '$(GOOSE_VERSION)' || $(GO) install github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION)
	@$(GOVULNCHECK) -version 2>/dev/null | grep -q '$(GOVULNCHECK_VERSION)' || $(GO) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

## migrate-up: применить все неприменённые миграции goose
migrate-up:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$$DATABASE_URL" up

## migrate-down: откатить последнюю применённую миграцию
migrate-down:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$$DATABASE_URL" down

## migrate-status: показать применённые и ожидающие миграции
migrate-status:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$$DATABASE_URL" status

## test: все тесты с рейс-детектором, 2 прогона и покрытием
# Интеграционные тесты (pkg/postgres) пропускаются, если не задана
# TEST_DATABASE_URL: TEST_DATABASE_URL=... make test
#
# MIGRATIONS_DIR для тестов — абсолютный путь ($(abspath …)): go test
# запускает бинарник пакета с рабочим каталогом этого пакета, и
# относительный `migrations` (верный для goose, который зовут из корня
# модуля) там не находится. Перекрываем экспортированное значение только
# на строке запуска тестов — цели migrate-* продолжают получать
# относительный путь.
test:
	MIGRATIONS_DIR=$(abspath $(MIGRATIONS_DIR)) $(GO) test -v ./... -count=2 -race -coverprofile=cover.out.tmp

## lint: проверить код с помощью golangci-lint
lint:
	@$(GOLANGCI_LINT) run ./...

## go-check: проверить зависимости на известные уязвимости
go-check:
	@$(GOVULNCHECK) $(GOVULNCHECK_OPTS) ./...

# BINS — все бинарники проекта (новый юнит = новая строка здесь,
# build подхватит через bin/%).
BINS := brooq

## build: собрать все бинарники BINS в bin/ (под-цель на каждый)
build: $(addprefix bin/,$(BINS))

## bin/<name>: собрать один бинарник (pattern-цель; make bin/brooq).
# FORCE — всегда пересобираем (надёжность деплоя).
bin/%: FORCE
	CGO_ENABLED=0 $(GO) build -o $(CWD)/bin/$* -ldflags $(LDFLAGS) $(CWD)/cmd/$*

# FORCE — маркер «всегда устарело» (идиома форсированной пересборки).
.PHONY: FORCE
FORCE:

## spec-validate: проверить спеки и изменения OpenSpec (strict)
spec-validate:
	openspec validate --all --strict --no-interactive

## run: локальный запуск юнита (из .env/окружения)
run:
	$(GO) run $(CWD)/cmd/brooq

## test-db-up / test-db-down: локальная PostgreSQL для интеграционных тестов (docker compose)
# Порт 55433 (не 5432 — чтобы не конфликтовать с локальным сервером).
# Использование: make test-db-up и
# TEST_DATABASE_URL=postgres://brooq:brooq@localhost:55433/brooq_test?sslmode=disable make test
test-db-up:
	docker compose -f docker-compose.test.yml up -d --wait

test-db-down:
	docker compose -f docker-compose.test.yml down

## deploy-units: скопировать systemd-юниты на VPS (sudo) и перечитать systemd.
# Вызывается deploy'ем после rsync-копий; первичная настройка — тот же путь.
deploy-units:
	@set -e; \
	 echo "=== Install units ==="; \
	 ssh -t $(VPS_USER)@$(VPS_HOST) "sudo install -m 644 $(VPS_DIR)/units/*.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl enable $(VPS_UNITS)"

## deploy: собрать и развернуть на VPS (rsync bin/+миграции+юниты → goose up
# → рестарт юнитов; ADR-0006)
deploy: build
	@set -e; \
	 echo "=== Deploy $(PROJECT_NAME) ==="; \
	 rsync -avz --progress bin/ $(VPS_USER)@$(VPS_HOST):$(VPS_DIR)/bin/; \
	 rsync -avz --progress migrations/ $(VPS_USER)@$(VPS_HOST):$(VPS_DIR)/migrations/; \
	 rsync -avz --progress deploy/vps/*.service $(VPS_USER)@$(VPS_HOST):$(VPS_DIR)/units/; \
	 $(MAKE) deploy-units; \
	 $(MAKE) deploy-migrate; \
	 $(MAKE) deploy-restart; \
	 echo "=== Deploy OK ==="

deploy-migrate:
	ssh -t $(VPS_USER)@$(VPS_HOST) "cd $(VPS_DIR) && set -a && . ./.env && set +a && test -n \"\$$DATABASE_URL\" && goose -dir migrations postgres \"\$$DATABASE_URL\" up"

deploy-restart:
	ssh -t $(VPS_USER)@$(VPS_HOST) "sudo systemctl restart $(VPS_UNITS)"
