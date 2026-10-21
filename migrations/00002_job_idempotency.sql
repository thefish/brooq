-- +goose Up
-- 00002_job_idempotency: расширение схемы под движок очереди.
-- Миграция накатывается шагом деплоя, а не бинарником при старте,
-- и совместима со старым кодом: новые колонка и индексы
-- ничего не ломают, расширенный CHECK принимает все прежние значения.

-- Ключ идемпотентности: производитель может повторить постановку и не
-- создать дубль. NULL означает «ключа нет» — такие задания не ограничены.
ALTER TABLE job ADD COLUMN idempotency_key TEXT;

-- Идемпотентность: не более одного задания на пару (тема, ключ).
-- Частичный, потому что без ключа строки не должны конфликтовать.
CREATE UNIQUE INDEX job_idempotency_idx
    ON job (topic, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Выборка готовых к работе по теме: hot path воркера. job_ready_idx из
-- 00001 не удаляем — он покрывает выборку без фильтра по теме, а 00001
-- не правится; дублирование индексов — принятая цена.
CREATE INDEX job_ready_topic_idx
    ON job (topic, available_at, id)
    WHERE state IN ('pending', 'failed');

-- Перечень состояний: пять -> шесть (добавляется 'canceled').
-- NOT VALID + отдельный VALIDATE. Транзакция одна (goose оборачивает
-- файл), поэтому DROP CONSTRAINT держит ACCESS EXCLUSIVE до COMMIT —
-- включая VALIDATE: окно блокировки не сокращается. NOT VALID здесь
-- нужен для другого: ADD CONSTRAINT не валидирует таблицу сразу, а
-- VALIDATE отдельным оператором можно вынести в отдельный шаг, когда
-- таблица вырастет. Имя ограничения сохраняется прежним.
ALTER TABLE job DROP CONSTRAINT job_state_check;
ALTER TABLE job ADD CONSTRAINT job_state_check
    CHECK (state IN ('pending', 'processing', 'done', 'failed', 'dead', 'canceled'))
    NOT VALID;
ALTER TABLE job VALIDATE CONSTRAINT job_state_check;

-- +goose Down
-- Откат в обратном порядке. Вернуть CHECK на пяти значениях можно
-- только пока в таблице нет строк в состоянии 'canceled': иначе VALIDATE
-- завершится ошибкой. Это защита от потери данных, а не дефект (D10).
ALTER TABLE job DROP CONSTRAINT job_state_check;
ALTER TABLE job ADD CONSTRAINT job_state_check
    CHECK (state IN ('pending', 'processing', 'done', 'failed', 'dead'))
    NOT VALID;
ALTER TABLE job VALIDATE CONSTRAINT job_state_check;

DROP INDEX job_ready_topic_idx;
DROP INDEX job_idempotency_idx;

ALTER TABLE job DROP COLUMN idempotency_key;
