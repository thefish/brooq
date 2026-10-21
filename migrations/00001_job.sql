-- +goose Up
-- 00001_job: таблица заданий очереди. Это СКЕЛЕТ: набор
-- колонок — минимум, без которого очередь не работает (состояние,
-- попытки, время готовности, аренда строки воркером). API очереди,
-- точный набор состояний и правила повторов фиксирует первый change
-- движка; после этого правки — только НОВОЙ миграцией.
CREATE TABLE job (
    id           UUID PRIMARY KEY,
    topic        TEXT NOT NULL,                 -- вид задания (маршрутизация воркеров)
    payload      JSONB NOT NULL,                -- тело задания, схему знает потребитель
    state        TEXT NOT NULL DEFAULT 'pending'
                 CHECK (state IN ('pending', 'processing', 'done', 'failed', 'dead')),
    attempts     INTEGER NOT NULL DEFAULT 0,    -- сколько раз задание уже брали
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(), -- раньше этого времени не брать
    locked_by    TEXT,                          -- кто взял строку (id воркера)
    locked_until TIMESTAMPTZ,                   -- конец аренды: истечение = задание свободно
    last_error   TEXT,                          -- последняя причина сбоя (без секретов)
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Выборка готовых к работе: только актуальные строки в индексе,
-- завершённые и мёртвые не мешают выборке (partial index).
CREATE INDEX job_ready_idx ON job (available_at, id) WHERE state = 'pending';
-- Истёкшие аренды: reaper возвращает такие задания в оборот.
CREATE INDEX job_lease_idx ON job (locked_until) WHERE state = 'processing';
CREATE INDEX job_topic_idx ON job (topic, state);

-- +goose Down
DROP TABLE job;
