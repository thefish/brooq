-- +goose Up
-- 00003_job_retention: уборка терминальных строк по TTL.
-- Терминальные задания (done, dead, canceled) иначе копятся вечно:
-- таблица растёт, индексы распухают, а ключ идемпотентности никогда не
-- освобождается. Миграция только готовит схему; саму уборку делает
-- метод хранилища отдельным DELETE пачками.

-- Индекс под уборку. updated_at — момент перехода в терминальное
-- состояние (его выставляет каждый оператор исхода), поэтому «старше
-- TTL» и «терминальное дольше TTL» — одно и то же. Индекс частичный:
-- активные строки (pending, processing, failed) в нём не участвуют,
-- значит он не растёт вместе с очередью работы и не мешает hot path.
CREATE INDEX job_retention_idx
    ON job (updated_at)
    WHERE state IN ('done', 'dead', 'canceled');

-- Настройка autovacuum — не дефолт, а осознанное изменение под высокий
-- churn. По умолчанию autovacuum срабатывает при 20 % мёртвых строк:
-- для очереди с постоянным потоком вставок/удалений это редкие и очень
-- долгие проходы, которые мешают работе. Понижение scale factor даёт
-- частые короткие проходы. Порог 100 не даёт вакуумировать крошечную
-- таблицу на каждые несколько удалений.
ALTER TABLE job SET (
    autovacuum_vacuum_scale_factor  = 0.02,
    autovacuum_vacuum_threshold     = 100,
    autovacuum_analyze_scale_factor = 0.05
);

-- +goose Down
-- Откат в обратном порядке: сначала вернуть дефолты autovacuum, затем
-- убрать индекс уборки. Данные не теряются — удалённые уборкой строки
-- откат не возвращает, это необратимо по природе TTL.
ALTER TABLE job RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_threshold, autovacuum_analyze_scale_factor);
DROP INDEX job_retention_idx;
