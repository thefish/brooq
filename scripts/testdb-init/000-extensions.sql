-- Расширения уровня БД накатывает "devops" ДО любых миграций (см.
-- migrations/00001_foundation.sql): этот initdb-скрипт выполняется
-- docker-entrypoint при первом старте контейнера — до goose up.
-- Нужно для gen_random_bytes (шифрование учётных данных).
CREATE EXTENSION IF NOT EXISTS pgcrypto;
