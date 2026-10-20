-- Migration 0002_custom_types.sql
-- Created on 2026-10-18T12:50:01.916847

CREATE TYPE status_enum AS ENUM ('pending', 'active', 'inactive', 'archived');
ALTER TABLE users ADD COLUMN status status_enum;

-- Rollback migration

ALTER TABLE users DROP COLUMN status;
DROP TYPE status_enum;
