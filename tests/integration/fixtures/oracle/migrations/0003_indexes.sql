-- Migration 0003_indexes.sql
-- Created on 2026-10-18T12:52:03.576973

CREATE INDEX idx_users_checked_number ON users(checked_number);

-- Rollback migration

DROP INDEX idx_users_checked_number;
