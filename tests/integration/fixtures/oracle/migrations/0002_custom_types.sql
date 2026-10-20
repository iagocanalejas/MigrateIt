-- Migration 0002_custom_types.sql
-- Created on 2026-10-18T12:50:01.916847

ALTER TABLE users ADD status VARCHAR2(50) DEFAULT 'pending'
CHECK (status IN ('pending', 'active', 'inactive', 'archived'));

-- Rollback migration

ALTER TABLE users DROP COLUMN status;
