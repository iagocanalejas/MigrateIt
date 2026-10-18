-- Migration 0006_functions_and_triggers.sql
-- Created on 2026-10-18T12:50:46.994293

CREATE TRIGGER precision_user_update
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    SET NEW.no_precision_number = CEIL(NEW.precision_number);
END;

-- Rollback migration

DROP TRIGGER precision_user_update;
