-- Migration 0006_functions_and_triggers.sql
-- Created on 2026-10-18T12:50:46.994293

CREATE TRIGGER precision_user_update
AFTER UPDATE ON users
FOR EACH ROW
WHEN OLD.precision_number IS DISTINCT FROM NEW.precision_number
BEGIN
    UPDATE users
    SET no_precision_number = CAST(CEIL(NEW.precision_number) AS INTEGER)
    WHERE rowid = NEW.rowid;
END;

-- Rollback migration

DROP TRIGGER precision_user_update;
