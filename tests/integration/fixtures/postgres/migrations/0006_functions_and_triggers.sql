-- Migration 0006_functions_and_triggers.sql
-- Created on 2026-10-18T12:50:46.994293

CREATE OR REPLACE FUNCTION user_no_precision()
RETURNS TRIGGER AS $$
BEGIN
    NEW.no_precision_number := CEIL(NEW.precision_number);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER precision_user_update
AFTER UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION user_no_precision();

-- Rollback migration

DROP TRIGGER precision_user_update ON users;
DROP FUNCTION user_no_precision();
