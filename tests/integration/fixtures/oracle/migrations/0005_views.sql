-- Migration 0005_views.sql
-- Created on 2026-10-18T12:50:59.819695

CREATE VIEW user_with_big_number AS
SELECT users.not_null_name
FROM users
WHERE checked_number > 100000;

-- Rollback migration

DROP VIEW user_with_big_number;
