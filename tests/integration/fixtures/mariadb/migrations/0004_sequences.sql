-- Migration 0004_sequences.sql
-- Created on 2026-10-18T12:50:36.210467

CREATE SEQUENCE seq_users START WITH 1 INCREMENT BY 1;
ALTER TABLE users ADD COLUMN idx INT DEFAULT (NEXTVAL(seq_users));

-- Rollback migration

ALTER TABLE users DROP COLUMN idx;
DROP SEQUENCE seq_users;
