-- Migration 0004_sequences.sql
-- Created on 2026-10-18T12:50:36.210467

CREATE SEQUENCE seq_users START WITH 1 INCREMENT BY 1 NOCACHE;
ALTER TABLE users ADD idx INT DEFAULT seq_users.NEXTVAL;

-- Rollback migration

ALTER TABLE users DROP COLUMN idx;
DROP SEQUENCE seq_users;
