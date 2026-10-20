-- Migration 0001_base_table.sql
-- Created on 2026-10-18T12:48:53.735644

CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    not_null_name VARCHAR(100) NOT NULL,
    unique_email VARCHAR(255) UNIQUE,
    default_title VARCHAR(200) NOT NULL DEFAULT 'Default Title',
    body TEXT,
    checked_number INTEGER NOT NULL CHECK(checked_number > 0) DEFAULT 1,
    no_precision_number INTEGER,
    precision_number REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rollback migration

DROP TABLE users;
