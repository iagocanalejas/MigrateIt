-- Migration 0001_base_table.sql
-- Created on 2026-10-18T12:48:53.735644

CREATE TABLE users (
	id SERIAL PRIMARY KEY,
	not_null_name VARCHAR(100) NOT NULL,
	unique_email VARCHAR(255) UNIQUE,
	default_title VARCHAR(200) NOT NULL DEFAULT 'Default Title',
	body TEXT,
	checked_number INTEGER NOT NULL DEFAULT 1 CHECK(checked_number > 0),
	no_precision_number DECIMAL(5),
	precision_number DECIMAL(5,3),
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rollback migration

DROP TABLE users;
