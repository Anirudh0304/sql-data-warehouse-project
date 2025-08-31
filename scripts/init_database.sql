-- ============================================================================
-- Drop and recreate the 'datawarehouse' database
-- (⚠️ Must be run outside a transaction in Postgres)
-- ============================================================================

-- Terminate active connections and drop DB if it exists
DO
$$
BEGIN
   IF EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse') THEN
      PERFORM pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE datname = 'datawarehouse'
        AND pid <> pg_backend_pid();
      EXECUTE 'DROP DATABASE datawarehouse';
   END IF;
END
$$;

-- Create the database
CREATE DATABASE datawarehouse;

-- ============================================================================
-- Create Schemas inside 'datawarehouse'
-- (Run this AFTER connecting to the 'datawarehouse' DB)
-- ============================================================================
\c datawarehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
