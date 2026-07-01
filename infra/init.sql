-- =============================================================================
-- Customer Intelligence Platform — PostgreSQL schema & role initialization
-- -----------------------------------------------------------------------------
-- Task 3: creates the six data schemas and the platform's PostgreSQL roles,
-- then applies the role/permission matrix from the design's Security Design
-- section, including default privileges so that tables created in future runs
-- (by dbt, the generator, and the ML pipeline) inherit the correct access.
--
-- Idempotency: this script is safe to run multiple times.
--   * Schemas use CREATE SCHEMA IF NOT EXISTS.
--   * PostgreSQL has no CREATE ROLE IF NOT EXISTS, so roles are created inside
--     DO blocks guarded by a pg_roles existence check.
--   * GRANT / REVOKE / ALTER DEFAULT PRIVILEGES are naturally repeatable.
--
-- Secrets: no passwords are hard-coded. Each login role's password is read from
-- an environment variable via psql's \getenv (PostgreSQL 15+). Provide these on
-- the `postgres` service environment (and .env / .env.example):
--     RAW_WRITER_PASSWORD, STAGING_WRITER_PASSWORD, MART_WRITER_PASSWORD,
--     MART_READER_PASSWORD, METABASE_DB_PASSWORD
-- An unset variable yields an empty password (login disabled), never a leak.
--
-- Table DDL (raw.*, marts.*, ml.*, observability.*) is intentionally NOT here —
-- it is created in Task 20. Default privileges below cover those future tables.
-- =============================================================================

\set ON_ERROR_STOP on

-- Resolve the current database and the bootstrap (superuser) role name without
-- hard-coding them. `current_user` during container init is the POSTGRES_USER
-- superuser that owns every object created by this script.
SELECT current_database() AS dbname, current_user AS admin_user \gset

-- Read role passwords from the environment. Initialize to empty first so the
-- psql variables are always defined even if the env var is absent.
\set raw_writer_pw ''
\getenv raw_writer_pw RAW_WRITER_PASSWORD
\set staging_writer_pw ''
\getenv staging_writer_pw STAGING_WRITER_PASSWORD
\set mart_writer_pw ''
\getenv mart_writer_pw MART_WRITER_PASSWORD
\set mart_reader_pw ''
\getenv mart_reader_pw MART_READER_PASSWORD
\set metabase_reader_pw ''
\getenv metabase_reader_pw METABASE_DB_PASSWORD


-- -----------------------------------------------------------------------------
-- 1. Schemas
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;           -- append-only landing zone
CREATE SCHEMA IF NOT EXISTS staging;       -- dbt stg_* models
CREATE SCHEMA IF NOT EXISTS intermediate;  -- dbt int_* models
CREATE SCHEMA IF NOT EXISTS marts;         -- dbt mart_* models (business-facing)
CREATE SCHEMA IF NOT EXISTS ml;            -- ML scoring outputs & feature snapshots
CREATE SCHEMA IF NOT EXISTS observability; -- pipeline_run_log, dq_failures, insights


-- -----------------------------------------------------------------------------
-- 2. Roles (idempotent — CREATE ROLE IF NOT EXISTS does not exist in PostgreSQL)
--    All roles are least-privilege LOGIN roles with no superuser/DDL rights at
--    the cluster level. Passwords are set separately via ALTER ROLE so the
--    secret is never embedded inside a dollar-quoted DO block.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'raw_writer') THEN
    CREATE ROLE raw_writer WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'staging_writer') THEN
    CREATE ROLE staging_writer WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mart_writer') THEN
    CREATE ROLE mart_writer WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'mart_reader') THEN
    CREATE ROLE mart_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_reader') THEN
    CREATE ROLE metabase_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT;
  END IF;
END
$$;

-- Set (or reset) passwords from the environment. Re-running simply re-applies
-- the current value, keeping the script idempotent.
ALTER ROLE raw_writer      WITH PASSWORD :'raw_writer_pw';
ALTER ROLE staging_writer  WITH PASSWORD :'staging_writer_pw';
ALTER ROLE mart_writer     WITH PASSWORD :'mart_writer_pw';
ALTER ROLE mart_reader     WITH PASSWORD :'mart_reader_pw';
ALTER ROLE metabase_reader WITH PASSWORD :'metabase_reader_pw';


-- -----------------------------------------------------------------------------
-- 3. Schema isolation — revoke the implicit PUBLIC access first, then grant
--    only the specific roles below. This enforces the "deny by default" posture
--    from the Security Design section.
-- -----------------------------------------------------------------------------
REVOKE ALL ON SCHEMA raw, staging, intermediate, marts, ml, observability FROM PUBLIC;

-- Only the roles that own a login pathway need CONNECT; deny it to PUBLIC and
-- grant it explicitly to every platform role.
REVOKE CONNECT ON DATABASE :"dbname" FROM PUBLIC;
GRANT  CONNECT ON DATABASE :"dbname"
  TO raw_writer, staging_writer, mart_writer, mart_reader, metabase_reader;


-- -----------------------------------------------------------------------------
-- 4. Permission matrix
--    Role            | Schema access                | Permissions
--    raw_writer      | raw                          | SELECT, INSERT, UPDATE
--    staging_writer  | staging, intermediate        | CREATE + SELECT/INSERT/UPDATE/DELETE
--    mart_writer     | marts, ml, observability     | CREATE + SELECT/INSERT/UPDATE/DELETE
--    mart_reader     | marts, ml, observability     | SELECT only
--    metabase_reader | marts                        | SELECT only
-- -----------------------------------------------------------------------------

-- raw_writer — data-generator & Airflow ingestion tasks --------------------
GRANT USAGE ON SCHEMA raw TO raw_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES    IN SCHEMA raw TO raw_writer;
GRANT USAGE, SELECT           ON ALL SEQUENCES IN SCHEMA raw TO raw_writer;

-- staging_writer — dbt staging + intermediate runs -------------------------
GRANT USAGE, CREATE ON SCHEMA staging      TO staging_writer;
GRANT USAGE, CREATE ON SCHEMA intermediate TO staging_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA staging      TO staging_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA intermediate TO staging_writer;
GRANT USAGE, SELECT                  ON ALL SEQUENCES IN SCHEMA staging      TO staging_writer;
GRANT USAGE, SELECT                  ON ALL SEQUENCES IN SCHEMA intermediate TO staging_writer;

-- mart_writer — dbt mart runs + ML scoring writes --------------------------
GRANT USAGE, CREATE ON SCHEMA marts         TO mart_writer;
GRANT USAGE, CREATE ON SCHEMA ml            TO mart_writer;
GRANT USAGE, CREATE ON SCHEMA observability TO mart_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA marts         TO mart_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA ml            TO mart_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA observability TO mart_writer;
GRANT USAGE, SELECT                  ON ALL SEQUENCES IN SCHEMA marts         TO mart_writer;
GRANT USAGE, SELECT                  ON ALL SEQUENCES IN SCHEMA ml            TO mart_writer;
GRANT USAGE, SELECT                  ON ALL SEQUENCES IN SCHEMA observability TO mart_writer;

-- mart_reader — FastAPI connection pool (read-only) ------------------------
GRANT USAGE  ON SCHEMA marts         TO mart_reader;
GRANT USAGE  ON SCHEMA ml            TO mart_reader;
GRANT USAGE  ON SCHEMA observability TO mart_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts         TO mart_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ml            TO mart_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA observability TO mart_reader;

-- metabase_reader — Metabase JDBC (read-only, marts only) ------------------
GRANT USAGE  ON SCHEMA marts               TO metabase_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO metabase_reader;


-- -----------------------------------------------------------------------------
-- 5. Default privileges for FUTURE tables/sequences
--    Table DDL is created later (Task 20 as :"admin_user"; dbt as the writer
--    roles). ALTER DEFAULT PRIVILEGES applies per creating role, so we register
--    entries for both the bootstrap superuser and each writer role that creates
--    objects in a given schema.
-- -----------------------------------------------------------------------------

-- raw: objects created by the bootstrap role; generator only writes rows.
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user" IN SCHEMA raw
  GRANT SELECT, INSERT, UPDATE ON TABLES    TO raw_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user" IN SCHEMA raw
  GRANT USAGE, SELECT          ON SEQUENCES TO raw_writer;

-- staging / intermediate: objects created by dbt as staging_writer (and, for
-- any bootstrap-created tables, by :"admin_user").
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", staging_writer IN SCHEMA staging
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO staging_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", staging_writer IN SCHEMA staging
  GRANT USAGE, SELECT                  ON SEQUENCES TO staging_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", staging_writer IN SCHEMA intermediate
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO staging_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", staging_writer IN SCHEMA intermediate
  GRANT USAGE, SELECT                  ON SEQUENCES TO staging_writer;

-- marts: writers create via dbt; readers (mart_reader, metabase_reader) must
-- see anything created by either the bootstrap role or mart_writer.
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA marts
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO mart_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA marts
  GRANT USAGE, SELECT                  ON SEQUENCES TO mart_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA marts
  GRANT SELECT ON TABLES TO mart_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA marts
  GRANT SELECT ON TABLES TO metabase_reader;

-- ml + observability: created by the bootstrap role (Task 20) and written by
-- mart_writer; mart_reader gets read-only on future tables.
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA ml
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO mart_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA ml
  GRANT USAGE, SELECT                  ON SEQUENCES TO mart_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA ml
  GRANT SELECT ON TABLES TO mart_reader;

ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA observability
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO mart_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA observability
  GRANT USAGE, SELECT                  ON SEQUENCES TO mart_writer;
ALTER DEFAULT PRIVILEGES FOR ROLE :"admin_user", mart_writer IN SCHEMA observability
  GRANT SELECT ON TABLES TO mart_reader;

-- =============================================================================
-- End of Task 3 initialization.
-- =============================================================================
