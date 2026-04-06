-- =============================================================================
-- create_schemas.sql
-- Creates the three warehouse schemas and all tables.
-- Safe to run multiple times (uses IF NOT EXISTS).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS mart;

-- =============================================================================
-- STAGING LAYER
-- =============================================================================

CREATE TABLE IF NOT EXISTS stg.stg_activities (
    code                  VARCHAR(50),
    initial_activity      TEXT,
    proposed_activity     TEXT,
    implementing_entity   VARCHAR(200),
    delivery_partner      VARCHAR(200),
    results_area          VARCHAR(200),
    category              VARCHAR(100),
    budget_year1          NUMERIC,
    budget_year2          NUMERIC,
    budget_year3          NUMERIC,
    budget_total          NUMERIC,
    budget_used           NUMERIC,
    budget_used_year1     NUMERIC,
    budget_used_year2     NUMERIC,
    budget_used_year3     NUMERIC,
    status                VARCHAR(50),
    progress              NUMERIC,
    notes                 TEXT,
    start_date            DATE,
    end_date              DATE,
    stg_loaded_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS stg.stg_indicators (
    id                          INTEGER,
    activity_id                 INTEGER,
    activity_code               VARCHAR(50),
    implementing_entity         VARCHAR(200),   -- resolved via JOIN in extract
    key_project_activity        TEXT,
    new_proposed_indicator      TEXT,
    indicator_type              VARCHAR(100),
    naphs                       BOOLEAN,
    indicator_definition        TEXT,
    data_source                 TEXT,
    baseline_proposal_year      NUMERIC,
    target_year1                NUMERIC,
    target_year2                NUMERIC,
    target_year3                NUMERIC,
    submitted                   TEXT,
    comments                    TEXT,
    portal_edited               TEXT,
    comment_addressed           TEXT,
    actual_baseline             NUMERIC,
    actual_year1                NUMERIC,
    actual_year2                NUMERIC,
    actual_year3                NUMERIC,
    progress_year1              NUMERIC,
    progress_year2              NUMERIC,
    progress_year3              NUMERIC,
    status_year1                VARCHAR(100),
    status_year2                VARCHAR(100),
    status_year3                VARCHAR(100),
    last_progress_update        DATE,
    qualitative_stage_year1     VARCHAR(100),
    qualitative_stage_year2     VARCHAR(100),
    qualitative_stage_year3     VARCHAR(100),
    stg_loaded_at               TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS dwh.dim_implementing_entity (
    entity_id    SERIAL PRIMARY KEY,
    entity_name  VARCHAR(200) NOT NULL UNIQUE,
    etl_loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dim_results_area (
    area_id      SERIAL PRIMARY KEY,
    area_name    VARCHAR(200) NOT NULL UNIQUE,
    etl_loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dim_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE,
    etl_loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dim_delivery_partner (
    partner_id    SERIAL PRIMARY KEY,
    partner_name  VARCHAR(200) NOT NULL UNIQUE,
    etl_loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dim_strategic_area (
    area_id    SERIAL PRIMARY KEY,
    area_name  VARCHAR(100) NOT NULL UNIQUE,
    etl_loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.dim_indicator (
    indicator_id         INTEGER PRIMARY KEY,
    activity_code        VARCHAR(50),
    indicator_text       TEXT,
    indicator_definition TEXT,
    indicator_type       VARCHAR(100),
    naphs_flag           BOOLEAN,
    quantitative_flag    BOOLEAN,
    qualitative_flag     BOOLEAN,
    strategic_area       VARCHAR(100),
    entity_name          VARCHAR(200),
    data_source          TEXT,
    etl_loaded_at        TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- FACT TABLES
-- =============================================================================

CREATE TABLE IF NOT EXISTS dwh.fact_indicator_progress (
    id                    SERIAL PRIMARY KEY,
    indicator_id          INTEGER,
    activity_code         VARCHAR(50),
    entity_name           VARCHAR(200),
    strategic_area        VARCHAR(100),
    indicator_type        VARCHAR(100),
    quantitative_flag     BOOLEAN,
    qualitative_flag      BOOLEAN,
    naphs_flag            BOOLEAN,
    year_number           SMALLINT NOT NULL,
    target                NUMERIC,
    actual                NUMERIC,
    progress_pct          NUMERIC,
    completion_rate       NUMERIC,
    gap                   NUMERIC,
    qualitative_stage     VARCHAR(100),
    qualitative_score     NUMERIC,
    achievement_category  VARCHAR(20),
    status                VARCHAR(100),
    last_progress_update  DATE,
    etl_loaded_at         TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dwh.fact_budget_execution (
    id                SERIAL PRIMARY KEY,
    activity_code     VARCHAR(50),
    entity_name       VARCHAR(200),
    results_area      VARCHAR(200),
    delivery_partner  VARCHAR(200),
    category          VARCHAR(100),
    year_number       SMALLINT NOT NULL,
    budget_allocated  NUMERIC,
    budget_used       NUMERIC,
    execution_rate    NUMERIC,
    progress          NUMERIC,
    status            VARCHAR(50),
    start_date        DATE,
    end_date          DATE,
    etl_loaded_at     TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- MART TABLES  (pre-aggregated, dashboard-ready)
-- =============================================================================

-- Tab 1 KPI cards (one row per year)
CREATE TABLE IF NOT EXISTS mart.mart_indicator_kpis (
    year_number        SMALLINT NOT NULL PRIMARY KEY,
    total_indicators   INTEGER,
    quantitative       INTEGER,
    qualitative        INTEGER,
    pct_completed      NUMERIC,
    pct_on_track       NUMERIC,
    pct_at_risk        NUMERIC,
    pct_not_started    NUMERIC,
    avg_progress       NUMERIC,
    etl_loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Tab 2 entity performance (one row per entity × year)
CREATE TABLE IF NOT EXISTS mart.mart_entity_performance (
    year_number       SMALLINT     NOT NULL,
    entity_name       VARCHAR(200) NOT NULL,
    total_indicators  INTEGER,
    avg_progress      NUMERIC,
    completed         INTEGER,
    on_track          INTEGER,
    at_risk           INTEGER,
    not_started       INTEGER,
    etl_loaded_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (year_number, entity_name)
);

-- Tab 3 strategic area summary (one row per area × year)
CREATE TABLE IF NOT EXISTS mart.mart_strategic_summary (
    year_number       SMALLINT     NOT NULL,
    strategic_area    VARCHAR(100) NOT NULL,
    num_indicators    INTEGER,
    avg_progress      NUMERIC,
    completed         INTEGER,
    on_track          INTEGER,
    at_risk           INTEGER,
    not_started       INTEGER,
    etl_loaded_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (year_number, strategic_area)
);

-- Tab 4 full indicator tracker + bottleneck flags (one row per indicator × year)
CREATE TABLE IF NOT EXISTS mart.mart_indicator_tracker (
    indicator_id          INTEGER      NOT NULL,
    year_number           SMALLINT     NOT NULL,
    activity_code         VARCHAR(50),
    entity_name           VARCHAR(200),
    indicator_text        TEXT,
    indicator_type        VARCHAR(100),
    strategic_area        VARCHAR(100),
    naphs_flag            BOOLEAN,
    quantitative_flag     BOOLEAN,
    qualitative_flag      BOOLEAN,
    target                NUMERIC,
    actual                NUMERIC,
    progress_pct          NUMERIC,
    gap                   NUMERIC,
    completion_rate       NUMERIC,
    qualitative_stage     VARCHAR(100),
    qualitative_score     NUMERIC,
    achievement_category  VARCHAR(20),
    status                VARCHAR(100),
    last_progress_update  DATE,
    no_actuals_flag       BOOLEAN,
    at_risk_flag          BOOLEAN,
    stale_flag            BOOLEAN,
    gap_rank              INTEGER,
    etl_loaded_at         TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (indicator_id, year_number)
);

-- Budget performance mart (one row per activity × year)
CREATE TABLE IF NOT EXISTS mart.mart_budget_performance (
    activity_code     VARCHAR(50)  NOT NULL,
    year_number       SMALLINT     NOT NULL,
    entity_name       VARCHAR(200),
    results_area      VARCHAR(200),
    delivery_partner  VARCHAR(200),
    category          VARCHAR(100),
    budget_allocated  NUMERIC,
    budget_used       NUMERIC,
    execution_rate    NUMERIC,
    progress          NUMERIC,
    status            VARCHAR(50),
    start_date        DATE,
    end_date          DATE,
    etl_loaded_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (activity_code, year_number)
);

-- Activity status mart for status analysis tab (one row per entity × results_area × status × year)
CREATE TABLE IF NOT EXISTS mart.mart_activity_status (
    year_number     SMALLINT     NOT NULL,
    entity_name     VARCHAR(200) NOT NULL,
    results_area    VARCHAR(200) NOT NULL,
    status          VARCHAR(50)  NOT NULL,
    activity_count  INTEGER,
    etl_loaded_at   TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (year_number, entity_name, results_area, status)
);
