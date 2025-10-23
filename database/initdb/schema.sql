--
-- File: initdb/schema.sql
-- This script contains the Data Definition Language (DDL) to set up the schema.
-- It is designed to be executed automatically by the PostgreSQL Docker image entrypoint.
--

-- ===================================
-- 1. CLEANUP (Idempotency)
-- ===================================

-- Drop tables that depend on foreign keys first
DROP TABLE IF EXISTS crime CASCADE;
DROP TABLE IF EXISTS immigration CASCADE;
DROP TABLE IF EXISTS population CASCADE;
DROP TABLE IF EXISTS country CASCADE;
DROP TABLE IF EXISTS year CASCADE;

-- ===================================
-- 2. CREATE CORE DIMENSION TABLES
-- ===================================

-- Table: Country
CREATE TABLE country (
    country_iso3_id VARCHAR(3) PRIMARY KEY NOT NULL,
    country_name VARCHAR(255) NOT NULL
);

-- Table: Year
CREATE TABLE year (
    year_id INT PRIMARY KEY NOT NULL UNIQUE
);

-- ===================================
-- 3. CREATE FACT TABLES
-- ===================================

-- Table: Immigration
CREATE TABLE immigration (
    immigration_id SERIAL PRIMARY KEY,
    immigration_per_100000 NUMERIC(10, 4) NOT NULL, -- Rate of immigration

    -- Foreign Key 1: Link to Country
    country_iso3_id VARCHAR(3) NOT NULL,
    CONSTRAINT fk_immigration_country
        FOREIGN KEY (country_iso3_id)
        REFERENCES country (country_iso3_id)
        ON DELETE CASCADE,

    -- Foreign Key 2: Link to Year
    year_id INT NOT NULL,
    CONSTRAINT fk_immigration_year
        FOREIGN KEY (year_id)
        REFERENCES year (year_id)
        ON DELETE CASCADE,

    -- Constraint to prevent duplicate entries for the same country/year
    UNIQUE (country_iso3_id, year_id)
);


-- Table: Crime
CREATE TABLE crime (
    crime_id SERIAL PRIMARY KEY,
    convicts_per_100000 NUMERIC(10, 4) NOT NULL, -- Rate of convicts

    -- Foreign Key 1: Link to Country
    country_iso3_id VARCHAR(3) NOT NULL,
    CONSTRAINT fk_crime_country
        FOREIGN KEY (country_iso3_id)
        REFERENCES country (country_iso3_id)
        ON DELETE CASCADE,

    -- Foreign Key 2: Link to Year
    year_id INT NOT NULL,
    CONSTRAINT fk_crime_year
        FOREIGN KEY (year_id)
        REFERENCES year (year_id)
        ON DELETE CASCADE,

    -- Constraint to prevent duplicate entries for the same country/year
    UNIQUE (country_iso3_id, year_id)
);

-- Table: Population
CREATE TABLE population (
    population_id SERIAL PRIMARY KEY,
    population BIGINT NOT NULL, -- Population

    -- Foreign Key 1: Link to Country
    country_iso3_id VARCHAR(3) NOT NULL,
    CONSTRAINT fk_crime_country
        FOREIGN KEY (country_iso3_id)
        REFERENCES country (country_iso3_id)
        ON DELETE CASCADE,

    -- Foreign Key 2: Link to Year
    year_id INT NOT NULL,
    CONSTRAINT fk_crime_year
        FOREIGN KEY (year_id)
        REFERENCES year (year_id)
        ON DELETE CASCADE,

    -- Constraint to prevent duplicate entries for the same country/year
    UNIQUE (country_iso3_id, year_id)
);

-- ===================================
-- 4. CREATE BASIC FACTS
-- ===================================

INSERT INTO year (year_id)
VALUES
('2019'),
('2020'),
('2021'),
('2022'),
('2023'),
('2024');
