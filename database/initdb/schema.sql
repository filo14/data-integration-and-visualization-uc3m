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
DROP TABLE IF EXISTS demographic CASCADE;
DROP TABLE IF EXISTS country CASCADE;
DROP TABLE IF EXISTS year CASCADE;

-- Drop the custom enum type if it exists
DROP TYPE IF EXISTS "Gender";


-- ===================================
-- 2. CREATE CUSTOM TYPES (Enum)
-- ===================================

-- Create the custom enum type for Gender
CREATE TYPE "Gender" AS ENUM (
    'MALE',
    'FEMALE',
    'OTHER'
);

-- ===================================
-- 3. CREATE CORE DIMENSION TABLES
-- ===================================

-- Table: Country
CREATE TABLE country (
    country_iso3_id VARCHAR(3) PRIMARY KEY NOT NULL,
    country_name VARCHAR(255) NOT NULL,
    region VARCHAR(255) NOT NULL,
    subregion VARCHAR(255) NOT NULL
);

-- Table: Year
CREATE TABLE year (
    year_id SERIAL PRIMARY KEY,
    year INT NOT NULL UNIQUE
);

-- Table: Demographic
CREATE TABLE demographic (
    demographic_id SERIAL PRIMARY KEY,
    gender "Gender" NOT NULL, -- Uses the custom enum type
    adult BOOLEAN NOT NULL,
    foreign_national BOOLEAN NOT NULL,
    -- Unique constraint on the combination of demographic attributes
    UNIQUE (gender, adult, foreign_national)
);


-- ===================================
-- 4. CREATE FACT TABLES
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

    -- Foreign Key 2: Link to Demographic
    demographic_id INT NOT NULL,
    CONSTRAINT fk_crime_demographic
        FOREIGN KEY (demographic_id)
        REFERENCES demographic (demographic_id)
        ON DELETE CASCADE,

    -- Foreign Key 3: Link to Year
    year_id INT NOT NULL,
    CONSTRAINT fk_crime_year
        FOREIGN KEY (year_id)
        REFERENCES year (year_id)
        ON DELETE CASCADE,

    -- Constraint to prevent duplicate entries for the country/demographic/year
    UNIQUE (country_iso3_id, demographic_id, year_id)
);
