-- -------------------------------------------------------------------
-- DROP OLD TABLES
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS public.fact_offer_skill CASCADE;
DROP TABLE IF EXISTS public.fact_offer       CASCADE;
DROP TABLE IF EXISTS public.dim_contract     CASCADE;
DROP TABLE IF EXISTS public.dim_work_type    CASCADE;
DROP TABLE IF EXISTS public.dim_location     CASCADE;
DROP TABLE IF EXISTS public.dim_company      CASCADE;
DROP TABLE IF EXISTS public.dim_profile      CASCADE;
DROP TABLE IF EXISTS public.dim_skill        CASCADE;
DROP TABLE IF EXISTS public.dim_sector       CASCADE;
DROP TABLE IF EXISTS public.dim_calendar     CASCADE;
DROP TABLE IF EXISTS public.dim_education    CASCADE;
DROP TABLE IF EXISTS public.dim_experience   CASCADE;
DROP TABLE IF EXISTS public.dim_source       CASCADE;

-- -------------------------------------------------------------------
-- DIMENSIONS
-- -------------------------------------------------------------------
CREATE TABLE public.dim_contract (
  contract_id      SERIAL PRIMARY KEY,
  contract_type    TEXT   NOT NULL
);

CREATE TABLE public.dim_work_type (
  work_type_id     SERIAL PRIMARY KEY,
  work_type        TEXT   NOT NULL
);

CREATE TABLE public.dim_location (
  location_id      SERIAL PRIMARY KEY,
  city             TEXT,
  country          TEXT
);

CREATE TABLE public.dim_company (
  company_id       SERIAL PRIMARY KEY,
  company_name     TEXT   NOT NULL
);

CREATE TABLE public.dim_profile (
  profile_id       SERIAL PRIMARY KEY,
  profile          TEXT   NOT NULL
);

CREATE TABLE public.dim_skill (
  skill_id         SERIAL PRIMARY KEY,
  skill            TEXT   NOT NULL,
  skill_type       TEXT   NOT NULL CHECK (skill_type IN ('hard','soft'))
);

CREATE TABLE public.dim_sector (
  sector_id        SERIAL PRIMARY KEY,
  sector           TEXT   NOT NULL
);

-- -------------------------------------------------------------------
-- DIMENSION TEMPORELLE
-- -------------------------------------------------------------------
CREATE TABLE public.dim_calendar (
  date_id           DATE     PRIMARY KEY,
  year              INTEGER  NOT NULL,
  quarter           INTEGER  NOT NULL,
  month_number      INTEGER  NOT NULL,
  month_name        TEXT     NOT NULL,
  day               INTEGER  NOT NULL,
  year_month        TEXT     NOT NULL,
  day_of_week       INTEGER  NOT NULL,
  week_of_year      INTEGER  NOT NULL,
  date_str          TEXT     NOT NULL
);

-- -------------------------------------------------------------------
-- DIMENSION EDUCATION ET EXPERIENCE
-- -------------------------------------------------------------------
CREATE TABLE public.dim_education (
  education_id      SERIAL PRIMARY KEY,
  education_level   TEXT   NOT NULL
);

CREATE TABLE public.dim_experience (
  experience_id     SERIAL PRIMARY KEY,
  seniority         TEXT   NOT NULL
);

-- -------------------------------------------------------------------
-- DIMENSION SOURCE
-- -------------------------------------------------------------------
CREATE TABLE public.dim_source (
  source_id         SERIAL PRIMARY KEY,
  source_name       TEXT   NOT NULL,
  source_url        TEXT
);

-- -------------------------------------------------------------------
-- TABLE DES FAITS
-- -------------------------------------------------------------------
CREATE TABLE public.fact_offer (
  offer_id           SERIAL   PRIMARY KEY,
  source_id          INTEGER  REFERENCES dim_source(source_id),
  job_url            TEXT,
  title              TEXT,
  date_id            DATE     NOT NULL REFERENCES dim_calendar(date_id),
  contract_id        INTEGER  REFERENCES dim_contract(contract_id),
  work_type_id       INTEGER  REFERENCES dim_work_type(work_type_id),
  location_id        INTEGER  REFERENCES dim_location(location_id),
  company_id         INTEGER  REFERENCES dim_company(company_id),
  profile_id         INTEGER  REFERENCES dim_profile(profile_id),
  education_id       INTEGER  REFERENCES dim_education(education_id),
  experience_id      INTEGER  REFERENCES dim_experience(experience_id),
  sector_id          INTEGER  REFERENCES dim_sector(sector_id)
);

-- -------------------------------------------------------------------
-- LIAISON OFFRE ↔ COMPÉTENCE
-- -------------------------------------------------------------------
CREATE TABLE public.fact_offer_skill (
  offer_id INTEGER REFERENCES fact_offer(offer_id),
  skill_id INTEGER REFERENCES dim_skill(skill_id),
  PRIMARY KEY (offer_id, skill_id)
);