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

-- -------------------------------------------------------------------
-- DIMENSIONS
-- -------------------------------------------------------------------
CREATE TABLE public.dim_contract (
  contract_id   SERIAL PRIMARY KEY,
  contract_type TEXT   NOT NULL
);

CREATE TABLE public.dim_work_type (
  work_type_id  SERIAL PRIMARY KEY,
  work_type     TEXT   NOT NULL
);

CREATE TABLE public.dim_location (
  location_id   SERIAL PRIMARY KEY,
  city          TEXT,
  country       TEXT
);

CREATE TABLE public.dim_company (
  company_id    SERIAL PRIMARY KEY,
  company_name  TEXT   NOT NULL
);

CREATE TABLE public.dim_profile (
  profile_id    SERIAL PRIMARY KEY,
  profile       TEXT   NOT NULL
);

CREATE TABLE public.dim_skill (
  skill_id       SERIAL PRIMARY KEY,
  skill          TEXT   NOT NULL,
  skill_type     TEXT   NOT NULL CHECK (skill_type IN ('hard','soft'))
);

CREATE TABLE public.dim_sector (
  sector_id     SERIAL PRIMARY KEY,
  sector        TEXT   NOT NULL
);

-- -------------------------------------------------------------------
-- FACT OFFER
-- -------------------------------------------------------------------
CREATE TABLE public.fact_offer (
  offer_id           SERIAL PRIMARY KEY,
  job_url            TEXT,
  title              TEXT,
  publication_date   DATE,
  contract_id        INTEGER REFERENCES dim_contract(contract_id),
  work_type_id       INTEGER REFERENCES dim_work_type(work_type_id),
  location_id        INTEGER REFERENCES dim_location(location_id),
  company_id         INTEGER REFERENCES dim_company(company_id),
  profile_id         INTEGER REFERENCES dim_profile(profile_id),
  education_years    INTEGER,
  seniority          TEXT,
  sector_id          INTEGER REFERENCES dim_sector(sector_id)
);

-- -------------------------------------------------------------------
-- FACT OFFER ↔ SKILL (many-to-many)
-- -------------------------------------------------------------------
CREATE TABLE public.fact_offer_skill (
  offer_id INTEGER REFERENCES fact_offer(offer_id),
  skill_id INTEGER REFERENCES dim_skill(skill_id),
  PRIMARY KEY (offer_id, skill_id)
);
