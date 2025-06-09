-- -------------------------------------------------------------------
-- Reset ancien schéma
-- -------------------------------------------------------------------
DROP TABLE IF EXISTS public.fact_offer_company        CASCADE;
DROP TABLE IF EXISTS public.fact_offer_sector         CASCADE;
DROP TABLE IF EXISTS public.fact_offer_remote         CASCADE;
DROP TABLE IF EXISTS public.fact_offer_work_type      CASCADE;
DROP TABLE IF EXISTS public.fact_offer_contract       CASCADE;
DROP TABLE IF EXISTS public.fact_competence           CASCADE;
DROP TABLE IF EXISTS public.fact_offer                CASCADE;

DROP TABLE IF EXISTS public.dim_via                   CASCADE;
DROP TABLE IF EXISTS public.dim_seniority             CASCADE;
DROP TABLE IF EXISTS public.dim_education_level       CASCADE;
DROP TABLE IF EXISTS public.dim_competence            CASCADE;
DROP TABLE IF EXISTS public.dim_company               CASCADE;
DROP TABLE IF EXISTS public.dim_sector                CASCADE;
DROP TABLE IF EXISTS public.dim_remote                CASCADE;
DROP TABLE IF EXISTS public.dim_work_type             CASCADE;
DROP TABLE IF EXISTS public.dim_contract              CASCADE;
DROP TABLE IF EXISTS public.dim_salary                CASCADE;
DROP TABLE IF EXISTS public.dim_location              CASCADE;
DROP TABLE IF EXISTS public.dim_profile               CASCADE;

-- -------------------------------------------------------------------
-- Dimensions
-- -------------------------------------------------------------------
CREATE TABLE public.dim_profile (
  profile_id         INTEGER       PRIMARY KEY,
  profile            TEXT          NOT NULL
);

CREATE TABLE public.dim_location (
  location_id        INTEGER       PRIMARY KEY,
  location_text      TEXT          NOT NULL
);

CREATE TABLE public.dim_salary (
  salary_id          INTEGER       PRIMARY KEY,
  salary_range       TEXT          NOT NULL
);

CREATE TABLE public.dim_contract (
  contract_id        INTEGER       PRIMARY KEY,
  contract_type      TEXT          NOT NULL
);

CREATE TABLE public.dim_work_type (
  work_type_id       INTEGER       PRIMARY KEY,
  work_type          TEXT          NOT NULL
);

CREATE TABLE public.dim_remote (
  remote_id          INTEGER       PRIMARY KEY,
  is_remote          BOOLEAN       NOT NULL
);

CREATE TABLE public.dim_company (
  company_id         INTEGER       PRIMARY KEY,
  company_name       TEXT          NOT NULL
);

CREATE TABLE public.dim_sector (
  sector_id          INTEGER       PRIMARY KEY,
  sector             TEXT          NOT NULL
);

CREATE TABLE public.dim_competence (
  skill_id           INTEGER       PRIMARY KEY,
  skill              TEXT          NOT NULL,
  type_competence    VARCHAR(10)   NOT NULL  -- 'soft' ou 'hard'
);

CREATE TABLE public.dim_via (
  via_id             SERIAL        PRIMARY KEY,
  via_name           TEXT          NOT NULL UNIQUE
);

CREATE TABLE public.dim_seniority (
  seniority_id       SERIAL        PRIMARY KEY,
  seniority_level    TEXT          NOT NULL UNIQUE
);

CREATE TABLE public.dim_education_level (
  education_level_id SERIAL        PRIMARY KEY,
  level              INTEGER       NOT NULL UNIQUE
);

-- -------------------------------------------------------------------
-- Table de faits principale
-- -------------------------------------------------------------------
CREATE TABLE public.fact_offer (
  offer_id           INTEGER       PRIMARY KEY,
  job_url            TEXT,
  title              TEXT,
  via_id             INTEGER       REFERENCES public.dim_via(via_id),
  description        TEXT,
  publication_date   DATE,
  education_level_id INTEGER       REFERENCES public.dim_education_level(education_level_id),
  experience_years   INTEGER,
  seniority_id       INTEGER       REFERENCES public.dim_seniority(seniority_id),
  profile_id         INTEGER       REFERENCES public.dim_profile(profile_id),
  location_id        INTEGER       REFERENCES public.dim_location(location_id),
  salary_id          INTEGER       REFERENCES public.dim_salary(salary_id)
);

-- -------------------------------------------------------------------
-- Tables de faits d’association many-to-many
-- -------------------------------------------------------------------
CREATE TABLE public.fact_offer_contract (
  offer_id           INTEGER       REFERENCES public.fact_offer(offer_id),
  contract_id        INTEGER       REFERENCES public.dim_contract(contract_id),
  PRIMARY KEY (offer_id, contract_id)
);

CREATE TABLE public.fact_offer_work_type (
  offer_id           INTEGER       REFERENCES public.fact_offer(offer_id),
  work_type_id       INTEGER       REFERENCES public.dim_work_type(work_type_id),
  PRIMARY KEY (offer_id, work_type_id)
);

CREATE TABLE public.fact_offer_remote (
  offer_id           INTEGER       REFERENCES public.fact_offer(offer_id),
  remote_id          INTEGER       REFERENCES public.dim_remote(remote_id),
  PRIMARY KEY (offer_id, remote_id)
);

CREATE TABLE public.fact_offer_company (
  offer_id           INTEGER       REFERENCES public.fact_offer(offer_id),
  company_id         INTEGER       REFERENCES public.dim_company(company_id),
  PRIMARY KEY (offer_id, company_id)
);

CREATE TABLE public.fact_offer_sector (
  offer_id           INTEGER       REFERENCES public.fact_offer(offer_id),
  sector_id          INTEGER       REFERENCES public.dim_sector(sector_id),
  PRIMARY KEY (offer_id, sector_id)
);

CREATE TABLE public.fact_competence (
  offer_id           INTEGER       REFERENCES public.fact_offer(offer_id),
  skill_id           INTEGER       REFERENCES public.dim_competence(skill_id),
  PRIMARY KEY (offer_id, skill_id)
);
