-- ↓ Supprime tout pour repartir à zéro
DROP TABLE IF EXISTS
  fact_offer_sector,
  fact_offer_contract,
  fact_offer_work_type,
  fact_offer_remote,
  fact_offer_company,
  fact_competence,
  fact_offre,
  dim_sector,
  dim_contrat,
  dim_work_type,
  dim_remote,
  dim_company,
  dim_profil,
  dim_localisation,
  dim_salaire,
  dim_competence
CASCADE;

-- ========== DIMENSIONS ==========
CREATE TABLE dim_profil (
  profile_id   SERIAL PRIMARY KEY,
  profile      TEXT       NOT NULL
);

CREATE TABLE dim_localisation (
  location_id  SERIAL PRIMARY KEY,
  location     TEXT       NOT NULL
);

CREATE TABLE dim_salaire (
  salary_id    SERIAL PRIMARY KEY,
  salary_range TEXT       NOT NULL
);

CREATE TABLE dim_contrat (
  contract_id   SERIAL PRIMARY KEY,
  contract_type TEXT       NOT NULL UNIQUE
);

CREATE TABLE dim_work_type (
  work_type_id  SERIAL PRIMARY KEY,
  work_type     TEXT       NOT NULL UNIQUE
);

CREATE TABLE dim_remote (
  remote_id    SERIAL PRIMARY KEY,
  is_remote    BOOLEAN    NOT NULL
);

CREATE TABLE dim_company (
  company_id    SERIAL PRIMARY KEY,
  company_name  TEXT       NOT NULL UNIQUE
);

CREATE TABLE dim_sector (
  sector_id    SERIAL PRIMARY KEY,
  sector       TEXT       NOT NULL UNIQUE
);

CREATE TABLE dim_competence (
  skill_id        INTEGER   NOT NULL,
  skill           TEXT      NOT NULL,
  type_competence TEXT      NOT NULL CHECK(type_competence IN('hard','soft')),
  PRIMARY KEY(skill_id,type_competence)
);

-- ============ FAITS ============
CREATE TABLE fact_offre (
  offer_id          SERIAL PRIMARY KEY,
  job_url           TEXT,
  titre             TEXT,
  via               TEXT,
  description       TEXT,
  publication_date  DATE,
  education_level   TEXT,
  experience_years  TEXT,
  seniority         TEXT,
  profile_id        INTEGER REFERENCES dim_profil(profile_id),
  location_id       INTEGER REFERENCES dim_localisation(location_id),
  salary_id         INTEGER REFERENCES dim_salaire(salary_id)
);

-- Ponts vers nouvelles dims
CREATE TABLE fact_offer_contract (
  offer_id    INTEGER REFERENCES fact_offre(offer_id),
  contract_id INTEGER REFERENCES dim_contrat(contract_id),
  PRIMARY KEY(offer_id,contract_id)
);

CREATE TABLE fact_offer_work_type (
  offer_id      INTEGER REFERENCES fact_offre(offer_id),
  work_type_id  INTEGER REFERENCES dim_work_type(work_type_id),
  PRIMARY KEY(offer_id,work_type_id)
);

CREATE TABLE fact_offer_remote (
  offer_id   INTEGER REFERENCES fact_offre(offer_id),
  remote_id  INTEGER REFERENCES dim_remote(remote_id),
  PRIMARY KEY(offer_id,remote_id)
);

CREATE TABLE fact_offer_company (
  offer_id   INTEGER REFERENCES fact_offre(offer_id),
  company_id INTEGER REFERENCES dim_company(company_id),
  PRIMARY KEY(offer_id,company_id)
);

CREATE TABLE fact_offer_sector (
  offer_id  INTEGER REFERENCES fact_offre(offer_id),
  sector_id INTEGER REFERENCES dim_sector(sector_id),
  PRIMARY KEY(offer_id,sector_id)
);

CREATE TABLE fact_competence (
  offer_id    INTEGER REFERENCES fact_offre(offer_id),
  skill_id    INTEGER,
  niveau      TEXT    CHECK(niveau IN('hard','soft')),
  PRIMARY KEY(offer_id,skill_id)
);
