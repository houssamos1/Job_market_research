-- ==========================================
-- Schéma Job Market DB (avec contraintes)
-- ==========================================

-- 1) Suppression si existants
DROP TABLE IF EXISTS fact_competence      CASCADE;
DROP TABLE IF EXISTS fact_offre           CASCADE;
DROP TABLE IF EXISTS dim_date             CASCADE;
DROP TABLE IF EXISTS dim_competence       CASCADE;
DROP TABLE IF EXISTS dim_contrat          CASCADE;
DROP TABLE IF EXISTS dim_entreprise       CASCADE;
DROP TABLE IF EXISTS dim_localisation     CASCADE;
DROP TABLE IF EXISTS dim_profil           CASCADE;

-- 2) Création des tables

CREATE TABLE dim_date (
    id_date          SERIAL PRIMARY KEY,
    date_publication DATE    NOT NULL,
    jour             INT     NOT NULL,
    mois             INT     NOT NULL,
    annee            INT     NOT NULL,
    jour_semaine     TEXT    NOT NULL,
    est_weekend      BOOLEAN NOT NULL
);

CREATE TABLE dim_competence (
    id_competence    SERIAL PRIMARY KEY,
    nom              TEXT    NOT NULL,
    type_competence  TEXT    NOT NULL   -- 'hard' ou 'soft'
);

CREATE TABLE dim_contrat (
    id_contrat       SERIAL PRIMARY KEY,
    type_contrat     TEXT    NOT NULL   -- ex. 'full-time', 'internship'
);

CREATE TABLE dim_entreprise (
    id_entreprise    SERIAL PRIMARY KEY,
    nom              TEXT    NOT NULL
);

CREATE TABLE dim_localisation (
    id_localisation  SERIAL PRIMARY KEY,
    ville            TEXT    NOT NULL,
    pays             TEXT    NOT NULL
);

CREATE TABLE dim_profil (
    id_profil        SERIAL PRIMARY KEY,
    intitule         TEXT    NOT NULL
);

CREATE TABLE fact_offre (
    id_offre         SERIAL PRIMARY KEY,
    id_date          INT     NOT NULL REFERENCES dim_date(id_date),
    id_entreprise    INT     NOT NULL REFERENCES dim_entreprise(id_entreprise),
    id_localisation  INT     NOT NULL REFERENCES dim_localisation(id_localisation),
    id_profil        INT     NOT NULL REFERENCES dim_profil(id_profil),
    id_contrat       INT     NOT NULL REFERENCES dim_contrat(id_contrat),
    source           TEXT    NOT NULL,
    date_publication TIMESTAMP NOT NULL,
    salaire_min      NUMERIC,
    salaire_max      NUMERIC
);

CREATE TABLE fact_competence (
    id_offre         INT NOT NULL REFERENCES fact_offre(id_offre),
    id_competence    INT NOT NULL REFERENCES dim_competence(id_competence),
    niveau           TEXT    NOT NULL,  -- 'hard' ou 'soft'
    PRIMARY KEY (id_offre, id_competence, niveau)
);

-- 3) Ajout des contraintes UNIQUE pour ON CONFLICT

ALTER TABLE dim_date
  ADD CONSTRAINT uq_dim_date_datepub UNIQUE (date_publication);

ALTER TABLE dim_competence
  ADD CONSTRAINT uq_dim_competence_nom_type UNIQUE (nom, type_competence);

ALTER TABLE dim_contrat
  ADD CONSTRAINT uq_dim_contrat_type UNIQUE (type_contrat);

ALTER TABLE dim_entreprise
  ADD CONSTRAINT uq_dim_entreprise_nom UNIQUE (nom);

ALTER TABLE dim_localisation
  ADD CONSTRAINT uq_dim_localisation_vp UNIQUE (ville, pays);

ALTER TABLE dim_profil
  ADD CONSTRAINT uq_dim_profil_intitule UNIQUE (intitule);
