-- ===============================
-- Schema SQL pour Job Market DB
-- Sans accents pour éviter erreurs
-- ===============================

-- Suppression si existent
DROP TABLE IF EXISTS fact_competence, fact_offre,
    dim_competence, dim_contrat, dim_date, dim_entreprise,
    dim_localisation, dim_profil CASCADE;

-- ====================
-- Tables dimensionnelles
-- ====================

CREATE TABLE dim_date (
    id_date SERIAL PRIMARY KEY,
    jour INT,
    mois INT,
    annee INT,
    jour_semaine TEXT,
    est_weekend BOOLEAN
);

CREATE TABLE dim_competence (
    id_competence SERIAL PRIMARY KEY,
    nom TEXT
);

CREATE TABLE dim_contrat (
    id_contrat SERIAL PRIMARY KEY,
    type_contrat TEXT
);

CREATE TABLE dim_entreprise (
    id_entreprise SERIAL PRIMARY KEY,
    nom TEXT
);

CREATE TABLE dim_localisation (
    id_localisation SERIAL PRIMARY KEY,
    ville TEXT,
    pays TEXT
);

CREATE TABLE dim_profil (
    id_profil SERIAL PRIMARY KEY,
    intitule TEXT
);

-- ====================
-- Tables de faits
-- ====================

CREATE TABLE fact_offre (
    id_offre SERIAL PRIMARY KEY,
    id_date INT REFERENCES dim_date(id_date),
    id_entreprise INT REFERENCES dim_entreprise(id_entreprise),
    id_localisation INT REFERENCES dim_localisation(id_localisation),
    id_profil INT REFERENCES dim_profil(id_profil),
    id_contrat INT REFERENCES dim_contrat(id_contrat),
    source TEXT,
    date_publication TIMESTAMP,
    salaire_min NUMERIC,
    salaire_max NUMERIC
);

CREATE TABLE fact_competence (
    id_offre INT REFERENCES fact_offre(id_offre),
    id_competence INT REFERENCES dim_competence(id_competence),
    niveau TEXT,
    PRIMARY KEY (id_offre, id_competence)
);
