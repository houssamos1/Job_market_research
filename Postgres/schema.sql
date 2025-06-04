-- Create Dimension Tables

CREATE TABLE DimDate (
    DateKey INTEGER PRIMARY KEY,
    Date DATE,
    Année INTEGER,
    Trimestre TEXT
);

CREATE TABLE DimEntreprise (
    EntrepriseKey INTEGER PRIMARY KEY,
    CompanyName TEXT,
    SecteurPrincipal TEXT
);

CREATE TABLE DimLocalisation (
    LocalisationKey INTEGER PRIMARY KEY,
    Ville TEXT,
    Région TEXT,
    Pays TEXT,
    TypeRemote TEXT
);

CREATE TABLE DimProfil (
    ProfilKey INTEGER PRIMARY KEY,
    TitreOffre TEXT,
    ProfilNormalized TEXT,
    NiveauProfil TEXT
);

CREATE TABLE DimContrat (
    ContratKey INTEGER PRIMARY KEY,
    ContratType TEXT,
    TypeTravail TEXT
);

CREATE TABLE DimCompétence (
    CompetenceKey INTEGER PRIMARY KEY,
    CompetenceNom TEXT,
    TypeCompetence TEXT
);

-- Create Fact Table

CREATE TABLE FactOffre (
    OffreID TEXT PRIMARY KEY,
    DateKey INTEGER REFERENCES DimDate(DateKey),
    EntrepriseKey INTEGER REFERENCES DimEntreprise(EntrepriseKey),
    LocalisationKey INTEGER REFERENCES DimLocalisation(LocalisationKey),
    ProfilKey INTEGER REFERENCES DimProfil(ProfilKey),
    ContratKey INTEGER REFERENCES DimContrat(ContratKey),
    IsDataProfile BOOLEAN,
    EducationLevel INTEGER,
    ExperienceYears INTEGER,
    Seniority TEXT,
    NbHardSkills INTEGER,
    NbSoftSkills INTEGER
);

-- Create Bridge Table for Many-to-Many Relationship

CREATE TABLE Fact_Compétence (
    OffreID TEXT REFERENCES FactOffre(OffreID),
    CompetenceKey INTEGER REFERENCES DimCompétence(CompetenceKey),
    PRIMARY KEY (OffreID, CompetenceKey)
);