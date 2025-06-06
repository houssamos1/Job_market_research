import json
import os
import pandas as pd
import pg8000
from dotenv import load_dotenv
from datetime import datetime

# 1. Charger les variables d’environnement
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "root")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")
DB_NAME = os.getenv("POSTGRES_DB", "offers")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))

# 2. Charger le fichier JSON
def load_json(path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)

# 3. Transformer une offre en dictionnaire SQL ready
def transformer_offre(offre):
    location = offre.get("location") or {}
    contract = offre.get("contract") or {}

    return {
        "titre": offre.get("title"),
        "entreprise": offre.get("company"),
        "ville": location.get("city"),
        "type_contrat": contract.get("type"),
        "description": offre.get("description"),
        "profil": offre.get("profile"),
        "competences": offre.get("skills", []),
        "date_publication": offre.get("date")
    }

# 4. Insérer les données dans les tables PostgreSQL
def insert_dim(conn, table, column, values):
    cur = conn.cursor()
    ids = {}
    for val in values:
        if not val:
            continue
        cur.execute(f"SELECT id_{table} FROM dim_{table} WHERE {column} = %s", (val,))
        res = cur.fetchone()
        if res:
            ids[val] = res[0]
        else:
            cur.execute(f"INSERT INTO dim_{table} ({column}) VALUES (%s) RETURNING id_{table}", (val,))
            ids[val] = cur.fetchone()[0]
    conn.commit()
    return ids

def insert_fact(conn, offre, ids_entreprise, ids_contrat, ids_profil, ids_localisation, ids_date, ids_competence):
    cur = conn.cursor()
    
    # Récupérer ou insérer la date
    date_pub = pd.to_datetime(offre["date_publication"], errors='coerce')
    if pd.isna(date_pub):
        return
    date_str = date_pub.strftime("%Y-%m-%d")
    cur.execute("SELECT id_date FROM dim_date WHERE jour = %s AND mois = %s AND annee = %s",
                (date_pub.day, date_pub.month, date_pub.year))
    date_res = cur.fetchone()
    if not date_res:
        cur.execute("""INSERT INTO dim_date (jour, mois, annee, jour_semaine, est_weekend)
                       VALUES (%s, %s, %s, %s, %s) RETURNING id_date""",
                    (date_pub.day, date_pub.month, date_pub.year,
                     date_pub.strftime('%A'), date_pub.weekday() >= 5))
        id_date = cur.fetchone()[0]
    else:
        id_date = date_res[0]

    # Insert fact_offre
    cur.execute("""INSERT INTO fact_offre (
                      id_date, id_entreprise, id_localisation, id_profil, id_contrat,
                      source, date_publication, salaire_min, salaire_max
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, NULL)
                   RETURNING id_offre""",
                (id_date,
                 ids_entreprise.get(offre["entreprise"]),
                 ids_localisation.get(offre["ville"]),
                 ids_profil.get(offre["profil"]),
                 ids_contrat.get(offre["type_contrat"]),
                 "corsignal", date_pub)
                )
    id_offre = cur.fetchone()[0]

    # Insert compétences
    for skill in offre["competences"]:
        id_comp = ids_competence.get(skill)
        if id_comp:
            cur.execute("""INSERT INTO fact_competence (id_offre, id_competence, niveau)
                           VALUES (%s, %s, %s)""", (id_offre, id_comp, ""))
    conn.commit()

# 5. Script principal
def main():
    path = "Postgres/input/corsignal1[1].json"
    raw = load_json(path)
    offres = [transformer_offre(o) for o in raw]

    print("🔌 Connexion à PostgreSQL avec pg8000...")
    conn = pg8000.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT
    )

    print("📥 Insertion des dimensions...")
    entreprises = list({o["entreprise"] for o in offres if o["entreprise"]})
    profils = list({o["profil"] for o in offres if o["profil"]})
    contrats = list({o["type_contrat"] for o in offres if o["type_contrat"]})
    villes = list({o["ville"] for o in offres if o["ville"]})
    competences = set()
    for o in offres:
        competences.update(o["competences"])

    ids_entreprise = insert_dim(conn, "entreprise", "nom", entreprises)
    ids_profil = insert_dim(conn, "profil", "intitule", profils)
    ids_contrat = insert_dim(conn, "contrat", "type_contrat", contrats)
    ids_localisation = insert_dim(conn, "localisation", "ville", villes)
    ids_competence = insert_dim(conn, "competence", "nom", list(competences))

    print("🧠 Insertion des faits...")
    for o in offres:
        insert_fact(conn, o, ids_entreprise, ids_contrat, ids_profil, ids_localisation, {}, ids_competence)

    print("✅ Chargement terminé avec succès.")
    conn.close()

if __name__ == "__main__":
    main()
