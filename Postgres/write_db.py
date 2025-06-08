import os
import json
from dotenv import load_dotenv
import pg8000.dbapi

# === Chargement des variables d’environnement ===
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "root")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")
DB_NAME = os.getenv("POSTGRES_DB", "offers")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5430))

# === Connexion PostgreSQL ===
conn = pg8000.dbapi.connect(
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    host=DB_HOST,
    port=DB_PORT
)
cursor = conn.cursor()

# === Fichier JSON source ===
json_path = r"C:\Users\houss\Desktop\DXC\Job_market_research\mcd_output.json"
with open(json_path, encoding="utf-8") as f:
    data = json.load(f)

# === Fonction d’insertion avec sécurité ===
def insert_with_ignore(table, row, conflict_keys):
    columns = list(row.keys())
    values = [row[col] for col in columns]
    placeholders = ", ".join(["%s"] * len(values))
    conflict_clause = ", ".join(conflict_keys)

    sql = f"""
        INSERT INTO {table} ({', '.join(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_clause}) DO NOTHING
    """
    try:
        cursor.execute(sql, values)
    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur dans {table}: {e} → {row}")
        with open("insertion_errors.log", "a", encoding="utf-8") as f:
            f.write(f"[{table}] {e}\n{row}\n\n")

# === Insertion des dimensions ===
for row in data["profiles"]:
    insert_with_ignore("dim_profil", row, ["profile_id"])

for row in data["locations"]:
    insert_with_ignore("dim_localisation", row, ["location_id"])

for row in data["salaries"]:
    insert_with_ignore("dim_salaire", row, ["salary_id"])

for row in data["soft_skills"]:
    row["type_competence"] = "soft"
    insert_with_ignore("dim_competence", row, ["skill_id", "type_competence"])

for row in data["hard_skills"]:
    row["type_competence"] = "hard"
    insert_with_ignore("dim_competence", row, ["skill_id", "type_competence"])

# === Insertion des offres ===
for row in data["offers"]:
    cleaned_row = {k: (v if v is not None else None) for k, v in row.items()}
    insert_with_ignore("fact_offre", cleaned_row, ["offer_id"])

# === Insertion des compétences liées aux offres ===
for row in data["offer_soft_skills"]:
    row["niveau"] = "soft"
    insert_with_ignore("fact_competence", row, ["offer_id", "skill_id"])

for row in data["offer_hard_skills"]:
    row["niveau"] = "hard"
    insert_with_ignore("fact_competence", row, ["offer_id", "skill_id"])

# === Commit et fermeture ===
conn.commit()
cursor.close()
conn.close()

print("✅ Insertion dans PostgreSQL terminée avec succès.")
