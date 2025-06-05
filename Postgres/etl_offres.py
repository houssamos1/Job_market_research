import json
import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# 1. Charger les variables d’environnement
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# 2. Construction de la connexion SQLAlchemy propre
conn_string = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(conn_string, client_encoding="utf8")

# 3. Charger JSON proprement
def load_clean_json(path):
    with open(path, encoding='utf-8') as f:
        content = f.read().replace('\n', ' ')
        return json.loads(content)

# 4. Transformer une offre
def transformer_offre(offre):
    location = offre.get("location") or {}
    contract = offre.get("contract") or {}

    return {
        "titre": offre.get("title"),
        "entreprise": offre.get("company"),
        "lieu": location.get("city"),
        "contrat": contract.get("type"),
        "description": offre.get("description"),
        "profil": offre.get("profile"),
        "competences": offre.get("skills", []),
        "date_publication": offre.get("date")
    }

# 5. Script principal
def main():
    path = "Postgres/input/corsignal1[1].json"
    raw = load_clean_json(path)
    offres = [transformer_offre(o) for o in raw]
    df = pd.DataFrame(offres)

    # Exemple : table dim_date
    dim_date = pd.DataFrame({
        "date_publication": pd.to_datetime(df["date_publication"], errors='coerce').dt.date
    }).dropna().drop_duplicates()

    dim_date.to_sql("dim_date", con=engine, if_exists="append", index=False)

    # Exemple : dim_entreprise
    dim_entreprise = pd.DataFrame({"entreprise": df["entreprise"]}).dropna().drop_duplicates()
    dim_entreprise.to_sql("dim_entreprise", con=engine, if_exists="append", index=False)

    print("✅ Données insérées avec succès.")

if __name__ == "__main__":
    main()
