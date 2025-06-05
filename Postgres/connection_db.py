import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    print("🔌 Tentative de connexion à PostgreSQL...")
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        options="-c client_encoding=UTF8"
    )
    print("✅ Connexion PostgreSQL réussie.")
    conn.close()
except Exception as e:
    print("❌ Erreur de connexion PostgreSQL :", e)
