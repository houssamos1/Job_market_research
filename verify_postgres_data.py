import psycopg2
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "offers")
DB_USER = os.getenv("POSTGRES_USER", "root")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "123456")

try:
    print("🔌 Connexion à PostgreSQL...")
    conn = psycopg2.connect(
        host=DB_HOST, port=int(DB_PORT), dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM fact_offer;")
    count = cur.fetchone()[0]
    print(f"\n✅ Nombre total d'offres insérées dans 'fact_offer' : {count}")

    # Afficher les colonnes existantes
    cur.execute(
        """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'fact_offer';
    """
    )
    columns = cur.fetchall()
    print("\n📌 Colonnes présentes dans 'fact_offer' :")
    for col in columns:
        print(f"- {col[0]}")

    # Afficher les 5 premières lignes
    cur.execute("SELECT * FROM fact_offer LIMIT 5;")
    rows = cur.fetchall()
    if rows:
        print("\n🔍 Aperçu des 5 premières lignes :")
        for row in rows:
            print(row)
    else:
        print("ℹ️ Aucune offre trouvée dans la table.")

    cur.close()
    conn.close()

except Exception as e:
    print("❌ Erreur lors de la connexion ou de la requête PostgreSQL :")
    print(e)
