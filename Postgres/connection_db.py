import os
from dotenv import load_dotenv
import pg8000

# Charger les variables d'environnement depuis .env
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "root")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")
DB_NAME = os.getenv("POSTGRES_DB", "offers")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5430")

print("🔍 Paramètres de connexion pg8000 :")
print(f"  Host    : {DB_HOST}")
print(f"  Port    : {DB_PORT}")
print(f"  User    : {DB_USER}")
print(f"  Database: {DB_NAME}\n")

try:
    print("🔌 Tentative de connexion pg8000…")
    conn = pg8000.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        
    )
    print("✅ Connexion pg8000 réussie\n")

   

except Exception as e:
    print("❌ Erreur pg8000 :", e)
