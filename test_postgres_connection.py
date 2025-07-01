import psycopg2

# Connexion à PostgreSQL dans Docker (exposé en local sur le port 5430)
conn = psycopg2.connect(
    host="localhost",     # ou "host.docker.internal" si besoin
    port="5430",
    dbname="offers",
    user="root",
    password="123456"
)

cur = conn.cursor()

# Afficher les 5 premières lignes de fact_offer
cur.execute("SELECT * FROM fact_offer LIMIT 5;")
rows = cur.fetchall()

print("✅ Connexion réussie. Extrait de la table fact_offer :")
for row in rows:
    print(row)

cur.close()
conn.close()
