import json
import psycopg2

# Configuration PostgreSQL
DB_CONFIG = {
    "host": "localhost",  # ou le nom de ton conteneur Docker s’il est accessible
    "port": 5432,
    "dbname": "jobdb",
    "user": "postgres",
    "password": "postgres",
}

# Charger le fichier JSON
with open("mcd_final.json", "r", encoding="utf-8") as f:
    mcd = json.load(f)

# Connexion à PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()


# Fonction pour insérer dans chaque table
def insert_data(table_name, data):
    if not data:
        print(f"❌ Aucun enregistrement pour {table_name}")
        return

    columns = list(data[0].keys())
    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"

    for row in data:
        values = tuple(row[col] for col in columns)
        cursor.execute(sql, values)

    print(f"✅ Données insérées dans {table_name} : {len(data)} lignes")


# Insertion dans l'ordre des dépendances
order = [
    "dim_contract",
    "dim_work_type",
    "dim_location",
    "dim_company",
    "dim_profile",
    "dim_skill",
    "dim_sector",
    "fact_offer",
    "fact_offer_skill",
]

for table in order:
    insert_data(table, mcd.get(table, []))

# Finalisation
conn.commit()
cursor.close()
conn.close()

print("🎉 Tous les enregistrements ont été insérés avec succès.")
