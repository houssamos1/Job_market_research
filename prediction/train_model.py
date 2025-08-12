import psycopg2
import pandas as pd
from prophet import Prophet

# Paramètres de connexion à ta DB
DB_PARAMS = {
    "dbname": "prediction",
    "user": "root",
    "password": "123456",
    "host": "postgres",
    "port": 5432
}

def load_training_data(id_titre):
    conn = psycopg2.connect(**DB_PARAMS)
    query = """
        SELECT ds, y
        FROM ts_offres
        WHERE id_titre = %s
        ORDER BY ds
    """
    df = pd.read_sql(query, conn, params=(id_titre,))
    conn.close()
    return df

def train_prophet_model(df):
    model = Prophet()
    model.fit(df)
    return model

def main():
    id_titre = 123  # Remplace par un id_titre existant dans ta base

    print(f"Chargement des données d'entraînement pour id_titre={id_titre}...")
    df = load_training_data(id_titre)

    if df.empty:
        print("❌ Pas de données disponibles pour cet id_titre.")
        return

    print(f"Données chargées: {len(df)} lignes")

    print("Entraînement du modèle Prophet...")
    model = train_prophet_model(df)

    print("Modèle entraîné avec succès !")

    # Optionnel: prévision sur 30 jours
    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())

if __name__ == "__main__":
    main()
