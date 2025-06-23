import os
import uuid
from datetime import datetime

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, to_date, trim
from pyspark.sql.types import StringType

# -----------------------------------------------------------------------------------
# INITIALISATION
# -----------------------------------------------------------------------------------


def create_spark_session():
    """
    Crée une SparkSession avec le package hadoop-aws pour accéder à MinIO via s3a://
    """
    print("🔥 Initialisation SparkSession...")
    return (
        SparkSession.builder.appName("JobCleaningPipeline")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )


def configure_minio(spark):
    """
    Configure l'accès à MinIO pour Spark via le protocole S3A.

    Nécessite les variables d'environnement :
    - MINIO_API : URL de MinIO (ex: http://minio:9000)
    - MINIO_ROOT_USER / MINIO_ROOT_PASSWORD : Identifiants d’accès
    """
    print("🔐 Configuration MinIO...")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", os.getenv("MINIO_API", "http://minio:9000"))
    hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


# -----------------------------------------------------------------------------------
# LECTURE DES FICHIERS JSON
# -----------------------------------------------------------------------------------


def list_valid_json_objects():
    """
    Retourne les chemins valides des objets JSON présents dans le bucket MinIO 'webscraping'.
    Seuls les fichiers .json dont la taille > 10 octets sont conservés.
    """
    client = Minio(
        os.getenv("MINIO_API"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )
    objects = client.list_objects("webscraping", recursive=True)
    valid_paths = [
        f"s3a://webscraping/{obj.object_name}"
        for obj in objects
        if obj.object_name.endswith(".json") and obj.size > 10
    ]
    return valid_paths


def read_all_json_from_minio(spark):
    """
    Lit et fusionne tous les fichiers JSON valides depuis MinIO dans un DataFrame PySpark.
    """
    print("📥 Lecture filtrée des fichiers JSON valides depuis MinIO...")
    valid_files = list_valid_json_objects()

    if not valid_files:
        print("⚠️ Aucun fichier JSON valide trouvé dans le bucket.")
        return None

    print(f"🔍 Fichiers détectés : {len(valid_files)}")
    for path in valid_files:
        print(f"   → {path}")

    df = spark.read.option("multiLine", True).json(valid_files)
    total = df.count()
    df.show(5, truncate=False)
    print(f"✅ Nombre total d'offres chargées : {total}")
    return df


# -----------------------------------------------------------------------------------
# NETTOYAGE DES DONNÉES
# -----------------------------------------------------------------------------------


def clean_data(df):
    """
    Nettoie et transforme les données :
    - Vérifie la présence des colonnes clés
    - Renomme les colonnes
    - Applique le split sur les champs multiples (compétences, secteurs, etc.)
    - Nettoie les types (dates, string)
    - Supprime les doublons selon `job_url`
    """
    print("🧼 Nettoyage des données...")

    # Champs obligatoires
    required = ["job_url", "titre", "via", "publication_date"]
    for field in required:
        df = df.filter(col(field).isNotNull() & (col(field) != ""))

    # Renommage et nettoyage
    df = (
        df.withColumnRenamed("companie", "company_name")
        .withColumnRenamed("niveau_etudes", "education_level")
        .withColumnRenamed("niveau_experience", "seniority")
        .withColumnRenamed("competences", "hard_skills")
        .withColumnRenamed("secteur", "sector")
        .withColumnRenamed("salaire", "salary_range")
        .withColumnRenamed("domaine", "domain")
        .withColumn("hard_skills", split(col("hard_skills"), ",\\s*"))
    )

    # Cas conditionnel : soft_skills peut être absente
    if "soft_skills" in df.columns:
        df = df.withColumn("soft_skills", split(col("soft_skills"), ",\\s*"))
    else:
        print("⚠️ Colonne 'soft_skills' absente — elle sera ignorée.")

    df = (
        df.withColumn("sector", split(col("sector"), ",\\s*"))
        .withColumn("education_level", trim(col("education_level").cast(StringType())))
        .withColumn("seniority", trim(col("seniority").cast(StringType())))
        .withColumn("publication_date", to_date(col("publication_date"), "yyyy-MM-dd"))
        .dropDuplicates(["job_url"])
    )

    print("✅ Nettoyage terminé.")
    return df


# -----------------------------------------------------------------------------------
# ÉCRITURE / SAUVEGARDE
# -----------------------------------------------------------------------------------


def generate_output_filename():
    """
    Génère un nom de fichier unique basé sur la date et un UUID.
    Exemple : processed_jobs_20250619_ab12cd34.json
    """
    file_id = str(uuid.uuid4())[:8]
    today = datetime.now().strftime("%Y%m%d")
    return f"processed_jobs_{today}_{file_id}.json"


def save_locally(df, path="/tmp/cleaned_output"):
    """
    Sauvegarde le DataFrame nettoyé localement en JSON (écrasement du dossier).
    """
    print(f"💾 Sauvegarde locale dans {path}")
    df.coalesce(1).write.mode("overwrite").json(path)
    return path


def find_json_in_folder(folder):
    """
    Cherche le fichier JSON généré dans un dossier local donné.
    """
    for f in os.listdir(folder):
        if f.endswith(".json"):
            return os.path.join(folder, f)
    return None


def upload_to_minio(local_path, filename, bucket="traitement"):
    """
    Upload le fichier JSON local vers le bucket MinIO spécifié.
    """
    print("📤 Upload vers MinIO...")
    client = Minio(
        os.getenv("MINIO_API"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )
    json_file = find_json_in_folder(local_path)
    if json_file:
        client.fput_object(bucket, filename, json_file, content_type="application/json")
        print(f"🚀 Upload terminé : {bucket}/{filename}")
    else:
        print("❌ Aucun fichier JSON à uploader.")


# -----------------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------------


def main():
    """
    Pipeline complet :
    1. Initialise Spark et MinIO
    2. Charge les données JSON valides
    3. Nettoie les données
    4. Sauvegarde localement
    5. Upload vers MinIO
    """
    print("🚀 DÉMARRAGE DU SCRIPT SPARK")
    try:
        spark = create_spark_session()
        configure_minio(spark)

        df_raw = read_all_json_from_minio(spark)
        if df_raw is None or df_raw.count() == 0:
            print("🛑 Fin du script : aucun fichier JSON à traiter.")
            return

        df_cleaned = clean_data(df_raw)
        filename = generate_output_filename()
        local_path = "/tmp/cleaned_output"
        save_locally(df_cleaned, local_path)
        upload_to_minio(local_path, filename)

        print("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    except Exception as e:
        print("❌ ERREUR DANS LE SCRIPT :", e)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
    print("🔥 Fichier mis à jour !")
