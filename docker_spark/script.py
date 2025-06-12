from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, lower
import json

# Créer la session Spark
spark = SparkSession.builder.appName("ETL Job Offers").getOrCreate()

# Lire le fichier JSON brut
df = spark.read.json("corsignal1.json", multiLine=True)


# Fonction utilitaire pour créer une dimension
def prepare_dim(df, col_name, id_name):
    return (
        df.select(lower(col(col_name)).alias(col_name))
        .dropna()
        .dropDuplicates()
        .withColumn(id_name, monotonically_increasing_id())
    )


# Créer les tables de dimension
dim_contract = prepare_dim(df, "contrat", "contract_id").withColumnRenamed(
    "contrat", "contract_type"
)
dim_seniority = prepare_dim(df, "seniority", "seniority_id")
dim_profile = prepare_dim(df, "profile", "profile_id")
dim_company = prepare_dim(df, "company_name", "company_id").withColumnRenamed(
    "company_name", "company_name_clean"
)

# Créer la table des faits
fact_offers = (
    df.join(dim_contract, df["contrat"] == dim_contract["contract_type"], "left")
    .join(dim_seniority, "seniority", "left")
    .join(dim_profile, "profile", "left")
    .join(dim_company, df["company_name"] == dim_company["company_name_clean"], "left")
    .select(
        "job_url",
        "titre",
        "publication_date",
        "experience_years",
        "education_level",
        "contract_id",
        "seniority_id",
        "profile_id",
        "company_id",
    )
)

# Sauvegarder le résultat en fichier JSON
fact_offers.write.mode("overwrite").json("mcd_final.json")

print("ETL terminé avec succès.")
