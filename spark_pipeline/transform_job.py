from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, lit
from pyspark.sql.types import StringType
import os

print("🔥 Fichier mis à jour !")
def main():
    spark = (
        SparkSession.builder.appName("JobTransform")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.5.1",
        )
        .getOrCreate()
    )

    # Config S3A pour MinIO
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint", os.getenv("MINIO_API", "http://minio:9000")
    )
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", os.getenv("MINIO_ROOT_USER")
    )
    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD")
    )
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

    # Lecture JSON depuis MinIO
    df = spark.read.json("s3a://webscraping/*.json")
    df = df.dropDuplicates(["job_url"])
    df = df.fillna({"contrat": "unknown", "seniority": "unknown", "education_level": 0})
    df = df.withColumn("date_id", to_date("publication_date", "yyyy-MM-dd"))

    # Préparation des dimensions
    dim_contract = df.select(col("contrat").alias("contract_type")).distinct()
    dim_profile = df.select("profile").distinct()
    dim_location = df.select(
        col("location.city").alias("city"), col("location.country").alias("country")
    ).distinct()
    dim_company = df.select("company_name").distinct()
    dim_education = df.select(
        col("education_level").cast(StringType()).alias("education_level")
    ).distinct()
    dim_experience = df.select("seniority").distinct()
    dim_sector = df.select(explode("sector").alias("sector")).distinct()
    dim_skill = (
        df.select(explode("hard_skills").alias("skill"))
        .withColumn("skill_type", lit("hard"))
        .union(
            df.select(explode("soft_skills").alias("skill")).withColumn(
                "skill_type", lit("soft")
            )
        )
        .distinct()
    )

    # Table de faits (pré-join, sans explode)
    fact_pre = df.select(
        "job_url",
        col("titre").alias("title"),
        "date_id",
        col("contrat").alias("contract_type"),
        col("location.city").alias("city"),
        col("location.country").alias("country"),
        "company_name",
        "profile",
        "education_level",
        "seniority",
        "sector",
    )

    fact_skill = (
        df.select(
            col("job_url").alias("job_url_fs"), explode("hard_skills").alias("skill")
        )
        .union(
            df.select(
                col("job_url").alias("job_url_fs"),
                explode("soft_skills").alias("skill"),
            )
        )
        .distinct()
    )

    # Connexion PostgreSQL
    pg_url = f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}"
    pg_props = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    # Vérifie les offres existantes
    try:
        existing_jobs = spark.read.jdbc(
            pg_url, "public.fact_offer", properties=pg_props
        ).select("job_url")
        print("✅ PostgreSQL: Existing job_url loaded")
    except:
        existing_jobs = spark.createDataFrame([], fact_pre.schema).select("job_url")
        print("⚠️ Aucun job_url existant trouvé (peut-être première exécution)")

    # Filtrage des nouvelles offres
    new_fact_pre = fact_pre.join(existing_jobs, on="job_url", how="left_anti")
    new_fact = new_fact_pre.withColumn("sector", explode("sector"))

    new_fact_skill = fact_skill.join(
        existing_jobs.withColumnRenamed("job_url", "job_url_fs"),
        on="job_url_fs",
        how="left_anti",
    )

    print(f"🆕 New offers to insert: {new_fact.count()}")
    print(f"🆕 New skills to insert: {new_fact_skill.count()}")

    # Insertion des dimensions
    for table, df_dim, cols in [
        ("dim_contract", dim_contract, ["contract_type"]),
        ("dim_profile", dim_profile, ["profile"]),
        ("dim_location", dim_location, ["city", "country"]),
        ("dim_company", dim_company, ["company_name"]),
        ("dim_education", dim_education, ["education_level"]),
        ("dim_experience", dim_experience, ["seniority"]),
        ("dim_sector", dim_sector, ["sector"]),
        ("dim_skill", dim_skill, ["skill", "skill_type"]),
    ]:
        df_dim.write.jdbc(pg_url, f"public.{table}", properties=pg_props, mode="append")

    # Insertion des faits
    new_fact.write.jdbc(pg_url, "public.fact_offer", properties=pg_props, mode="append")
    new_fact_skill.write.jdbc(
        pg_url, "public.fact_offer_skill", properties=pg_props, mode="append"
    )

    spark.stop()


if __name__ == "__main__":
    main()
