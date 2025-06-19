from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, lit, split, trim, array, when
from pyspark.sql.types import StringType
import os
import traceback


def create_spark_session():
    print("🔥 Initialisation de la session Spark...")
    return (
        SparkSession.builder.appName("JobTransform")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.5.1",
        )
        .getOrCreate()
    )


def configure_minio(spark):
    print("⚙️ Configuration de l'accès à MinIO...")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", os.getenv("MINIO_API", "http://minio:9000"))
    hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    hadoop_conf.set("fs.s3a.path.style.access", "true")


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, lit, array, when, to_date
from pyspark.sql.types import StringType


def read_and_prepare_data(spark):
    print("📥 Lecture des fichiers JSON depuis MinIO...")
    df_raw = (
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("multiLine", True)
        .json("s3a://webscraping/*.json")
    )

    if "_corrupt_record" in df_raw.columns:
        df_raw = df_raw.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    print(f"✅ Nombre total d'offres chargées : {df_raw.count()}")

    # Nettoyage et transformation
    df = (
        df_raw.withColumnRenamed("companie", "company_name")
        .withColumnRenamed("niveau_etudes", "education_level")
        .withColumnRenamed("niveau_experience", "seniority")
        .withColumnRenamed("competences", "hard_skills")
        .withColumn(
            "hard_skills", split(col("hard_skills").cast(StringType()), ",\\s*")
        )
        # ✅ Traitement robuste de 'sector' : string -> split, array -> inchangé
        .withColumn(
            "sector",
            when(
                col("sector").cast("string").isNotNull()
                & ~col("sector").cast("string").rlike(r"^\[.*\]"),
                split(col("sector").cast(StringType()), ",\\s*"),
            ).otherwise(col("sector")),
        )
        .withColumn("education_level", trim(col("education_level")))
        .withColumn("seniority", trim(col("seniority")))
        .withColumn("location_city", lit(None).cast(StringType()))
        .withColumn("location_country", lit(None).cast(StringType()))
        .withColumn("profile", lit("data analyst"))
        .withColumn("soft_skills", array())
        .withColumn("experience_years", lit(None))
        .withColumn("is_data_profile", lit(True))
        .withColumn("type_travail", lit(None))
        .withColumn("source_name", lit("rekrute"))
        .withColumn("date_id", to_date(col("publication_date"), "dd-MM-yyyy"))
    )

    return df.dropDuplicates(["job_url"])

    print("📊 Aperçu après transformation :")
    df.show(5, truncate=False)
    print("📉 Vérification des valeurs NULL dans colonnes clés :")
    df.select(
        [
            col(c).isNull().alias(f"{c}_is_null")
            for c in ["job_url", "titre", "company_name", "date_id", "contrat"]
        ]
    ).show()

    return df.dropDuplicates(["job_url"])


def create_dimensions(df):
    return {
        "dim_contract": df.select(col("contrat").alias("contract_type"))
        .filter(col("contrat").isNotNull())
        .distinct(),
        "dim_profile": df.select("profile")
        .filter(col("profile").isNotNull())
        .distinct(),
        "dim_location": df.select(
            col("location_city").alias("city"), col("location_country").alias("country")
        )
        .filter(col("location_city").isNotNull() & col("location_country").isNotNull())
        .distinct(),
        "dim_company": df.select("company_name")
        .filter(col("company_name").isNotNull())
        .distinct(),
        "dim_education": df.select(col("education_level").cast(StringType()))
        .filter(col("education_level").isNotNull())
        .distinct(),
        "dim_experience": df.select("seniority")
        .filter(col("seniority").isNotNull())
        .distinct(),
        "dim_sector": df.select(explode("sector").alias("sector"))
        .filter(col("sector").isNotNull())
        .distinct(),
        "dim_skill": df.select(explode("hard_skills").alias("skill"))
        .withColumn("skill_type", lit("hard"))
        .union(
            df.select(explode("soft_skills").alias("skill")).withColumn(
                "skill_type", lit("soft")
            )
        )
        .filter(col("skill").isNotNull())
        .distinct(),
    }


def create_fact_tables(df):
    fact_pre = df.select(
        "job_url",
        col("titre").alias("title"),
        "date_id",
        col("contrat").alias("contract_type"),
        col("location_city").alias("city"),
        col("location_country").alias("country"),
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
    return fact_pre, fact_skill


def get_postgres_connection_props():
    return {
        "url": f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}",
        "props": {
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "driver": "org.postgresql.Driver",
        },
    }


def load_existing_jobs(spark, pg_url, pg_props, schema):
    try:
        return spark.read.jdbc(pg_url, "public.fact_offer", properties=pg_props).select(
            "job_url"
        )
    except:
        print("⚠️ Aucun job_url existant trouvé (peut-être première exécution)")
        return spark.createDataFrame([], schema).select("job_url")


def insert_to_postgres(df_dict, pg_url, pg_props):
    for table_name, df in df_dict.items():
        try:
            existing_df = spark.read.jdbc(
                pg_url, f"public.{table_name}", properties=pg_props
            )
            df = df.join(existing_df, on=df.columns, how="left_anti")
        except:
            print(f"ℹ️ Première insertion dans {table_name} ou table vide.")
        count_insert = df.count()
        if count_insert > 0:
            print(f"📥 Insertion de {count_insert} lignes dans {table_name}")
            df.write.jdbc(
                pg_url, f"public.{table_name}", properties=pg_props, mode="append"
            )
        else:
            print(f"✅ Aucun nouvel enregistrement à insérer dans {table_name}")


def main():
    global spark
    try:
        spark = create_spark_session()
        configure_minio(spark)
        df = read_and_prepare_data(spark)
        dimensions = create_dimensions(df)
        fact_pre, fact_skill = create_fact_tables(df)

        pg_conn = get_postgres_connection_props()
        pg_url = pg_conn["url"]
        pg_props = pg_conn["props"]

        existing_jobs = load_existing_jobs(spark, pg_url, pg_props, fact_pre.schema)
        new_fact_pre = fact_pre.join(existing_jobs, on="job_url", how="left_anti")
        new_fact = new_fact_pre.withColumn("sector", explode("sector"))
        new_fact_skill = fact_skill.join(
            existing_jobs.withColumnRenamed("job_url", "job_url_fs"),
            on="job_url_fs",
            how="left_anti",
        )

        n_offres = new_fact_pre.count()
        n_skills = new_fact_skill.count()
        print(f"🆕 Nouvelles offres à insérer : {n_offres}")
        print(f"🆕 Nouvelles associations skill-offre : {n_skills}")

        dim_contract = spark.read.jdbc(
            pg_url, "public.dim_contract", properties=pg_props
        )
        dim_profile = spark.read.jdbc(pg_url, "public.dim_profile", properties=pg_props)
        dim_location = spark.read.jdbc(
            pg_url, "public.dim_location", properties=pg_props
        )
        dim_company = spark.read.jdbc(pg_url, "public.dim_company", properties=pg_props)
        dim_education = spark.read.jdbc(
            pg_url, "public.dim_education", properties=pg_props
        )
        dim_experience = spark.read.jdbc(
            pg_url, "public.dim_experience", properties=pg_props
        )
        dim_sector = spark.read.jdbc(pg_url, "public.dim_sector", properties=pg_props)

        fact_offer = (
            new_fact_pre.join(dim_contract, "contract_type", "left")
            .join(dim_profile, "profile", "left")
            .join(
                dim_location,
                (new_fact_pre.city == dim_location.city)
                & (new_fact_pre.country == dim_location.country),
                "left",
            )
            .join(dim_company, "company_name", "left")
            .join(dim_education, "education_level", "left")
            .join(dim_experience, "seniority", "left")
            .withColumn("source", lit("rekrute"))
            .select(
                "job_url",
                "title",
                "date_id",
                "contract_id",
                "location_id",
                "company_id",
                "profile_id",
                "education_id",
                "experience_id",
                "source",
            )
            .dropDuplicates(["job_url"])
        )

        print("🔍 Aperçu de fact_offer juste avant insertion :")
        fact_offer.show(5, truncate=False)

        fact_offer_sector = (
            new_fact.join(dim_sector, "sector", "left")
            .select("job_url", "sector_id")
            .dropDuplicates()
        )

        insert_to_postgres(dimensions, pg_url, pg_props)
        insert_to_postgres(
            {
                "fact_offer": fact_offer,
                "fact_offer_sector": fact_offer_sector,
                "fact_offer_skill": new_fact_skill,
            },
            pg_url,
            pg_props,
        )

        print("\n📊 RÉCAPITULATIF FINAL")
        print("────────────────────────────")
        print(f"✅ Offres insérées         : {n_offres}")
        print(f"✅ Compétences associées   : {n_skills}")
        print("✅ Dimensions mises à jour :")
        for dim in dimensions:
            print(f"   - {dim}")
        print("🚀 Pipeline terminé avec succès !")

    except Exception as e:
        print("❌ Une erreur fatale est survenue :")
        print(e)
        traceback.print_exc()
    finally:
        print("✅ Fin de Spark")
        spark.stop()


if __name__ == "__main__":
    main()
