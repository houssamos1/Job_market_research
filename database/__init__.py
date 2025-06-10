import os

from minio import Minio, S3Error


def start_client() -> Minio:
    """This function simply connects to the minio server and returns a client instance.

    It is supposed to be used in other modules to create and maintain connections to the Object sotrage server
    """
    MINIO_URL = os.environ.get("MINIO_API")
    ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
    SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
    # --- Connexion à MinIO ---
    client = Minio(
        MINIO_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )
    return client


def make_buckets(bucket_list: list = ["webscraping", "traitement"]):
    client = start_client()
    for bucket_name in bucket_list:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' créé.")
        else:
            print(f"Bucket '{bucket_name}' déjà existant.")


def save_json(
    bucket_name="webscraping",
    object_name="default",
    file_path="data_extraction/default.json",
    content_type="application/json",
):
    try:
        client = start_client()
        client.fput_object(bucket_name, object_name, file_path, content_type)
        print(f" Upload : {object_name}")
    except S3Error as err:
        print(f" Erreur : {object_name} → {err}")
