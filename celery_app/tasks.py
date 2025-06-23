import docker
from celery import chord, group, shared_task

from data_extraction.Websites import MarocAnn, Rekrute, bayt, emploi
from database import scraping_upload

# 🚀 Tâches de scraping


@shared_task(name="rekrute", bind=True, max_retries=3, default_retry_delay=10)
def rekrute_task(self):
    try:
        print("Appel du script rekrute")
        return Rekrute.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script rekrute: {e}")
        raise self.retry(exc=e)


@shared_task(name="bayt", bind=True, max_retries=3, default_retry_delay=10)
def bayt_task(self):
    try:
        print("Appel du script bayt")
        return bayt.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script bayt: {e}")
        raise self.retry(exc=e)


@shared_task(name="marocannonce", bind=True, max_retries=3, default_retry_delay=10)
def marocann_task(self):
    try:
        print("Appel du script maroc annonces")
        return MarocAnn.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script marocann: {e}")
        raise self.retry(exc=e)


@shared_task(name="emploi", bind=True, max_retries=3, default_retry_delay=10)
def emploi_task(self):
    try:
        print("Appel du script emploi")
        return emploi.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi: {e}")
        raise self.retry(exc=e)


# 📤 Tâche d'upload


@shared_task(name="scrape_upload")
def scrape_upload(results):
    try:
        print(f"Upload des résultats du scraping : {results}")
        scraping_upload()
        return "Upload terminé"
    except Exception as e:
        print(f"Exception lors de l'upload : {e}")
        return "Erreur pendant l'upload"


# 🧼 Tâche de lancement de Spark après scraping


@shared_task(name="spark_cleaning")
def spark_cleaning():
    client = docker.from_env()
    try:
        container = client.containers.run(
            image="spark_transform",
            name="spark_transform_job",
            command="spark-submit /opt/transform_job.py",  # adapte si différent
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                # Ajoute ici d'autres volumes si nécessaires
            },
            env_file=["/app/.docker.env"],
            detach=True,
            remove=True,
        )
        return f"Spark job lancé dans le conteneur : {container.name}"
    except docker.errors.APIError as e:
        return f"Erreur lors du lancement du Spark job : {str(e)}"


# 🔁 Workflow principal : scraping → upload → spark


@shared_task(name="scraping_workflow")
def scraping_workflow():
    scraping_tasks = group(
        emploi_task.s(), rekrute_task.s(), bayt_task.s(), marocann_task.s()
    )
    chord(scraping_tasks)(scrape_upload.s())
    print("Scraping terminé. Lancement du nettoyage Spark.")
    result = spark_cleaning.delay()
    return result.get()
