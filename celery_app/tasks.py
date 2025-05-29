from celery_app import app
from data_extraction.Websites import MarocAnn, Rekrute, bayt, emploi


@app.task(name="rekrute", bind=True, max_retries=3, default_retry_delay=10)
def rekrute_task(self):
    try:
        print("Appel du script rekrute")
        return Rekrute.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script rekrute: {e} ")
        raise self.retry(exc=e)


@app.task(name="bayt", bind=True, max_retries=3, default_retry_delay=10)
def bayt_task(self):
    try:
        print("Appel du script bayt")
        return bayt.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script bayt: {e} ")
        raise self.retry(exc=e)


@app.task(name="Marocannonce", bind=True, max_retries=3, default_retry_delay=10)
def marocann_task(self):
    try:
        print("Appel du script maroc annonces")
        return MarocAnn.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi marocann: {e} ")
        raise self.retry(exc=e)


@app.task(name="emploi", bind=True, max_retries=3, default_retry_delay=10)
def emploi_task(self):
    try:
        print("Appel du script emploi")
        return emploi.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi: {e} ")
        raise self.retry(exc=e)
