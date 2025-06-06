from celery import Celery, group

from data_extraction.Websites import MarocAnn, Rekrute, bayt, emploi

# Names the app "celery_app"
app = Celery("celery_app")
# path to the default config for "celery_app"
default_config = "celery_app.celeryconfig"
app.config_from_object(default_config)


@app.task(name="rekrute", bind=True, max_retries=3, default_retry_delay=10)
def rekrute_task(self):
    """
    Tâche Celery pour exécuter le script Rekrute.

    Essaie d'appeler la fonction principale de Rekrute et gère les exceptions
    en réessayant jusqu'à 3 fois avec un délai par défaut de 10 secondes.

    Args:
        self: Instance liée de la tâche Celery.

    Returns:
        Résultat de l'exécution de Rekrute.main().
    """
    try:
        print("Appel du script rekrute")
        return Rekrute.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script rekrute: {e} ")
        raise self.retry(exc=e)


@app.task(name="bayt", bind=True, max_retries=3, default_retry_delay=10)
def bayt_task(self):
    """
    Tâche Celery pour exécuter le script Bayt.

    Essaie d'appeler la fonction principale de bayt et gère les exceptions
    en réessayant jusqu'à 3 fois avec un délai par défaut de 10 secondes.

    Args:
        self: Instance liée de la tâche Celery.

    Returns:
        Résultat de l'exécution de bayt.main().
    """
    try:
        print("Appel du script bayt")
        return bayt.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script bayt: {e} ")
        raise self.retry(exc=e)


@app.task(name="Marocannonce", bind=True, max_retries=3, default_retry_delay=10)
def marocann_task(self):
    """
    Tâche Celery pour exécuter le script MarocAnn.

    Essaie d'appeler la fonction principale de MarocAnn et gère les exceptions
    en réessayant jusqu'à 3 fois avec un délai par défaut de 10 secondes.

    Args:
        self: Instance liée de la tâche Celery.

    Returns:
        Résultat de l'exécution de MarocAnn.main().
    """
    try:
        print("Appel du script maroc annonces")
        return MarocAnn.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi marocann: {e} ")
        raise self.retry(exc=e)


@app.task(name="emploi", bind=True, max_retries=3, default_retry_delay=10)
def emploi_task(self):
    """
    Tâche Celery pour exécuter le script emploi.

    Essaie d'appeler la fonction principale de emploi et gère les exceptions
    en réessayant jusqu'à 3 fois avec un délai par défaut de 10 secondes.

    Args:
        self: Instance liée de la tâche Celery.

    Returns:
        Résultat de l'exécution de emploi.main().
    """
    try:
        print("Appel du script emploi")
        return emploi.main()
    except Exception as e:
        print(f"Exception lors de l'execution du script emploi: {e} ")
        raise self.retry(exc=e)


@app.task(name="web_scrape")
def web_scrape():
    """
    Tâche Celery pour exécuter un groupe de tâches de scraping web.

    Lance les tâches emploi_task, rekrute_task, bayt_task et marocann_task en parallèle
    et affiche les résultats une fois toutes les tâches terminées.

    Returns:
        None
    """
    scrapers = group(
        emploi_task.s(), rekrute_task.s(), bayt_task.s(), marocann_task.s()
    )
    result = scrapers.apply_async()
    return result
