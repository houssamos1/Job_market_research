from celery import Celery

celery_app = Celery("celery_app")
celery_app.config_from_object("celery_app.celeryconfig")
celery_app.autodiscover_tasks()
