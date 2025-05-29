from celery_app.tasks import rekrute_task

rekrute_task.delay()
