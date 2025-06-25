#!/bin/sh
celery -A celery_app.tasks worker --loglevel=info -E &
uvicorn celery_app.api:app --host 0.0.0.0 --port 8000
