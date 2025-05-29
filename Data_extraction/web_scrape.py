from celery_app.tasks import (
    bayt_task,
    emploi_task,
    init_driver,
    marocann_task,
    rekrute_task,
)

try:
    driver = init_driver()
    driver.quit()
except Exception as e:
    print(f"Couldn't initialize driver: {e}")
rekrute_task.delay()
bayt_task.delay()
emploi_task.delay()
marocann_task.delay()
