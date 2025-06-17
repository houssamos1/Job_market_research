import os
import time

from celery_app.tasks import scraping_workflow
from data_extraction.Websites import init_driver

print(f"The driver should be patched and put in {os.environ.get('CHROME_DRIVER_DIR')}")
print(f"The chrome binary should be patched and put in {os.environ.get('CHROME_BIN')}")

patch_driver = init_driver()
patch_driver.quit()
time.sleep(5)
scraping_workflow.delay()
