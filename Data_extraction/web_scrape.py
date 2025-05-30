import os
import time

from celery_app.tasks import web_scrape
from data_extraction.Websites import init_driver

patch_driver = init_driver()
patch_driver.quit()
print(f"The driver should be patched and put in {os.environ.get('CHROME_DRIVER_BIN')}")
time.sleep(5)

web_scrape.delay()
