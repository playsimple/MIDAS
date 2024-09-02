import time
import schedule
from druid_autoscaler import AutoScalerFacade
from logging_config import logger


if __name__ == '__main__':
    logger.info("Starting Druid Middle-Manager Autoscaler")
    druid_autoscaler = AutoScalerFacade()
    schedule.every(30).seconds.do(druid_autoscaler.execute)
    while True:
        schedule.run_pending()
        time.sleep(15)
