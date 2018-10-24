import time
import requests
from apscheduler.schedulers.blocking import BlockingScheduler 

def psd_job():
    requests.get('http://api/psd')   

schedule=BlockingScheduler()
schedule.add_job(psd_job,'cron', hour=19, minute=39, second=30)

schedule.start()