from dotenv import load_dotenv
from pathlib import Path
import os

from fetch import fetch_items
from apscheduler.schedulers.blocking import BlockingScheduler
import fetch

env_path = Path(".env")
if(not Path.exists(env_path)):
    raise FileNotFoundError(".env cannot be loaded") 
load_dotenv(env_path)

# Once on startup
fetch_items()

scheduler = BlockingScheduler()
# Schedule using standard cron fields
scheduler.add_job(fetch_items, 'interval', minutes=3600, id='fetch_items')

scheduler.start()
