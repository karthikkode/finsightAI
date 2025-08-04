import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from pytz import utc

# Import the main functions from our data services
from stock_data_service.updater import run_daily_stock_update
from financial_news_service.updater import run_news_update

# Import database connection details from a shared config
# Note: We need a config file in the root or a shared location.
# For now, let's create a simple one.
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

# --- Scheduler Configuration ---

# The database URL for the job store. APScheduler uses SQLAlchemy.
jobstore_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

jobstores = {
    'default': SQLAlchemyJobStore(url=jobstore_url)
}

# We use the timezone to ensure jobs run at the correct local time
scheduler = AsyncIOScheduler(jobstores=jobstores, timezone=utc)

def schedule_jobs():
    """
    Defines and schedules all the recurring background jobs for the application.
    APScheduler is smart enough not to add duplicate jobs on restart if the
    job ID already exists in the persistent job store.
    """
    logging.info("Scheduling background jobs...")

    # --- Job 1: Daily Stock Price Update ---
    # This job runs once per day, Monday to Friday, at 6:00 PM UTC.
    # (Adjust the time as needed for your target market close)
    scheduler.add_job(
        run_daily_stock_update,
        'cron',
        day_of_week='mon-fri',
        hour=18,
        minute=0,
        id='daily_stock_update_job',  # A unique ID for the job
        replace_existing=True
    )
    logging.info("Scheduled: Daily stock price update (Mon-Fri at 18:00 UTC)")

    # --- Job 2: Financial News Update ---
    # This job runs every 4 hours, every day.
    scheduler.add_job(
        run_news_update,
        'interval',
        hours=4,
        id='hourly_news_update_job', # A unique ID for the job
        replace_existing=True
    )
    logging.info("Scheduled: Financial news update (every 4 hours)")

    logging.info("All jobs have been scheduled.")

