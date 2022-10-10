from dagster import schedule

from etl.jobs.run_etl import run_etl_job

# cron_schedule = (minute hour day/month month day/week)
@schedule(cron_schedule="1 * * * *", job=run_etl_job, execution_timezone="US/Central")
def etl_job_schedule(_context):
    run_config = {}
    return run_config
