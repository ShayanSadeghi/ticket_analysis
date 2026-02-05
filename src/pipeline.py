import subprocess
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    OpExecutionContext,
    ScheduleDefinition,
    get_dagster_logger,
    job,
    op,
)
from data_collector.scraper import AirlinePricingCollector

dagster_logger = get_dagster_logger()


@op
def run_scraper(context: OpExecutionContext) -> bool:
    """Run scraper script"""
    context.log.info("Running scraper")
    collector = AirlinePricingCollector(config_path="src/data_collector/config.yaml")
    collector.collect_data()
    context.log.info("Scraper completed successfully")

    return True


@op
def run_dbt(context: OpExecutionContext, scrape_result: bool):
    """Run dbt in analytics directory"""
    analytics_dir = Path(__file__).parent.parent / "analytics"

    context.log.info(f"Running dbt from {analytics_dir}")

    # Run dbt commands
    subprocess.run(["dbt", "run"], cwd=analytics_dir, check=True)
    context.log.info("dbt run completed")


@job
def scraper_pipeline():
    """Main pipeline: scraper -> dbt"""
    scrape_result = run_scraper()
    run_dbt(scrape_result)


scraper_schedule = ScheduleDefinition(
    job=scraper_pipeline,
    cron_schedule="*/15 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="UTC",
)
