import subprocess
from pathlib import Path

from dagster import (
    DefaultScheduleStatus,
    OpExecutionContext,
    ScheduleDefinition,
    get_dagster_logger,
    job,
    op,
    Definitions,
)
from src.data_collector.scraper import AirlinePricingCollector, TrainPricingCollector

dagster_logger = get_dagster_logger()

# ✅ Get the absolute path of THIS file (pipeline.py)
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parent.parent  # Goes from src/ to root/


@op
def run_scraper(context: OpExecutionContext) -> bool:
    """Run scraper script"""
    context.log.info("Running scraper")
    
    # ✅ Config is at: src/data_collector/config.yaml
    config_path = THIS_FILE.parent / "data_collector" / "config.yaml"
    
    context.log.info(f"Loading config from: {config_path.absolute()}")
    context.log.info(f"Config exists: {config_path.exists()}")
    
    # collector = AirlinePricingCollector(config_path=str(config_path))
    # collector.collect_data()
    with TrainPricingCollector() as collector:
        collector.collect_data()
    
    context.log.info("Scraper completed successfully")
    return True


@op
def run_dbt(context: OpExecutionContext, scrape_result: bool):
    """Run dbt in analytics directory"""
    
    # ✅ Analytics is at: root/analytics (one level UP from src/)
    analytics_dir = PROJECT_ROOT / "analytics"
    
    context.log.info(f"Running dbt from: {analytics_dir.absolute()}")
    context.log.info(f"Directory exists: {analytics_dir.exists()}")
    context.log.info(f"Is directory: {analytics_dir.is_dir()}")
    
    if not analytics_dir.exists():
        raise FileNotFoundError(f"Analytics directory not found at {analytics_dir.absolute()}")
    
    # ✅ Check if dbt_project.yml exists (validates it's a dbt project)
    dbt_project = analytics_dir / "dbt_project.yml"
    context.log.info(f"dbt_project.yml exists: {dbt_project.exists()}")
    
    subprocess.run(["dbt", "run"], cwd=str(analytics_dir), check=True)
    context.log.info("dbt run completed")


@job
def scraper_pipeline():
    """Main pipeline: scraper -> dbt"""
    scrape_result = run_scraper()
    run_dbt(scrape_result)


scraper_schedule = ScheduleDefinition(
    job=scraper_pipeline,
    cron_schedule="*/5 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="UTC",
)

# ✅ Required for Dagster to load your definitions
defs = Definitions(
    jobs=[scraper_pipeline],
    schedules=[scraper_schedule],
)