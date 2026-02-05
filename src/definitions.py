from dagster import Definitions
from pipeline import scraper_pipeline, scraper_schedule

defs = Definitions(
    jobs=[scraper_pipeline],
    schedules=[scraper_schedule],
)