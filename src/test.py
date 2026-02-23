from dagster import op, job, ScheduleDefinition,DefaultScheduleStatus

@op
def test_op():
    print("hey there")


@job
def test_job():
    test_op()



test_schedule = ScheduleDefinition(
    job=test_job,
    cron_schedule="1 * * * *"
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone='Asia/Tehran'
)