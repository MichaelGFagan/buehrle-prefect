import prefect
from prefect import task
from prefect.tasks.dbt.dbt import DbtShellTask


dbt = DbtShellTask(
    return_all=True,
    profiles_dir='~/.dbt/profiles.yml',
    environment='dev',
    log_stdout=True,
    log_stderr=True,
)


@task(trigger=all_finished)
def output_print(output):
    logger = prefect.context.get("logger")
    for o in output:
        logger.info(o)