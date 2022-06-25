import prefect
from prefect import task
from prefect.tasks.dbt.dbt import DbtShellTask


dbt = DbtShellTask(
    return_all=True,
    profile_name='buehrle_dbt',
    profiles_dir='/Users/michaelfagan/.dbt/',
    environment='dev',
    log_stdout=True,
    log_stderr=True,
    helper_script ="cd dbt",
)


@task()
def output_print(output):
    logger = prefect.context.get("logger")
    for o in output:
        logger.info(o)