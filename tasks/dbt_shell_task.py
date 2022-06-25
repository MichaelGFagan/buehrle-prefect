import prefect
from prefect import task
from prefect.client import Secret
from prefect.tasks.dbt.dbt import DbtShellTask


dbt = DbtShellTask(
    return_all=True,
    profile_name='buehrle-dbt',
    overwrite_profiles=True,
    environment='dev',
    log_stdout=True,
    log_stderr=True,
    helper_script ="cd dbt",
    dbt_kwargs={
        "type": "bigquery",
        "method": "service-account-json",
        "project": Secret("BIGQUERY_PROJECT_ID").get(),
        "dataset": Secret("BIGQUERY_DATASET").get(),
        "threads": 4,
        "keyfile_json": {
            "type": "service_account",
            "project_id": Secret("BIGQUERY_PROJECT_ID").get(),
            "private_key_id": Secret("BIGQUERY_PRIVATE_KEY_ID").get(),
            "private_key": Secret("BIGQUERY_PRIVATE_KEY").get(),
            "client_email": Secret("BIGQUERY_CLIENT_EMAIL").get(),
            "client_id": Secret("BIGQUERY_CLIENT_ID").get(),
            "auth_uri": Secret("BIGQUERY_AUTH_URI").get(),
            "token_uri": Secret("BIGQUERY_TOKEN_URI").get(),
            "auth_provider_x509_cert_url": Secret("BIGQUERY_AUTH_PROVIDER_X509_CERT_URL").get(),
            "client_x509_cert_url": Secret("BIGQUERY_CLIENT_X509_CERT_URL").get(),
        }
    }
)


@task()
def output_print(output):
    logger = prefect.context.get("logger")
    for o in output:
        logger.info(o)