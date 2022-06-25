import shutil

import prefect
from prefect import task
from prefect.client import Secret
import pygit2


@task(name="Clone dbt repo")
def clone_dbt_repo():
    logger = prefect.context.get("logger")
    shutil.rmtree("dbt", ignore_errors=True)  # Delete folder on run
    git_token = Secret("GITHUB_ACCESS_TOKEN").get()
    dbt_repo_name = "buehrle-dbt"
    dbt_repo = (
        f"https://{git_token}:x-oauth-basic@github.com/MichaelGFagan/{dbt_repo_name}"
    )

    pygit2.clone_repository(dbt_repo, "dbt")