import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
from prefect import task

credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)

@task
def chadwick():
    url = 'https://raw.githubusercontent.com/chadwickbureau/register/master/data/people.csv'
    df = pd.read_csv(url)
    project_id = 'baseball-source'
    table_id = 'chadwick.register'
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')
