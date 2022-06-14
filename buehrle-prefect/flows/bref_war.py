import pandas
import pandas_gbq
from google.oauth2 import service_account
from prefect import task

files_dict = {
    'bbref_war.batting': 'https://www.baseball-reference.com/data/war_daily_bat.txt',
    'bbref_war.pitching': 'https://www.baseball-reference.com/data/war_daily_pitch.txt'
}

credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)

@task
def bbref_war():
    for file in files_dict:
        url = files_dict[file]
        df = pandas.read_csv(url)
        project_id = 'baseball-source'
        table_id = file
        pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')
