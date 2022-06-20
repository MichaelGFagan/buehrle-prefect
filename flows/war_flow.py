from prefect import Flow
from tasks.bref_war import bref_war
from tasks.fangraphs import fangraphs

with Flow("WAR") as flow:
    bref_war()
    fangraphs()

flow.register(project_name="buehrle")