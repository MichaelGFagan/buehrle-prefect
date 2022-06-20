import prefect
from prefect import Flow
from ..tasks.bref_war import bbref_war
from ..tasks.fangraphs import fangraphs

with Flow("WAR") as flow:
    bbref_war()
    fangraphs()

flow.register(project_name="buehrle")