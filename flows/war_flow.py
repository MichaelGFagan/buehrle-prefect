from prefect import Flow
import tasks.bref_war as bref_war
import tasks.fangraphs as fangraphs

with Flow("WAR") as flow:
    bref_war.bref_war()
    fangraphs.fangraphs()

def flow_register():
    flow.register(project_name="buehrle")
