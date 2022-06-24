from prefect import Flow, Parameter
import tasks.bref_war as bref_war
import tasks.fangraphs as fangraphs

with Flow("WAR") as flow:
    bref_war.bref_war()
    fangraphs_start=Parameter('fangraphs_start', default=2022)
    fangraphs_end = Parameter('fangraphs_end', default=2022)
    fangraphs.fangraphs(start=fangraphs_start, end=fangraphs_end)

def flow_register():
    flow.register(project_name="buehrle")
