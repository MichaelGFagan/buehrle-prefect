from prefect import Flow, Parameter
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import tasks.bref_war as bref_war
import tasks.fangraphs as fangraphs
import tasks.chadwick as chadwick
import tasks.clone_dbt_repo as clone_dbt_repo
import tasks.dbt_shell_task as dbt_shell_task


schedule = Schedule(clocks=[CronClock("0 15 * * *")])

with Flow("WAR", schedule=schedule) as flow:
    chadwick = chadwick.chadwick()
    bref_war = bref_war.bref_war()
    fangraphs_start = Parameter('fangraphs_start', default=2022)
    fangraphs_end = Parameter('fangraphs_end', default=2022)
    fangraphs = fangraphs.fangraphs(start=fangraphs_start, end=fangraphs_end)
    clone_repo = clone_dbt_repo.clone_dbt_repo(
        upstream_tasks=[chadwick, bref_war, fangraphs],
    )
    deps = dbt_shell_task.dbt(
        command="dbt deps",
        task_args={"name": "dbt deps"},
        upstream_tasks=[clone_repo],
    )
    deps_output = dbt_shell_task.output_print(
        deps,
        task_args={"name": "dbt deps output"},
    )
    run = dbt_shell_task.dbt(
        command="dbt run -m +wins_above_replacement",
        task_args={"name": "dbt run"},
        upstream_tasks=[deps_output]
    )
    run_output = dbt_shell_task.output_print(
        run,
        task_args={"name": "dbt run output"}
    )


def flow_register():
    flow.register(project_name="buehrle")
