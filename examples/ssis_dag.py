import datetime

from airflow import DAG
from airflow.operators import SqlServerOperator, SsisPackageOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors import SsisPackageSensor

default_args = {
    "owner": "John Smith",
    "start_date": datetime.datetime.now(),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "provide_context": True,
}


def calculate_timeout(estimated_time):
    return (
        max(60, 60 * estimated_time // 4),
        max(60 * 2, int(60 * estimated_time * 1.375)),
    )


with DAG("ssis_dag", default_args=default_args, schedule_interval=None) as dag:

    start_dag = DummyOperator(task_id="start")

    execute_stored_procedure = SqlServerOperator(
        task_id="execute_stored_procedure",
        conn_id="SERVER_CON_NAME",
        database="DATABASE_NAME",
        sql="EXEC dbo.SOME_STORED_PROCEDURE",
    )

    run_package_task_id = "run_package"
    package_name = "SamplePackage.dtsx"

    run_package = SsisPackageOperator(
        task_id=run_package_task_id,
        conn_id="SERVER_CON_NAME",
        database="SSISDB",
        folder="Packages",
        project="SamplePackage",
        package=package_name,
    )

    poke_interval, timeout = calculate_timeout(3)
    check_execution_task_id = "check_execution"
    check_execution = SsisPackageSensor(
        task_id=check_execution_task_id,
        conn_id="SERVER_CON_NAME",
        database="SSISDB",
        xcom_task_id=run_package_task_id,
        poke_interval=poke_interval,
        timeout=timeout,
        retries=0,
        mode="reschedule",
    )

    send_email = EmailOperator(
        task_id="send_email",
        to="email@example.com",
        subject=f"{package_name} ran successfully",
        html_content=f"<p>{datetime.datetime.now()}</p>",
    )

    end_dag = DummyOperator(task_id="end_dag")

start_dag >> execute_stored_procedure >> run_package >> check_execution >> send_email >> end_dag
