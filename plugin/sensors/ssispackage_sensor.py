from airflow.sensors.base_sensor_operator import BaseSensorOperator
from hooks.sqlserver_hook import SqlServerHook
from airflow.utils.decorators import apply_defaults


class PackageExecutionError(Exception):
    def __init__(self, message, package_name, execution_status):
        super(PackageExecutionError, self).__init__(
            message, package_name, execution_status
        )
        self.message = message
        self.package_name = package_name
        self.execution_status = execution_status


class SsisPackageSensor(BaseSensorOperator):
    sql_query = """
    SELECT CASE
                WHEN status = 1 THEN 'Created'
                WHEN status = 2 THEN 'Running'
                WHEN status = 3 THEN 'Canceled'
                WHEN status IN ( 4, 6 ) THEN 'Failure'
                WHEN status = 5 THEN 'Pending'
                WHEN status = 7 THEN 'Success'
                WHEN status = 8 THEN 'Stopping'
                WHEN status = 9 THEN 'Completed'
                ELSE 'Failure' END AS [status_desc],
           package_name
      FROM SSISDB.catalog.executions
     WHERE execution_id = ?
     ORDER BY created_time DESC"""

    @apply_defaults
    def __init__(
        self,
        conn_id,
        database,
        sql=None,
        parameters=None,
        xcom_task_id=None,
        *args,
        **kwargs,
    ):
        super(SsisPackageSensor, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.sql = SsisPackageSensor.sql_query if not sql else sql
        self.parameters = parameters
        self.xcom_task_id = xcom_task_id

    def poke(self, context):
        hook = SqlServerHook(conn_id=self.conn_id, schema=self.database)

        execution_id = context["task_instance"].xcom_pull(
            self.xcom_task_id, key="execution_id"
        )

        self.log.info(
            "Poking: %s (with execution_id %s)", self.conn_id, execution_id
        )

        records = hook.get_first(
            self.sql, execution_id if not self.parameters else self.parameters
        )

        if not records:
            return False

        self.log.info(f"Current status: {records[0]}")

        termination_flag = records[0] in (
            "Canceled",
            "Completed",
            "Failure",
            "Pending",
            "Stopping",
            "Success",
        )

        if termination_flag:
            task_instance = context["task_instance"]
            task_instance.xcom_push(key="execution_status", value=records[0])
            task_instance.xcom_push(key="package_name", value=records[1])

        if records[0] in ("Failure",):
            raise PackageExecutionError(
                message="Package execution ended abnormally",
                package_name=records[1],
                execution_status=records[0],
            )

        return termination_flag
