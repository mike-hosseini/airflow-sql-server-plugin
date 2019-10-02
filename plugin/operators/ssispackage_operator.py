from airflow.models import BaseOperator
from hooks.sqlserver_hook import SqlServerHook
from airflow.utils.decorators import apply_defaults


class SsisPackageOperator(BaseOperator):
    sql_query = """
    DECLARE	@execution_id bigint
    
    EXEC	ssisdb.catalog.create_execution
    		@folder_name = N'{folder}',
    		@project_name = N'{project}',
    		@package_name = N'{package}',
    		@execution_id = @execution_id OUTPUT
    
    EXEC ssisdb.catalog.start_execution
        @execution_id
    
    COMMIT
    
    SELECT	@execution_id"""

    @apply_defaults
    def __init__(
        self, conn_id, database, folder, project, package, *args, **kwargs
    ):
        super(SsisPackageOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.folder = folder
        self.project = project
        self.package = package

    def execute(self, context):
        sqlserver_hook = SqlServerHook(
            conn_id=self.conn_id, schema=self.database
        )

        sql = SsisPackageOperator.sql_query.format(
            folder=self.folder, project=self.project, package=self.package
        )

        self.log.info("Running package")
        self.log.info(sql)

        result = sqlserver_hook.get_first(sql)

        if not result or len(result) < 1:
            self.log.info(result)
            raise ValueError("No execution ID was returned")

        task_instance = context["task_instance"]
        task_instance.xcom_push(key="execution_id", value=result[0])
