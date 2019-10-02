from airflow.models import BaseOperator
from hooks.sqlserver_hook import SqlServerHook
from airflow.utils.decorators import apply_defaults


class SqlServerOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, conn_id, database, sql, parameters=None, *args, **kwargs
    ):
        super(SqlServerOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.database = database
        self.sql = sql
        self.parameters = parameters

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = SqlServerHook(conn_id=self.conn_id, schema=self.database)
        hook.run(self.sql, parameters=self.parameters)
