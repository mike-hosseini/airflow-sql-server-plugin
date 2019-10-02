from airflow.hooks.dbapi_hook import DbApiHook
from contextlib import closing
from sqlalchemy import create_engine
import urllib


class SqlServerHook(DbApiHook):
    conn_name_attr = "conn_id"
    default_conn_name = "sqlserver_default"
    supports_autocommit = False
    trusted_connection = "yes"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self.driver = kwargs.pop("driver", "{ODBC Driver 17 for SQL Server}")

    def get_uri(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))

        formatted_connection_string = "DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection={trusted_connection}".format(
            driver=self.driver,
            server=conn.host,
            database=self.schema or conn.schema,
            trusted_connection="yes",
        )

        return "mssql+pyodbc:///?odbc_connect={parameters}".format(
            parameters=urllib.parse.quote_plus(formatted_connection_string)
        )

    def get_sqlalchemy_engine(self, engine_kwargs=dict(fast_executemany=True)):
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_engine(self.get_uri(), **engine_kwargs)

    def get_conn(self):
        engine = self.get_sqlalchemy_engine()
        return engine.raw_connection()
