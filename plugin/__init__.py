from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import hooks
import sensors


class SQLServerPlugin(AirflowPlugin):
    name = "sql_server_plugin"
    operators = [operators.SqlServerOperator, operators.SsisPackageOperator]
    hooks = [hooks.SqlServerHook]
    sensors = [sensors.SsisPackageSensor]
