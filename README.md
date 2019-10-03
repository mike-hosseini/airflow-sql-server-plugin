# Apache Airflow plugin for SQL Server

Kerberized SQL Server Plugin for Apache Airflow.

## Motivation

Apache Airflow's current SQL Server integration uses [pymssql](https://github.com/pymssql/pymssql) and it is no longer being maintained. In addition, the author also recommends people to use PyODBC which is a DB API module for ODBC and it is also listed on [Microsoft's website](https://docs.microsoft.com/en-us/sql/connect/sql-connection-libraries).

## Solution

This plugin makes available a set of Hooks and Operators to make Airflow work with PyODBC. This assumes you rely on Active Directory for user authentication and authorization. This means that if the node that's running your Airflow executor has a Kerberos ticket.

I had a use case for executing SQL Server Integration Services (SSIS) packages from Airflow so I have also made available an Operator and Sensor for it:

1. `SsisPackageOperator`
2. `SsisPackageSensor`

You could find instructions on how to use it in the `examples/` directory.

## Installation

This is a drop-in plugin for Apache Airflow, you could add it to the root of your Airflow installation under `plugins` and restart Airflow to have it available.

## Example DAGs

I have made available a sample DAG that makes use of this plugin.

* Operator for executing queries

This assumes you have created a connection on Airflow with no username and password, and just the FQDN or IP address of the SQL Server instance (`SERVER_CON_NAME`)

```python
from airflow.operators import SqlServerOperator

SqlServerOperator(
    task_id="execute_stored_procedure",
    conn_id="SERVER_CON_NAME",
    database="DATABASE_NAME",
    sql="EXEC dbo.SOME_STORED_PROCEDURE",
)
```

* Operator for executing SSIS packages


This assumes you have created a connection on Airflow with no username and password, and just the FQDN or IP address of the SQL Server instance (`SERVER_CON_NAME`) that hosts your SSIS Catalog.


```python
from airflow.operators import SsisPackageOperator

SsisPackageOperator(
    task_id=run_package_task_id,
    conn_id="SERVER_CON_NAME",
    database="SSISDB",
    folder="Packages",
    project="SamplePackage",
    package=package_name,
)

```

## Contributions

Currently, there is no support for SQL Server Authentication and for a good reason. If you ask any AppSec expert, it's better to tie permissions to roles than individuals logins. In any case, there may be legitimate uses for it so I am open to adding it in, so PRs are welcome.
