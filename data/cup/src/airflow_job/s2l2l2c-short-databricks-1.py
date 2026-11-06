import json
import random
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.utils.task_group import TaskGroup

import logging

import boto3

import pendulum

DAG_ID = "S2L2L2C-short-Pipeline-databricks"

DEFAULT_ARGS = {
    "owner": "cepal",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": pendulum.duration(seconds=120),
}

ssm_client = boto3.client("ssm", region_name="eu-central-1")

# Get notification Airflow Status arn
airflow_status_arn = ssm_client.get_parameter(Name="/cfn/sns/airflow_status/arn")[
    "Parameter"
]["Value"]

# fmt: off
landing_databricks_job = ssm_client.get_parameter(
    Name="/cfn/databricks/job/landing/name"
)["Parameter"]["Value"]
curated_databricks_job = ssm_client.get_parameter(
    Name="/cfn/databricks/job/curated/name"
)["Parameter"]["Value"]

landing_daily_bucket = ssm_client.get_parameter(
    Name="/cfn/s3/landing-daily/name"
)["Parameter"]["Value"]

landing_bucket = ssm_client.get_parameter(
    Name="/cfn/s3/landing/name"
)["Parameter"]["Value"]
curated_bucket = ssm_client.get_parameter(
    Name="/cfn/s3/curated/name"
)["Parameter"]["Value"]

#catalog_name_staging = ssm_client.get_parameter(   #glue catalog  name of catalog like d-ec1-cepal-poc-staging-area
#    Name="/cfn/glue/catalog/poc/staging/name"
#)["Parameter"]["Value"]
catalog_name_landing = ssm_client.get_parameter(
    Name="/cfn/glue/catalog/landing/name"
)["Parameter"]["Value"]

catalog_name_landed = ssm_client.get_parameter(
     Name="/cfn/glue/catalog/landed/name"
 )["Parameter"]["Value"]

catalog_name_reference = ssm_client.get_parameter(
     Name="/cfn/glue/catalog/reference/name"
 )["Parameter"]["Value"]

catalog_name_curated = ssm_client.get_parameter(
     Name="/cfn/glue/catalog/curated/name"
 )["Parameter"]["Value"]

airflow_env = ssm_client.get_parameter(
    Name="/cfn/airflow/environment"
)["Parameter"]["Value"]
infrastructure_environment = ssm_client.get_parameter(
    Name="/cfn/infrastructure/environment"
)["Parameter"]["Value"]

# if airflow_env == "dev":
#     curated_glue_job = ssm_client.get_parameter(
#         Name="/cfn/glue/job/curated/name"
#     )["Parameter"]["Value"]
# elif airflow_env == "qa":
#     curated_glue_job = ssm_client.get_parameter(
#         Name="/cfn/glue/job/curated/name"
#     )["Parameter"]["Value"]
# else:
#     curated_glue_job = ssm_client.get_parameter(
#         Name="/cfn/glue/job/curated/name"
#     )["Parameter"]["Value"]

if airflow_env == "dev":
    airflow_env_extended = 'Development'
elif airflow_env == "qa":
    airflow_env_extended = 'QA'
elif airflow_env == "prd":
    airflow_env_extended = 'Production'
else:
    airflow_env_extended = 'Unknown'

#landing_dwh_stg_schema_crawler = f"{infrastructure_environment}-ec1-cepal-daily-lng"
#landed_dwh_stg_schema_crawler = f"{infrastructure_environment}-ec1-cepal-daily-landed"

#curated_crawler = f"{infrastructure_environment}-ec1-cepal-crawler-curated"

# fmt: on

S3_LANDING_CONNECTION = "s3_landing_connection"
REDIS_CONNECTION = "redis_connection"
MSSQL_CONNECTION = "mssql_staging_conn_prod"
DATABRICKS_DEV_CONNECTION = "databricks_default"
SCHEMATA = f"{airflow_env}-LoadToLanding:staging:schemaList"
DB_NAME = "dwh_stg"
SERVER_NAME = "dwhcore"

DATABRICKS_CLUSTER = "Ilias Karalis's Cluster 2"


def get_schemata_from_redis():
    """
    Retrieves schemata from Redis and returns them as a list.

    Returns:
        list: A list of schemata retrieved from Redis.
    """
    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()
    schema_string = redis_conn.get(SCHEMATA)
    schema_list = schema_string.decode().split(",")
    # schema_set = set(schema_list)
    # schema_stol = list(schema_set)
    # arotron_index = schema_stol.index('arotron')
    # profits_index = schema_stol.index('profits')
    # schema_test = [schema_stol[arotron_index], schema_stol[profits_index]]
    # #schema_test = [schema_stol[cti_index]]
    
    # print(f'Schema taken: {schema_test}')
    # #schema_test = ['arotron']
    return schema_list
    #return schema_string.decode().split(",")


def get_groups_of_schema(schema_name):
    """
    Retrieves the groups of a schema from Redis and returns them as a list.

    Args:
        schema_name (str): The name of the schema.

    Returns:
        list: A list of groups associated with the specified schema.
    """
    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()
    groups_string = redis_conn.get(
        f"{airflow_env}-LoadToLanding:staging:{schema_name}:groupList"
    )
    return groups_string.decode().split(",")


# def get_tables_of_group_json(redis_key):
#     redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
#     redis_conn = redis_hook.get_conn()
#     result = redis_conn.execute_command('JSON.GET', redis_key, '.tables')
#     if result:
#         result_list = json.loads(result)
#         return result_list
#     else:
#         return None


def get_tables_of_group(redis_key):
    """
    Retrieves the tables associated with a group from Redis.

    Args:
        redis_key (str): The Redis key to retrieve the tables from.

    Returns:
        str or None: The tables associated with the group as a string, or None if the key is not found.
    """
    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()
    result_string = redis_conn.get(redis_key)
    if result_string:
        return result_string.decode()
    else:
        return None


def get_date_to_run(dag_run):
    """
    Retrieves the date to run from the DAG run configuration or uses the current date as a fallback.

    Args:
        **context: The context dictionary containing the 'dag_run' and 'ti' objects.

    Returns:
        str: The date to run.

    """
    dag_run_config = dag_run.conf
    try:
        date_to_run = dag_run_config["date_to_run"]
    except KeyError:
        date_to_run = datetime.now().strftime("%Y-%m-%d")

    return date_to_run


def _set_redis_key(redis_key, redis_value):
    """
    Sets a Redis key-value pair.

    Args:
        redis_key (str): The key to set in Redis.
        redis_value (str): The value to associate with the key.

    """
    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()
    redis_conn.set(redis_key, redis_value)


def compare_dates(schema_name, group_name, **kwargs):
    """
    Compares the date to run and the last execution date for a given schema and group.

    Args:
        schema_name (str): The name of the schema.
        group_name (str): The name of the group.
        **kwargs: Additional keyword arguments containing the 'task_instance' object.

    Returns:
        str: The task ID of the appropriate action based on the date comparison.

    """
    date_to_run = kwargs["task_instance"].xcom_pull(task_ids="get_date_to_run_task")
    last_execution_date = kwargs["task_instance"].xcom_pull(
        task_ids=f"{schema_name}_landing.{schema_name}_{group_name}.get_last_execution_date_task"
    )

    date_to_run = datetime.strptime(date_to_run, "%Y-%m-%d").date()
    last_execution_date = datetime.strptime(last_execution_date, "%Y-%m-%d").date()

    if last_execution_date < date_to_run:
#        return f"{schema_name}_landing.{schema_name}_{group_name}.delete_landing_daily_tables_task"
        return f"{schema_name}_landing.{schema_name}_{group_name}.submit_s2l2l_zone_databricks_job"
    else:
        return f"{schema_name}_landing.{schema_name}_{group_name}.group_has_already_been_processed"


def get_redis_last_execution_date(schema_name, group_name):
    """
    Retrieves the last execution date from Redis for a given schema and group.

    Args:
        schema_name (str): The name of the schema.
        group_name (str): The name of the group.

    Returns:
        str: The last execution date as a string.

    """
    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()
    last_execution_date_string = redis_conn.get(
        f"{airflow_env}-LoadToLanding:staging:{schema_name}:{group_name}:lastExecution"
    )
    return last_execution_date_string.decode()


def get_redis_string(redis_key):
    """
    Retrieves the last execution date from Redis for a given schema and group.

    Args:
        redis_key (str): The Redis key to retrieve the tables from.

    Returns:
        str: The last execution date as a string.

    """
    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()
    redis_string = redis_conn.get(redis_key)
    if redis_string:
        return redis_string.decode()
    else:
        return None


def get_s3_folder_names(bucket_name, s3_path):
    """
    Retrieve the names of folders within a given S3 bucket and path.

    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_path (str): The path within the S3 bucket.

    Returns:
        list: A list of folder names within the specified S3 bucket and path.
    """
    import boto3

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_path, Delimiter="/")

    folder_names = []
    for prefix in response.get("CommonPrefixes", []):
        # Extract the folder name from the prefix
        folder_path = prefix["Prefix"]
        folder_name = folder_path.rstrip("/").split("/")[-1]
        folder_names.append(folder_name)

    return folder_names


def delete_files_from_landing(schema_name, **kwargs):
    """
    Deletes files from landing S3 bucket for a given schema.

    Args:
        schema_name (str): The name of the schema.
        **kwargs: Additional keyword arguments containing the 'task_instance' object.

    """
    date_to_run = kwargs["task_instance"].xcom_pull(task_ids="get_date_to_run_task")
    tables = get_s3_folder_names(
        landing_bucket, f"{SERVER_NAME}/{DB_NAME}/{schema_name}/"
    )
    s3_hook = S3Hook(aws_conn_id=S3_LANDING_CONNECTION)

    for table in tables:
        destination_prefix = f"{SERVER_NAME}/{DB_NAME}/{schema_name}/{table}/{date_to_run.replace('-', '')}/"
        files_to_delete = s3_hook.list_keys(
            bucket_name=landing_bucket,
            prefix=destination_prefix,
            delimiter="",
            page_size=None,
            max_items=None,
        )
        s3_hook.delete_objects(bucket=landing_bucket, keys=files_to_delete)


def delete_files_from_landing_daily(schema_name, tables_to_delete):
    """
    Delete files from an S3 location based on specified schema and table names.

    Args:
        schema_name (str): Name of the schema under which the tables are located.
        tables_to_delete (List[str]): List of table names from which files need to be deleted.

    """
    s3_hook = S3Hook(aws_conn_id=S3_LANDING_CONNECTION)

    for table in tables_to_delete:
        destination_prefix = f"{SERVER_NAME}/{DB_NAME}/{schema_name}/{table}/"
        files_to_delete = s3_hook.list_keys(
            bucket_name=landing_daily_bucket,
            prefix=destination_prefix,
            delimiter="",
            page_size=None,
            max_items=None,
        )
        s3_hook.delete_objects(bucket=landing_daily_bucket, keys=files_to_delete)


def delete_s3_folders(bucket_name, prefixes):
    """
    Deletes multiple folders (prefixes) from an S3 bucket.
    Args:
        bucket_name: Name of the S3 bucket
        param prefixes: List of prefixes (folder paths) to delete
    """
    s3_hook = S3Hook()

    for prefix in prefixes:
        # List objects within the prefix
        objects_to_delete = s3_hook.list_keys(bucket_name, prefix=prefix)

        if objects_to_delete:
            # Delete objects
            s3_hook.delete_objects(bucket_name, objects_to_delete)
        else:
            print(f"No objects found for prefix: {prefix}")


def copy_files_to_landing(schema_name, **kwargs):
    """
    Copies files from landing daily S3 bucket to landing S3 bucket for a given schema.

    Args:
        schema_name (str): The name of the schema.
        **kwargs: Additional keyword arguments containing the 'task_instance' object.

    """
    date_to_run = kwargs["task_instance"].xcom_pull(task_ids="get_date_to_run_task")
    source_prefix = f"{SERVER_NAME}/{DB_NAME}/{schema_name}/"
    s3_hook = S3Hook(aws_conn_id=S3_LANDING_CONNECTION)
    files_to_copy = s3_hook.list_keys(
        bucket_name=landing_daily_bucket,
        prefix=source_prefix,
        delimiter="",
        page_size=None,
        max_items=None,
    )

    for file in files_to_copy:
        source_bucket_key = file
        dest_bucket_key = (
            file[: file.rfind("/") + 1]
            + date_to_run.replace("-", "")
            + file[file.rfind("/") :]
        )
        s3_hook.copy_object(
            source_bucket_name=landing_daily_bucket,
            source_bucket_key=source_bucket_key,
            dest_bucket_name=landing_bucket,
            dest_bucket_key=dest_bucket_key,
        )


def get_business_date(**kwargs):
    """
    Retrieves the business date from Redis for given execution date.

    Args:
        **kwargs: Additional keyword arguments containing the 'task_instance' object.

    Returns:
        str: Business date corresponding to execution date
    """
    date_to_run = kwargs["task_instance"].xcom_pull(task_ids="get_date_to_run_task")

    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()

    business_date = redis_conn.get(
        f"calendar:BusinessDateFromIngestion:{date_to_run.replace('-', '')}"
    )
    return business_date.decode()


def get_tables_conf_from_redis(tables_of_group):
    """
    Retrieves tables with their configurations from Redis.

    Args:
        tables_of_group (str | None): The tables associated with the group or None.

    Returns:
        str: A JSON str with tables and their configurations.
    """
    redis_hook = RedisHook(redis_conn_id=REDIS_CONNECTION)
    redis_conn = redis_hook.get_conn()

    table_list = tables_of_group.split(",")

    tables_conf = {}
    for table in table_list:
        ingestion_case = redis_conn.get(f"{airflow_env}-Landed:DataMode:{table}")

        if ingestion_case:
            if ingestion_case.decode() == "incremental":
                primary_keys = redis_conn.get(f"{airflow_env}-Landed:Keys:{table}")
                tables_conf[table] = {
                    "ingestion_case": ingestion_case.decode(),
                    "primary_keys": primary_keys.decode(),
                }
            elif ingestion_case.decode() == "full":
                tables_conf[table] = {
                    "ingestion_case": ingestion_case.decode(),
                }
            else:
                print(ingestion_case.decode())
                tables_conf[table] = {"ingestion_case": None}

        else:
            tables_conf[table] = {"ingestion_case": None}

    return json.dumps(tables_conf)


def print_diagnostics():
    logging.info("Starting diagnostics...")
    logging.info(f'Database name: {DB_NAME}')
    logging.info(f'Schema name: {schema_name}')
    logging.info(tables_from_redis)
    logging.info(table_list_redis_key)
    logging.info(groups_to_process)
    #logging.info(tables)
    logging.info(f'Server: {SERVER_NAME}')


with DAG(
    dag_id=DAG_ID,
    description="The short databricks ETL pipeline | Staging - Landing - Landed",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=pendulum.duration(hours=22),
    start_date=datetime(2024, 2, 21),
    catchup=False,
    schedule="0 3 * * 2-6",  # UTC
    tags=["S2L2L-short"],
) as dag:
    start, end = [EmptyOperator(task_id=tid) for tid in ["start", "end"]]

    staging_db = EmptyOperator(task_id="staging_db")

#    portfolio = EmptyOperator(task_id="portfolio")

    get_date_to_run_task = PythonOperator(
        task_id="get_date_to_run_task",
        python_callable=get_date_to_run,
    )

    get_business_date_task = PythonOperator(
        task_id="get_business_date_task",
        python_callable=get_business_date,
    )

    get_schemata_from_redis_task = PythonOperator(
        task_id="get_schemata_from_redis_task",
        python_callable=get_schemata_from_redis,
    )

    #schemata_to_process = ['arotron']
    schemata_to_process = get_schemata_from_redis()

    dwh_pld_sensor = SqlSensor(
        task_id=f"wait_dwh_pld_success",
        conn_id=MSSQL_CONNECTION,
        sql=f"SELECT CASE WHEN pld_dwh = '{{{{ ti.xcom_pull(task_ids='get_date_to_run_task') }}}}' THEN 1 ELSE NULL END AS date_check FROM process.schema_refresh_date",
        poke_interval=300,  # 5 minutes
        timeout=75600,  # 22 hours
    )

    business_date = f"{{{{ ti.xcom_pull(task_ids='get_business_date_task')[0:4] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[4:6] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[6:8] }}}}"
    ingestion_date = f"{{ ti.xcom_pull(task_ids='get_date_to_run_task') }}"
    send_start_notification_task = SnsPublishOperator(
        task_id="send_start_notification",
        target_arn=airflow_status_arn,
        message=f"The Dag S2L2L2C has been successfully started for BusinessDate: {business_date} ",
        subject=f"Status : Start | Dag S2L2L2C | {airflow_env_extended} Airflow",
    )

    send_finish_notification_task = SnsPublishOperator(
        task_id="send_finish_notification",
        target_arn=airflow_status_arn,
        message=f"The Dag S2L2L2C has been successfully completed for BusinessDate: {business_date} ",
        subject=f"Status : Finish | Dag S2L2L2C | {airflow_env_extended} Airflow",
    )

    for schema_name in schemata_to_process:
        logging.info(f"Schema name: {schema_name}")
    #    trigger_landing_dwh_stg_schema_crawler = GlueCrawlerOperator(
    #        task_id=f"trigger_landing_dwh_stg_{schema_name}_crawler",
    #        config={"Name": f"{landing_dwh_stg_schema_crawler}-{schema_name}"},
    #    )

    #    trigger_landed_dwh_stg_schema_crawler = GlueCrawlerOperator(
    #        task_id=f"trigger_landed_dwh_stg_{schema_name}_crawler",
    #        config={"Name": f"{landed_dwh_stg_schema_crawler}-{schema_name}"},
    #    )

        groups_to_process = get_groups_of_schema(schema_name)
        #groups_to_process = ['arotron02', 'arotron03', 'arotron01']

        # [START schema_landing_task_group]
        with TaskGroup(group_id=f"{schema_name}_landing") as schema_landing:
            schema_sensor = SqlSensor(
                task_id=f"sql_sensor",
                conn_id=MSSQL_CONNECTION,
                sql=f"SELECT CASE WHEN {schema_name} = '{{{{ ti.xcom_pull(task_ids='get_date_to_run_task') }}}}' AND pld_dwh = '{{{{ ti.xcom_pull(task_ids='get_date_to_run_task') }}}}' THEN 1 ELSE NULL END AS date_check FROM process.schema_refresh_date",
                poke_interval=300,  # 5 minutes
                timeout=75600,  # 22 hours
            )

            end_schema = EmptyOperator(task_id=f"end_schema")

            schema_group_landing_groups = []
            for group_name in groups_to_process:
                table_list_redis_key = f"{airflow_env}-LoadToLanding:staging:{schema_name}:{group_name}:tableList"

                tables_from_redis = get_tables_of_group(table_list_redis_key)
                tables_to_delete_in_landing_daily = [
                    table for table in tables_from_redis.split(",")
                ]

                # [START schema_group_landing_task_group]
                with TaskGroup(
                    group_id=f"{schema_name}_{group_name}"
                ) as schema_group_landing:
                    get_group_last_execution_date_task = PythonOperator(
                        task_id=f"get_last_execution_date_task",
                        python_callable=get_redis_last_execution_date,
                        op_kwargs={
                            "schema_name": f"{schema_name}",
                            "group_name": f"{group_name}",
                        },
                    )

                    compare_dates_task = BranchPythonOperator(
                        task_id=f"compare_dates_task",
                        python_callable=compare_dates,
                        op_kwargs={
                            "schema_name": f"{schema_name}",
                            "group_name": f"{group_name}",
                        },
                    )

                    submit_s2l2l_zone_databricks_job = DatabricksRunNowOperator(
                        task_id=f"submit_s2l2l_zone_databricks_job",
                        job_name= landing_databricks_job,
                        databricks_conn_id=DATABRICKS_DEV_CONNECTION,
                        #new_cluster=DATABRICKS_CLUSTER,
                        notebook_params={
                          #  "--catalog_name": catalog_name_staging,
                            "database_name": DB_NAME,    #dwh_stg
                            "schema_name": schema_name,  #arotron
                            "group_name": group_name,
                            "tables_to_process": tables_from_redis, #tables for current running group p.x arotron02's tables
                            "business_date": business_date,
                            "ingestion_date": ingestion_date,
                        },
                          #databricks_retry_limits=3,
                          #databricks_retry_delay=random.uniform(10, 20),
                    )

                    group_has_already_been_processed = EmptyOperator(
                        task_id=f"group_has_already_been_processed"
                    )

                    end_group = EmptyOperator(
                        task_id=f"end_group",
                        trigger_rule="one_success",
                    )

                    set_redis_group_last_exec_date_task = PythonOperator(
                       task_id=f"set_redis_last_exec_date_task",
                       python_callable=_set_redis_key,
                       op_kwargs={
                           "redis_key": f"{airflow_env}-LoadToLanding:staging:{schema_name}:{group_name}:lastExecution",
                           "redis_value": f"{{{{ ti.xcom_pull(task_ids='get_date_to_run_task') }}}}",
                       },
                    )

                    get_group_last_execution_date_task >> compare_dates_task

                    compare_dates_task >> group_has_already_been_processed >> end_group

                    (
                        compare_dates_task
                        >> submit_s2l2l_zone_databricks_job
                        >> set_redis_group_last_exec_date_task
                        >> end_group
                    )

                    schema_group_landing_groups.append(schema_group_landing)

                # [END schema_group_landing_task_group]

            (
                schema_sensor
                >> schema_group_landing_groups
                >> end_schema
                #>> end 
                #>> send_finish_notification_task
            )

        # [END schema_landing_and_landed_task_group]
        (
            get_schemata_from_redis_task
            >> schema_landing
        )


            #############################################################################################
            #################################    CURATED         ########################################
            #################################    START           ########################################
            #############################################################################################

        if schema_name == "cti":
            # Process dependent jobs from profits
            redis_key = f"{airflow_env}-LoadToCurated:cti_profits_qca_qc:groupList"
            dependent_curated_tables_to_process = get_redis_string(redis_key)
            dependent_curated_tables_to_process = [
                table for table in dependent_curated_tables_to_process.split(",")
            ]
            with TaskGroup(
                group_id=f"cti_profits_qca_qc_curated"
            ) as cti_profits_qca_qc_curated:
                end_schema = EmptyOperator(task_id="end_schema")
                dependent_curated_jobs = []

                for curated_table in dependent_curated_tables_to_process:
                    with TaskGroup(
                        group_id=f"cti_profits_{curated_table}"
                    ) as cti_profits_table_curated:
                        get_table_schema_task = PythonOperator(
                            task_id=f"get_table_schema_task",
                            python_callable=get_redis_string,
                            op_kwargs={
                                "redis_key": f"{airflow_env}-curated:schema:{'_'.join(curated_table.split('_')[:-1])  if '_' in curated_table else curated_table}",
                            },
                        )


                        submit_curated_zone_databricks_job = DatabricksRunNowOperator(
                            task_id=f"submit_curated_zone_databricks_job",
                            job_name= curated_databricks_job,
                            databricks_conn_id=DATABRICKS_DEV_CONNECTION,
                            #new_cluster=DATABRICKS_CLUSTER,
                            notebook_params={
                            #  "--catalog_name": catalog_name_staging,
                                "curated_zone": catalog_name_curated,
                                "landed_zone": catalog_name_landed,
                                "reference_zone": catalog_name_reference,
                                "curated_table": curated_table,
                                "run_date": f"{{{{ ti.xcom_pull(task_ids='get_business_date_task')[0:4] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[4:6] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[6:8] }}}}",
                                "schema": f"{{{{ ti.xcom_pull(task_ids='cti_profits_qca_qc_curated.cti_profits_{curated_table}.get_table_schema_task') }}}}",
                            },
                            #databricks_retry_limits=3,
                            #databricks_retry_delay=random.uniform(10, 20),
                        )

                        end_table = EmptyOperator(
                            task_id="end_table",
                            trigger_rule="one_success",
                        )

                        (
                            get_table_schema_task
                            >> submit_curated_zone_databricks_job
                            >> end_table
                        )

                        dependent_curated_jobs.append(cti_profits_table_curated)

                dependent_curated_jobs >> end_schema >> end
                
            schema_landing >> cti_profits_qca_qc_curated >> end

                     
        if schema_name == "process":
            # Process dependent jobs from profits
            redis_key = f"{airflow_env}-LoadToCurated:process_profits:groupList"
            dependent_curated_tables_to_process = get_redis_string(redis_key)
            dependent_curated_tables_to_process = [
                table for table in dependent_curated_tables_to_process.split(",")
            ]
            with TaskGroup(
                group_id=f"process_profits_curated"
            ) as process_profits_curated:
                end_schema = EmptyOperator(task_id="end_schema")
                dependent_curated_jobs = []

                for curated_table in dependent_curated_tables_to_process:
                    with TaskGroup(
                        group_id=f"process_profits_{curated_table}"
                    ) as process_profits_table_curated:
                        get_table_schema_task = PythonOperator(
                            task_id=f"get_table_schema_task",
                            python_callable=get_redis_string,
                            op_kwargs={
                                "redis_key": f"{airflow_env}-curated:schema:{'_'.join(curated_table.split('_')[:-1])  if '_' in curated_table else curated_table}",
                            },
                        )

                        submit_curated_zone_databricks_job = DatabricksRunNowOperator(
                            task_id=f"submit_curated_zone_databricks_job",
                            job_name= curated_databricks_job,
                            databricks_conn_id=DATABRICKS_DEV_CONNECTION,
                            #new_cluster=DATABRICKS_CLUSTER,
                            notebook_params={
                            #  "--catalog_name": catalog_name_staging,
                                "curated_zone": catalog_name_curated,
                                "landed_zone": catalog_name_landed,
                                "reference_zone": catalog_name_reference,
                                "curated_table": curated_table,
                                "run_date": f"{{{{ ti.xcom_pull(task_ids='get_business_date_task')[0:4] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[4:6] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[6:8] }}}}",
                                "schema": f"{{{{ ti.xcom_pull(task_ids='process_profits_curated.process_profits_{curated_table}.get_table_schema_task') }}}}",
                            },
                            #databricks_retry_limits=3,
                            #databricks_retry_delay=random.uniform(10, 20),
                        )

                        end_table = EmptyOperator(
                            task_id="end_table",
                            trigger_rule="one_success",
                        )

                        (
                            get_table_schema_task
                            >> submit_curated_zone_databricks_job
                            >> end_table
                        )

                        dependent_curated_jobs.append(process_profits_table_curated)

                dependent_curated_jobs >> end_schema >> end

            schema_landing >> process_profits_curated

        if schema_name == "profits":
            # Qca dependent jobs from profits
            redis_key = f"{airflow_env}-LoadToCurated:qca_profits:groupList"
            dependent_curated_tables_to_process = get_redis_string(redis_key)
            dependent_curated_tables_to_process = [
                table for table in dependent_curated_tables_to_process.split(",")
            ]
            with TaskGroup(group_id=f"qca_profits_curated") as qca_profits_curated:
                end_schema = EmptyOperator(task_id="end_schema")
                dependent_curated_jobs = []

                for curated_table in dependent_curated_tables_to_process:
                    with TaskGroup(
                        group_id=f"qca_profits_{curated_table}"
                    ) as qca_profits_table_curated:
                        get_table_schema_task = PythonOperator(
                            task_id=f"get_table_schema_task",
                            python_callable=get_redis_string,
                            op_kwargs={
                                "redis_key": f"{airflow_env}-curated:schema:{'_'.join(curated_table.split('_')[:-1])  if '_' in curated_table else curated_table}",
                            },
                        )

                        submit_curated_zone_databricks_job = DatabricksRunNowOperator(
                            task_id=f"submit_curated_zone_databricks_job",
                            job_name= curated_databricks_job,
                            databricks_conn_id=DATABRICKS_DEV_CONNECTION,
                            #new_cluster=DATABRICKS_CLUSTER,
                            notebook_params={
                            #  "--catalog_name": catalog_name_staging,
                                "curated_zone": catalog_name_curated,
                                "landed_zone": catalog_name_landed,
                                "reference_zone": catalog_name_reference,
                                "curated_table": curated_table,
                                "run_date": f"{{{{ ti.xcom_pull(task_ids='get_business_date_task')[0:4] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[4:6] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[6:8] }}}}",
                                "schema": f"{{{{ ti.xcom_pull(task_ids='qca_profits_curated.qca_profits_{curated_table}.get_table_schema_task') }}}}",
                            },
                            #databricks_retry_limits=3,
                            #databricks_retry_delay=random.uniform(10, 20),
                        )

                        end_table = EmptyOperator(
                            task_id="end_table",
                            trigger_rule="one_success",
                        )

                        (
                            get_table_schema_task
                            >> submit_curated_zone_databricks_job
                            >> end_table
                        )

                        dependent_curated_jobs.append(qca_profits_table_curated)

                dependent_curated_jobs >> end_schema >> end

            schema_landing >> qca_profits_curated

            # QC dependent jobs from profits
            redis_key = f"{airflow_env}-LoadToCurated:qc_profits:groupList"
            dependent_curated_tables_to_process = get_redis_string(redis_key)
            dependent_curated_tables_to_process = [
                table for table in dependent_curated_tables_to_process.split(",")
            ]
            with TaskGroup(group_id=f"qc_profits_curated") as qc_profits_curated:
                end_schema = EmptyOperator(task_id="end_schema")
                dependent_curated_jobs = []

                for curated_table in dependent_curated_tables_to_process:
                    with TaskGroup(
                        group_id=f"qc_profits_{curated_table}"
                    ) as qc_profits_table_curated:
                        get_table_schema_task = PythonOperator(
                            task_id=f"get_table_schema_task",
                            python_callable=get_redis_string,
                            op_kwargs={
                                "redis_key": f"{airflow_env}-curated:schema:{'_'.join(curated_table.split('_')[:-1])  if '_' in curated_table else curated_table}",
                            },
                        )

                        submit_curated_zone_databricks_job = DatabricksRunNowOperator(
                            task_id=f"submit_curated_zone_databricks_job",
                            job_name= curated_databricks_job,
                            databricks_conn_id=DATABRICKS_DEV_CONNECTION,
                            #new_cluster=DATABRICKS_CLUSTER,
                            notebook_params={
                            #  "--catalog_name": catalog_name_staging,
                                "curated_zone": catalog_name_curated,
                                "landed_zone": catalog_name_landed,
                                "reference_zone": catalog_name_reference,
                                "curated_table": curated_table,
                                "run_date": f"{{{{ ti.xcom_pull(task_ids='get_business_date_task')[0:4] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[4:6] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[6:8] }}}}",
                                "schema": f"{{{{ ti.xcom_pull(task_ids='qc_profits_curated.qc_profits_{curated_table}.get_table_schema_task') }}}}",
                            },
                            #databricks_retry_limits=3,
                            #databricks_retry_delay=random.uniform(10, 20),
                        )

                        end_table = EmptyOperator(
                            task_id="end_table",
                            trigger_rule="one_success",
                        )

                        (
                            get_table_schema_task
                            >> submit_curated_zone_databricks_job
                            >> end_table
                        )

                        dependent_curated_jobs.append(qc_profits_table_curated)

                dependent_curated_jobs >> end_schema >> end

            schema_landing >> qc_profits_curated

            # Cti dependent jobs from profits
            schema_landing >> cti_profits_qca_qc_curated

            # Process dependent jobs from profits
            schema_landing >> process_profits_curated

        if schema_name == "qc":
            schema_landing >> qc_profits_curated

            # Cti dependent jobs from qc
            schema_landing >> cti_profits_qca_qc_curated

        if schema_name == "qca":
            schema_landing >> qca_profits_curated

            # Cti dependent jobs from qca
            schema_landing >> cti_profits_qca_qc_curated

        curated_schemata = get_redis_string(
            f"{airflow_env}-LoadToCurated:curated:schemaList"
        )
        curated_schemata = [cur_schema for cur_schema in curated_schemata.split(",")]

        if schema_name in curated_schemata:
            redis_key = f"{airflow_env}-LoadToCurated:{schema_name}:groupList"
            curated_tables_to_process = get_redis_string(redis_key)
            curated_tables_to_process = [
                table for table in curated_tables_to_process.split(",")
            ]

            with TaskGroup(group_id=f"{schema_name}_curated") as schema_curated:
                end_schema = EmptyOperator(task_id="end_schema")
                curated_jobs = []

                for curated_table in curated_tables_to_process:
                    with TaskGroup(
                        group_id=f"{schema_name}_{curated_table}"
                    ) as schema_table_curated:

                        get_table_schema_task = PythonOperator(
                            task_id=f"get_table_schema_task",
                            python_callable=get_redis_string,
                            op_kwargs={
                                "redis_key": f"{airflow_env}-curated:schema:{'_'.join(curated_table.split('_')[:-1])  if '_' in curated_table else curated_table}",
                            },
                        )

                        submit_curated_zone_databricks_job = DatabricksRunNowOperator(
                            task_id=f"submit_curated_zone_databricks_job",
                            job_name= curated_databricks_job,
                            databricks_conn_id=DATABRICKS_DEV_CONNECTION,
                            #new_cluster=DATABRICKS_CLUSTER,
                            notebook_params={
                            #  "--catalog_name": catalog_name_staging,
                                "curated_zone": catalog_name_curated,
                                "landed_zone": catalog_name_landed,
                                "reference_zone": catalog_name_reference,
                                "curated_table": curated_table,
                                "run_date": f"{{{{ ti.xcom_pull(task_ids='get_business_date_task')[0:4] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[4:6] }}}}-{{{{ ti.xcom_pull(task_ids='get_business_date_task')[6:8] }}}}",
                                "schema": f"{{{{ ti.xcom_pull(task_ids='{schema_name}_curated.{schema_name}_{curated_table}.get_table_schema_task') }}}}",
                            },
                            #databricks_retry_limits=3,
                            #databricks_retry_delay=random.uniform(10, 20),
                        )
                        end_table = EmptyOperator(
                            task_id="end_table",
                            trigger_rule="one_success",
                        )

                        (
                            get_table_schema_task
                            >> submit_curated_zone_databricks_job
                            >> end_table
                        )

                        curated_jobs.append(schema_table_curated)

                curated_jobs >> end_schema
        
            schema_landing >> schema_curated >> end
        else:
            schema_landing >> end
                        
                #############################################################################################
                #################################    CURATED         ########################################
                #################################    END             ########################################
                #############################################################################################


    (
        start
        >> get_date_to_run_task
        >> get_business_date_task
        >> staging_db
        >> dwh_pld_sensor  # new
        >> get_schemata_from_redis_task
    )

    dwh_pld_sensor >> send_start_notification_task

    send_failure_notification_task = SnsPublishOperator(
        task_id=f"send_failure_notification",
        target_arn=airflow_status_arn,
        message=f"The Dag S2L2L2C failed. Please review dag for more details",
        subject=f"*** Status : Failure *** | Dag S2L2L2C | {airflow_env_extended} Airflow",
        trigger_rule="one_failed",  # Trigger the task if any upstream task fails
    )

    end >> send_finish_notification_task
    send_finish_notification_task
