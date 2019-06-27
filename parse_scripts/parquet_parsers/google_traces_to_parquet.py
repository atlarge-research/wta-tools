import json
import mmh3
import os
import re
import sys
import time
from functools import reduce

import findspark
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from objects.task import Task
from objects.resource import Resource
from objects.resource_state import ResourceState
from objects.task_state import TaskState
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = 'Usage: python(3) ./google_traces_to_parquet_with_spark.py path_to_root_dir'
NAME = 'Google'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', NAME)


def parse(path_to_dir):
    if 'DAS5' in os.environ:  # If we want to execute it on the DAS-5 super computer
        print("We are on DAS5, {0} is master.".format(os.environ['HOSTNAME'] + ".ib.cluster"))
        spark = SparkSession.builder \
            .master("spark://" + os.environ['HOSTNAME'] + ".ib.cluster:7077") \
            .appName("WTA parser") \
            .config("spark.executor.memory", "28G") \
            .config("spark.executor.cores", "8") \
            .config("spark.executor.instances", "10") \
            .config("spark.driver.memory", "40G") \
            .getOrCreate()
    else:
        findspark.init(spark_home="<path_to_spark>")
        spark = SparkSession.builder \
            .master("local[8]") \
            .appName("WTA parser") \
            .config("spark.executor.memory", "20G") \
            .config("spark.driver.memory", "8G") \
            .getOrCreate()

    # Convert times which are in microseconds and do not fit in a long to milliseconds
    convert_micro_to_milliseconds = F.udf(lambda x: x / 1000)

    if not os.path.exists(os.path.join(TARGET_DIR, TaskState.output_path())):
        print("######\n Start parsing TaskState\n ######")
        task_usage_df = spark.read.format('com.databricks.spark.csv').options(mode="FAILFAST", inferschema="true").load(
            os.path.join(path_to_dir, 'task_usage', '*.csv'))
        # task_usage_df = spark.read.format('com.databricks.spark.csv').options(mode="FAILFAST", inferschema="true").load(
        #     'fake_task_usage.csv')
        oldColumns = task_usage_df.schema.names
        newColumns = ["ts_start",
                      "ts_end",
                      "workflow_id",
                      "id",
                      "resource_id",
                      "cpu_rate",
                      "memory_consumption",
                      "assigned_memory_usage",
                      "unmapped_page_cache",
                      "total_page_cache",
                      "max_memory_usage",
                      "mean_disk_io_time",
                      "mean_local_disk_space_usage",
                      "max_cpu_rate",
                      "max_disk_io_time",
                      "cycles_per_instruction",
                      "memory_accesses_per_instruction",
                      "sample_portion",
                      "aggregation_type",
                      "sampled_cpu_usage", ]

        task_usage_df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]),
                               range(len(oldColumns)), task_usage_df)

        # Drop columns with too low level details
        task_usage_df = task_usage_df.drop('memory_accesses_per_instruction')
        task_usage_df = task_usage_df.drop('cycles_per_instruction')
        task_usage_df = task_usage_df.drop('unmapped_page_cache')
        task_usage_df = task_usage_df.drop('total_page_cache')

        # Conver the timestamps from micro to milliseconds and cast them to long.
        task_usage_df = task_usage_df.withColumn('ts_start', convert_micro_to_milliseconds(F.col('ts_start')))
        task_usage_df = task_usage_df.withColumn('ts_start', F.col('ts_start').cast(T.LongType()))
        task_usage_df = task_usage_df.withColumn('ts_end', convert_micro_to_milliseconds(F.col('ts_end')))
        task_usage_df = task_usage_df.withColumn('ts_end', F.col('ts_end').cast(T.LongType()))

        # Some fields have weird symbols in them, clean those.
        truncate_at_lt_symbol_udf = F.udf(lambda x: re.sub('[^0-9.eE\-+]', '', str(x)) if x is not None else x)
        task_usage_df = task_usage_df.withColumn('workflow_id', truncate_at_lt_symbol_udf(F.col('workflow_id')))
        task_usage_df = task_usage_df.withColumn('max_cpu_rate', truncate_at_lt_symbol_udf(F.col('max_cpu_rate')))

        # Now that the columns have been sanitized, cast them to the right type
        task_usage_df = task_usage_df.withColumn('workflow_id', F.col('workflow_id').cast(T.LongType()))
        task_usage_df = task_usage_df.withColumn('max_cpu_rate', F.col('max_cpu_rate').cast(T.FloatType()))

        task_usage_df.write.parquet(os.path.join(TARGET_DIR, TaskState.output_path()), mode="overwrite",
                                    compression="snappy")
        print("######\n Done parsing TaskState\n ######")

    if not os.path.exists(os.path.join(TARGET_DIR, Task.output_path())):

        if 'task_usage_df' not in locals():
            task_usage_df = spark.read.parquet(os.path.join(TARGET_DIR, TaskState.output_path()))

        print("######\n Start parsing Tasks\n ######")
        task_df = spark.read.format('com.databricks.spark.csv').options(inferschema="true", mode="FAILFAST",
                                                                        parserLib="univocity").load(
            os.path.join(path_to_dir, 'task_events', '*.csv'))

        oldColumns = task_df.schema.names
        newColumns = ["ts_submit",
                      "missing_info",
                      "workflow_id",
                      "id",
                      "resource_id",
                      "event_type",
                      "user_id",
                      "scheduler",
                      "nfrs",
                      "resources_requested",
                      "memory_requested",
                      "disk_space_request",
                      "machine_restrictions", ]

        task_df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]),
                         range(len(oldColumns)), task_df)

        task_df = task_df.withColumn('ts_submit', convert_micro_to_milliseconds(F.col('ts_submit')))
        task_df = task_df.withColumn('ts_submit', F.col('ts_submit').cast(T.LongType()))

        # Filter tasks that never reached completion
        task_df.createOrReplaceTempView("task_table")
        task_df = spark.sql("""WITH filtered_tasks AS (
        SELECT DISTINCT t1.workflow_id AS workflow_id, t1.id AS id
            FROM task_table t1
            WHERE t1.event_type IN(0, 1, 4)
            group by t1.workflow_id, t1.id
            having count(distinct event_type) = 3
        )
    SELECT t.*
    FROM task_table t INNER JOIN filtered_tasks f
    ON t.id = f.id AND t.workflow_id = f.workflow_id""")

        task_aggregation_structtype = T.StructType([
            T.StructField("workflow_id", T.LongType(), True),
            T.StructField("id", T.LongType(), True),
            T.StructField("type", T.StringType(), True),
            T.StructField("ts_submit", T.LongType(), True),
            T.StructField("submission_site", T.LongType(), True),
            T.StructField("runtime", T.LongType(), True),
            T.StructField("resource_type", T.StringType(), True),
            T.StructField("resource_amount_requested", T.DoubleType(), True),
            T.StructField("parents", T.ArrayType(T.LongType()), True),
            T.StructField("children", T.ArrayType(T.LongType()), True),
            T.StructField("user_id", T.LongType(), True),
            T.StructField("group_id", T.LongType(), True),
            T.StructField("nfrs", T.StringType(), True),
            T.StructField("wait_time", T.LongType(), True),
            T.StructField("params", T.StringType(), True),
            T.StructField("memory_requested", T.DoubleType(), True),
            T.StructField("network_io_time", T.DoubleType(), True),
            T.StructField("disk_space_requested", T.DoubleType(), True),
            T.StructField("energy_consumption", T.DoubleType(), True),
            T.StructField("resource_used", T.StringType(), True),
        ])

        # Compute based on the event type
        @F.pandas_udf(returnType=task_aggregation_structtype, functionType=F.PandasUDFType.GROUPED_MAP)
        def compute_aggregated_task_usage_metrics(df):
            def get_first_non_value_in_column(column_name):
                s = df[column_name]
                idx = s.first_valid_index()
                return s.loc[idx] if idx is not None else None

            task_workflow_id = get_first_non_value_in_column("workflow_id")
            task_id = get_first_non_value_in_column("id")

            task_submit_time = df[df['event_type'] == 0]['ts_submit'].min(skipna=True)
            task_start_time = df[df['event_type'] == 1]['ts_submit'].min(skipna=True)
            task_finish_time = df[df['event_type'] == 4]['ts_submit'].max(skipna=True)

            if None in [task_start_time, task_submit_time, task_finish_time]:
                return None

            task_resource_request = df['resources_requested'].max(skipna=True)
            task_memory_request = df['memory_requested'].max(skipna=True)
            task_priority = df['nfrs'].max(skipna=True)
            task_disk_space_requested = df['disk_space_request'].max(skipna=True)

            task_machine_id_list = df.resource_id.unique()

            task_waittime = int(task_start_time) - int(task_submit_time)
            task_runtime = int(task_finish_time) - int(task_start_time)

            def default(o):
                if isinstance(o, np.int64):
                    return int(o)

            data_dict = {
                "workflow_id": task_workflow_id,
                "id": task_id,
                "type": "",  # Unknown
                "ts_submit": task_submit_time,
                "submission_site": -1,  # Unknown
                "runtime": task_runtime,
                "resource_type": "core",  # Fields are called CPU, but they are core count (see Google documentation)
                "resource_amount_requested": task_resource_request,
                "parents": [],
                "children": [],
                "user_id": mmh3.hash64(get_first_non_value_in_column("user_id"))[0],
                "group_id": -1,
                "nfrs": json.dumps({"priority": task_priority}, default=default),
                "wait_time": task_waittime,
                "params": "{}",
                "memory_requested": task_memory_request,
                "network_io_time": -1,  # Unknown
                "disk_space_requested": task_disk_space_requested,
                "energy_consumption": -1,  # Unknown
                "resource_used": json.dumps(task_machine_id_list, default=default),
            }

            return pd.DataFrame(data_dict, index=[0])

        task_df = task_df.groupBy(["workflow_id", "id"]).apply(compute_aggregated_task_usage_metrics)
        task_df.explain(True)

        # Now add disk IO time - This cannot be done in the previous Pandas UDF function as
        # accessing another dataframe in the apply function is not allowed
        disk_io_structtype = T.StructType([
            T.StructField("workflow_id", T.LongType(), True),
            T.StructField("id", T.LongType(), True),
            T.StructField("disk_io_time", T.DoubleType(), True),
        ])

        @F.pandas_udf(returnType=disk_io_structtype, functionType=F.PandasUDFType.GROUPED_MAP)
        def compute_disk_io_time(df):
            def get_first_non_value_in_column(column_name):
                s = df[column_name]
                idx = s.first_valid_index()
                return s.loc[idx] if idx is not None else None

            task_workflow_id = get_first_non_value_in_column("workflow_id")
            task_id = get_first_non_value_in_column("id")

            disk_io_time = ((df['ts_end'] - df['ts_start']) * df['mean_disk_io_time']).sum(skipna=True) / 1000
            data_dict = {
                "workflow_id": task_workflow_id,
                "id": task_id,
                "disk_io_time": disk_io_time
            }

            return pd.DataFrame(data_dict, index=[0])

        disk_io_df = task_usage_df.select(['workflow_id', 'id', 'mean_disk_io_time', 'ts_end', 'ts_start']).groupBy(
            ["workflow_id", "id"]).apply(compute_disk_io_time)
        disk_io_df.explain(True)

        join_condition = (task_df.workflow_id == disk_io_df.workflow_id) & (task_df.id == disk_io_df.id)
        task_df = task_df.join(disk_io_df, ["workflow_id", "id"])

        task_df.write.parquet(os.path.join(TARGET_DIR, Task.output_path()), mode="overwrite", compression="snappy")
        print("######\n Done parsing Tasks\n ######")
    else:
        task_df = spark.read.parquet(os.path.join(TARGET_DIR, Task.output_path()))

    if not os.path.exists(os.path.join(TARGET_DIR, Resource.output_path())):
        print("######\n Start parsing Resource\n ######")
        # Parse the machine information in the traces, these should match with the resource_ids in task_usage
        resources_structtype = T.StructType([  # Using StringTypes as we drop those columns
            T.StructField("time", T.StringType(), False),
            T.StructField("id", T.LongType(), False),
            T.StructField("attribute_name", T.StringType(), False),
            T.StructField("attribute_value", T.StringType(), False),
            T.StructField("attribute_deleted", T.StringType(), False),
        ])

        resource_df = spark.read.format('com.databricks.spark.csv').schema(resources_structtype).options(
            mode="FAILFAST").load(os.path.join(path_to_dir, 'machine_attributes', '*.csv'))

        resource_df = resource_df.select(["id"])  # Only keep the ID, the rest we do not need.

        # Since the information in the traces is completely opaque, we use the educated guess from Amvrosiadis et al.
        # in their ATC 2018 article.
        resource_df = resource_df.withColumn('type', F.lit("core"))
        resource_df = resource_df.withColumn('num_resources', F.lit(8))
        resource_df = resource_df.withColumn('proc_model', F.lit("AMD Opteron Barcelona"))
        resource_df = resource_df.withColumn('memory', F.lit(-1))
        resource_df = resource_df.withColumn('disk_space', F.lit(-1))
        resource_df = resource_df.withColumn('network', F.lit(-1))
        resource_df = resource_df.withColumn('os', F.lit(""))
        resource_df = resource_df.withColumn('details', F.lit("{}"))

        # Write the resource_df to the specified location
        resource_df.write.parquet(os.path.join(TARGET_DIR, Resource.output_path()), mode="overwrite",
                                  compression="snappy")
        print("######\n Done parsing Resource\n ######")

    if not os.path.exists(os.path.join(TARGET_DIR, ResourceState.output_path())):
        print("######\n Start parsing ResourceState\n ######")
        resource_events_structtype = T.StructType([
            T.StructField("timestamp", T.DecimalType(20, 0), False),
            T.StructField("machine_id", T.LongType(), False),
            T.StructField("event_type", T.IntegerType(), False),
            T.StructField("platform_id", T.StringType(), False),
            T.StructField("available_resources", T.FloatType(), False),
            T.StructField("available_memory", T.FloatType(), False),
        ])

        resource_event_df = spark.read.format('com.databricks.spark.csv').schema(resource_events_structtype).options(
            mode="FAILFAST").load(os.path.join(path_to_dir, 'machine_events', '*.csv'))

        resource_event_df = resource_event_df.withColumn('timestamp', convert_micro_to_milliseconds(F.col('timestamp')))
        resource_event_df = resource_event_df.withColumn('timestamp', F.col('timestamp').cast(T.LongType()))

        resource_event_df = resource_event_df.withColumn('available_disk_space', F.lit(-1))
        resource_event_df = resource_event_df.withColumn('available_disk_io_bandwidth', F.lit(-1))
        resource_event_df = resource_event_df.withColumn('available_network_bandwidth', F.lit(-1))
        resource_event_df = resource_event_df.withColumn('average_load_1_minute', F.lit(-1))
        resource_event_df = resource_event_df.withColumn('average_load_5_minute', F.lit(-1))
        resource_event_df = resource_event_df.withColumn('average_load_15_minute', F.lit(-1))

        # Write the resource_df to the specified location
        resource_event_df.write.parquet(os.path.join(TARGET_DIR, ResourceState.output_path()), mode="overwrite",
                                        compression="snappy")
        print("######\n Done parsing ResourceState\n ######")

    if not os.path.exists(os.path.join(TARGET_DIR, Workflow.output_path())):
        print("######\n Start parsing Workflows\n ######")
        workflow_structype = T.StructType([
            T.StructField("id", T.LongType(), False),
            T.StructField("ts_submit", T.LongType(), False),
            T.StructField("task_count", T.IntegerType(), False),
            T.StructField("critical_path_length", T.LongType(), False),
            T.StructField("critical_path_task_count", T.IntegerType(), False),
            T.StructField("approx_max_concurrent_tasks", T.IntegerType(), False),
            T.StructField("nfrs", T.StringType(), False),
            T.StructField("scheduler", T.StringType(), False),
            T.StructField("total_resources", T.DoubleType(), False),
            T.StructField("total_memory_usage", T.DoubleType(), False),
            T.StructField("total_network_usage", T.LongType(), False),
            T.StructField("total_disk_space_usage", T.LongType(), False),
            T.StructField("total_energy_consumption", T.LongType(), False),
        ])

        @F.pandas_udf(returnType=workflow_structype, functionType=F.PandasUDFType.GROUPED_MAP)
        def compute_workflow_stats(df):
            id = df['workflow_id'].iloc[0]
            ts_submit = df['ts_submit'].min()
            task_count = len(df)
            critical_path_length = -1  # We do not know the task dependencies, so -1
            critical_path_task_count = -1
            approx_max_concurrent_tasks = -1
            nfrs = "{}"
            scheduler = ""
            total_resources = df['resource_amount_requested'].sum()  # TODO or assigned?
            total_memory_usage = df['memory_requested'].sum()  # TODO or consumption, or assigned?
            total_network_usage = -1
            total_disk_space_usage = -1
            total_energy_consumption = -1

            data_dict = {
                "id": id, "ts_submit": ts_submit, 'task_count': task_count,
                'critical_path_length': critical_path_length,
                'critical_path_task_count': critical_path_task_count,
                'approx_max_concurrent_tasks': approx_max_concurrent_tasks, 'nfrs': nfrs, 'scheduler': scheduler,
                'total_resources': total_resources, 'total_memory_usage': total_memory_usage,
                'total_network_usage': total_network_usage, 'total_disk_space_usage': total_disk_space_usage,
                'total_energy_consumption': total_energy_consumption
            }

            return pd.DataFrame(data_dict, index=[0])

        # Create and write the workflow dataframe
        workflow_df = task_df.groupBy('workflow_id').apply(compute_workflow_stats)

        workflow_df.write.parquet(os.path.join(TARGET_DIR, Workflow.output_path()), mode="overwrite",
                                  compression="snappy")
        print("######\n Done parsing Workflows\n ######")

    print("######\n Start parsing Workload\n ######")
    json_dict = Workload.get_json_dict_from_spark_task_dataframe(task_df,
                                                                 domain="Industrial",
                                                                 start_date="2011-05-01",
                                                                 end_date="2011-05-30",
                                                                 authors=["Google"])

    os.makedirs(os.path.join(TARGET_DIR, Workload.output_path()), exist_ok=True)
    with open(os.path.join(TARGET_DIR, Workload.output_path(), "generic_information.json"), "w") as file:
        # Need this on 32-bit python.
        def default(o):
            if isinstance(o, np.int64):
                return int(o)

        file.write(json.dumps(json_dict, default=default))
    print("######\n Done parsing Workload\n ######")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    start = time.perf_counter()
    parse(sys.argv[1])
    print(time.perf_counter() - start)
