import datetime
import json
import mmh3
import os
import sys
import time
from uuid import uuid4

import findspark
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from objects.task import Task
from objects.task_state import TaskState
from objects.workflow import Workflow
from objects.resource import Resource
from objects.workload import Workload

USAGE = 'Usage: python(3) ./two_sigma_traces_to_parquet_with_spark.py path_to_dir'
NAME = 'Two_Sigma'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', NAME)


def parse(path_to_dir):
    global TARGET_DIR
    TARGET_DIR = os.path.join(TARGET_DIR, os.path.split(path_to_dir)[1])

    if 'DAS5' in os.environ:  # If we want to execute it on the DAS-5 super computer
        print("We are on DAS5, {0} is master.".format(os.environ['HOSTNAME'] + ".ib.cluster"))
        spark = SparkSession.builder \
            .master("spark://" + os.environ['HOSTNAME'] + ".ib.cluster:7077") \
            .appName("WTA parser") \
            .config("spark.executor.memory", "28G") \
            .config("spark.executor.cores", "8") \
            .config("spark.executor.instances", "10") \
            .config("spark.driver.memory", "40G") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .getOrCreate()
    else:
        findspark.init(spark_home="<path to spark>")
        spark = SparkSession.builder \
            .master("local[8]") \
            .appName("WTA parser") \
            .config("spark.executor.memory", "20G") \
            .config("spark.driver.memory", "8G") \
            .getOrCreate()

    if not os.path.exists(os.path.join(TARGET_DIR, Task.output_path())):
        print("######\nStart parsing Tasks\n######")
        task_df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(
            os.path.join(path_to_dir, '*.csv.processed'))

        # Drop the pref table, saving memory and filter out unsuccessful jobs as their information is not reliable
        task_df = task_df.drop('pref').filter(task_df.status == ":instance.status/success").drop('status').cache()

        @F.pandas_udf(T.LongType(), F.PandasUDFType.SCALAR)
        def sub_two_datetimes(s1, s2):
            arr = []
            for i in s1.keys():
                d1 = datetime.datetime.strptime(s1[i], '%a %b %d %H:%M:%S %Z %Y')
                d2 = datetime.datetime.strptime(s2[i], '%a %b %d %H:%M:%S %Z %Y')
                arr.append(int((d2 - d1).total_seconds() * 1000))

            return pd.Series(arr)

        task_df = task_df \
            .withColumn('wait_time', sub_two_datetimes(F.col('submit-time'), F.col('start-time'))) \
            .withColumn('runtime', sub_two_datetimes(F.col('start-time'), F.col('end-time')))

        @F.pandas_udf(T.LongType(), F.PandasUDFType.SCALAR)
        def date_time_to_unix(series):
            arr = []
            epoch = datetime.datetime.utcfromtimestamp(0)
            for i in series.keys():
                arr.append(
                    np.int64(
                        (datetime.datetime.strptime(series[i], '%a %b %d %H:%M:%S %Z %Y') - epoch).total_seconds() * 1000)
                )

            return pd.Series(arr)

        task_df = task_df.withColumn('submit-time', date_time_to_unix(F.col('submit-time'))).withColumnRenamed(
            'submit-time', "ts_submit").drop(
            'start-time').drop('end-time').cache()

        min_ts = task_df.agg({"ts_submit": "min"}).collect()[0][0]
        task_df = task_df.withColumn('ts_submit', F.col('ts_submit') - F.lit(min_ts))

        @F.pandas_udf(T.DoubleType(), F.PandasUDFType.SCALAR)
        def convert_to_kb(v):
            return v * 1024

        task_df = task_df.withColumn('memory', convert_to_kb(task_df.memory)).withColumnRenamed("memory",
                                                                                                "memory_consumption")

        @F.pandas_udf(T.IntegerType(), F.PandasUDFType.SCALAR)
        def string_to_int(v):
            arr = []
            for i in v.keys():
                arr.append(mmh3.hash(v[i], signed=True))

            return pd.Series(arr)

        @F.pandas_udf(T.LongType(), F.PandasUDFType.SCALAR)
        def string_to_long(v):
            arr = []
            for i in v.keys():
                arr.append(mmh3.hash64(v[i], signed=True)[0])

            return pd.Series(arr)

        @F.pandas_udf(T.LongType(), F.PandasUDFType.SCALAR)
        def assign_workflow_ids(v):
            arr = []
            for i in v.keys():
                if v[i]:
                    arr.append(mmh3.hash64(v[i], signed=True)[0])
                else:
                    arr.append(mmh3.hash64(uuid4().bytes, signed=True)[0])  # Assign a UUID, collision chance is negligible.

            return pd.Series(arr)

        task_df = task_df.withColumn('user', string_to_int(task_df.user)).withColumnRenamed("user", "user_id")
        task_df = task_df.withColumn('job-uuid', string_to_long(F.col('job-uuid'))).withColumnRenamed('job-uuid', 'task_id')

        type_udf = F.udf(lambda x: "Independent" if x is None else "Composite", T.StringType())
        task_df = task_df.withColumn('type', type_udf(task_df.simset))

        task_df = task_df.withColumn('simset', assign_workflow_ids(F.col('simset'))).withColumnRenamed('simset',
                                                                                                       "workflow_id")
        task_df = task_df.withColumnRenamed('cpu', 'resource_amount_requested')

        task_df = task_df.withColumnRenamed('instance', 'resource_used')

        # Set the static items that are not present in the trace
        task_df = task_df.withColumn('submission_site', F.lit(0))
        task_df = task_df.withColumn('parents', F.array().cast(T.ArrayType(T.LongType())))
        task_df = task_df.withColumn('children', F.array().cast(T.ArrayType(T.LongType())))
        task_df = task_df.withColumn('group_id', F.lit(0))
        task_df = task_df.withColumn('nfrs', F.lit("{}"))
        task_df = task_df.withColumn('params', F.lit("{}"))
        task_df = task_df.withColumn('memory_requested', F.lit(-1))
        task_df = task_df.withColumn('network_io_time', F.lit(-1))
        task_df = task_df.withColumn('disk_io_time', F.lit(-1))
        task_df = task_df.withColumn('disk_space_requested', F.lit(-1))
        task_df = task_df.withColumn('energy_consumption', F.lit(-1))

        os.makedirs(os.path.join(TARGET_DIR, Task.output_path()), exist_ok=True)
        task_df.write.parquet(os.path.join(TARGET_DIR, Task.output_path()), mode="overwrite", compression="snappy")
        print("######\nDone parsing Tasks\n######")

    if not os.path.exists(os.path.join(TARGET_DIR, TaskState.output_path())):
        print("######\nStart parsing TaskState\n######")

        if 'task_df' not in locals():
            task_df = spark.read.parquet(os.path.join(TARGET_DIR, Task.output_path()))

        task_state_structtype = T.StructType([
            T.StructField("ts_start", T.LongType(), False),
            T.StructField("ts_end", T.LongType(), False),
            T.StructField("workflow_id", T.LongType(), False),
            T.StructField("task_id", T.LongType(), False),
            T.StructField("resource_id", T.LongType(), False),
            T.StructField("cpu_rate", T.DoubleType(), False),
            T.StructField("canonical_memory_usage", T.DoubleType(), False),
            T.StructField("assigned_memory", T.DoubleType(), False),
            T.StructField("minimum_memory_usage", T.DoubleType(), False),
            T.StructField("maximum_memory_usage", T.DoubleType(), False),
            T.StructField("disk_io_time", T.DoubleType(), False),
            T.StructField("maximum_disk_bandwidth", T.DoubleType(), False),
            T.StructField("local_disk_space_usage", T.DoubleType(), False),
            T.StructField("maximum_cpu_rate", T.DoubleType(), False),
            T.StructField("maximum_disk_io_time", T.DoubleType(), False),
            T.StructField("sample_rate", T.DoubleType(), False),
            T.StructField("sample_portion", T.DoubleType(), False),
            T.StructField("sampled_cpu_usage", T.DoubleType(), False),
            T.StructField("network_io_time", T.DoubleType(), False),
            T.StructField("maximum_network_bandwidth", T.DoubleType(), False),
        ])

        @F.pandas_udf(returnType=task_state_structtype, functionType=F.PandasUDFType.GROUPED_MAP)
        def compute_task_states(df):
            workflow_id = df['workflow_id'].iloc[0]
            task_id = df['task_id'].iloc[0]
            ts_start = df['ts_submit'].min()
            ts_end = ts_start + df['runtime'].max()
            resource_id = df['resource_used'].iloc[0]
            cpu_rate = -1
            canonical_memory_usage = df['memory_consumption'].mean()
            assigned_memory = -1
            minimum_memory_usage = df['memory_consumption'].min()
            maximum_memory_usage = df['memory_consumption'].max()
            disk_io_time = -1
            maximum_disk_bandwidth = -1
            local_disk_space_usage = -1
            maximum_cpu_rate = -1
            maximum_disk_io_time = -1
            sample_rate = -1
            sample_portion = -1
            sampled_cpu_usage = -1
            network_io_time = -1
            maximum_network_bandwidth = -1

            data_dict = {
                "ts_start": ts_start,
                "ts_end": ts_end,
                "workflow_id": workflow_id,
                "task_id": task_id,
                "resource_id": resource_id,
                "cpu_rate": cpu_rate,
                "canonical_memory_usage": canonical_memory_usage,
                "assigned_memory": assigned_memory,
                "minimum_memory_usage": minimum_memory_usage,
                "maximum_memory_usage": maximum_memory_usage,
                "disk_io_time": disk_io_time,
                "maximum_disk_bandwidth": maximum_disk_bandwidth,
                "local_disk_space_usage": local_disk_space_usage,
                "maximum_cpu_rate": maximum_cpu_rate,
                "maximum_disk_io_time": maximum_disk_io_time,
                "sample_rate": sample_rate,
                "sample_portion": sample_portion,
                "sampled_cpu_usage": sampled_cpu_usage,
                "network_io_time": network_io_time,
                "maximum_network_bandwidth": maximum_network_bandwidth,
            }

            return pd.DataFrame(data_dict, index=[0])

        task_state_df = task_df.groupBy(['workflow_id', 'task_id']).apply(compute_task_states)
        os.makedirs(os.path.join(TARGET_DIR, TaskState.output_path()), exist_ok=True)
        task_state_df.write.parquet(os.path.join(TARGET_DIR, TaskState.output_path()), mode="overwrite",
                                    compression="snappy")
        print("######\nDone parsing TaskState\n######")


    if not os.path.exists(os.path.join(TARGET_DIR, Resource.output_path())):
        print("######\nStart parsing Resources\n######")

        if 'task_df' not in locals():
            task_df = spark.read.parquet(os.path.join(TARGET_DIR, Task.output_path()))

        resource_id_column = [i.resource_used for i in task_df.select('resource_used').distinct().collect()]

        resources = []
        for resource_id in resource_id_column:
            resources.append(Resource(resource_id, 'Cluster Node', 24, '', 256, -1, -1, '').get_parquet_dict())

        resource_df = pd.DataFrame(resources)
        os.makedirs(os.path.join(TARGET_DIR, Resource.output_path()), exist_ok=True)
        resource_df.to_parquet(os.path.join(TARGET_DIR, Resource.output_path(), 'part.0.parquet'), engine="pyarrow")
        print("######\nDone parsing Resources\n######")

    if not os.path.exists(os.path.join(TARGET_DIR, Workflow.output_path())):
        print("######\nStart parsing Workflows\n######")

        if 'task_df' not in locals():
            task_df = spark.read.parquet(os.path.join(TARGET_DIR, Task.output_path()))

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
            critical_path_length = -1
            critical_path_task_count = -1
            approx_max_concurrent_tasks = -1
            nfrs = "{}"
            scheduler = "Cook"
            total_resources = df['resource_amount_requested'].sum()
            total_memory_usage = df['memory_consumption'].sum()
            total_network_usage = -1
            total_disk_space_usage = -1
            total_energy_consumption = -1

            data_dict = {
                "id": id, "ts_submit": ts_submit, 'task_count': task_count, 'critical_path_length': critical_path_length,
                'critical_path_task_count': critical_path_task_count,
                'approx_max_concurrent_tasks': approx_max_concurrent_tasks,
                'nfrs': nfrs, 'scheduler': scheduler, 'total_resources': total_resources,
                'total_memory_usage': total_memory_usage,
                'total_network_usage': total_network_usage, 'total_disk_space_usage': total_disk_space_usage,
                'total_energy_consumption': total_energy_consumption
            }

            return pd.DataFrame(data_dict, index=[0])

        workflow_df = task_df.groupBy('workflow_id').apply(compute_workflow_stats)
        workflow_df.explain(True)
        workflow_df.write.parquet(os.path.join(TARGET_DIR, Workflow.output_path()), mode="overwrite", compression="snappy")
        print("######\nDone parsing Workflows\n######")

    print("######\nStart parsing Ẁorkload\n######")
    pandas_task_df = pd.read_parquet(os.path.join(TARGET_DIR, Task.output_path()),
                                     engine="pyarrow")
    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(pandas_task_df,
                                                                  domain="Industrial",
                                                                  start_date=None,
                                                                  end_date=None,
                                                                  authors=["Two Sigma"])

    os.makedirs(os.path.join(TARGET_DIR, Workload.output_path()), exist_ok=True)

    with open(os.path.join(TARGET_DIR, Workload.output_path(), "generic_information.json"), "w") as file:
        # Need this on 32-bit python.
        def default(o):
            if isinstance(o, np.int64):
                return int(o)

        file.write(json.dumps(json_dict, default=default))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    start = time.perf_counter()
    parse(sys.argv[1])
    print(time.perf_counter() - start)
