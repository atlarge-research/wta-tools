import json
import math
import mmh3
import os
import sys
import time

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import toposort
from pyspark.sql import Row as SparkRow
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *

from objects.resource import Resource
from objects.task import Task
from objects.task_state import TaskState
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = "Usage: python(3) ./alibaba2018_to_parquet.py path_to_dir"
NAME = "Alibaba 2018"
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), "output_parquet", NAME)


def parse(path_to_dir):
    global TARGET_DIR
    TARGET_DIR = os.path.join(TARGET_DIR, os.path.split(path_to_dir)[-1])

    if "DAS5" in os.environ:  # If we want to execute it on the DAS-5 super computer
        print("We are on DAS5, {0} is master.".format(os.environ["HOSTNAME"] + ".ib.cluster"))
        spark = SparkSession.builder \
            .master("spark://" + os.environ['HOSTNAME'] + ".ib.cluster:7077") \
            .appName("WTA parser") \
            .config("spark.executor.memory", "28G") \
            .config("spark.executor.cores", "8") \
            .config("spark.executor.instances", "10") \
            .config("spark.driver.memory", "256G") \
            .config("spark.driver.maxResultSize", "40G") \
            .config("spark.network.timeout", "100000s") \
            .config("spark.rpc.askTimeout", "100000s") \
            .config("spark.default.parallelism", "2000") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .config("spark.cleaner.periodicGC.interval", "5s") \
            .getOrCreate()
    else:
        import findspark
        findspark.init("<path_to_spark>")
        spark = SparkSession.builder \
            .master("local[4]") \
            .appName("WTA parser") \
            .config("spark.executor.memory", "2G") \
            .config("spark.driver.memory", "2G") \
            .getOrCreate()

    machine_meta = spark.read.csv(os.path.join(path_to_dir, "machine_meta.csv"), schema=StructType([
        StructField("machine_id", StringType(), True),
        StructField("time_stamp", LongType(), True),
        StructField("failure_domain_1", LongType(), True),
        StructField("failure_domain_2", StringType(), True),
        StructField("cpu_num", LongType(), True),
        StructField("mem_size", LongType(), True),
        StructField("status", StringType(), True)
    ]))

    machine_usage = spark.read.csv(os.path.join(path_to_dir, "machine_usage.csv"), schema=StructType([
        StructField("machine_id", StringType(), True),
        StructField("time_stamp", DoubleType(), True),
        StructField("cpu_util_percent", LongType(), True),
        StructField("mem_util_percent", LongType(), True),
        StructField("mem_gps", DoubleType(), True),
        StructField("mkpi", LongType(), True),
        StructField("net_in", DoubleType(), True),
        StructField("net_out", DoubleType(), True),
        StructField("disk_io_percent", DoubleType(), True)
    ]))

    container_meta = spark.read.csv(os.path.join(path_to_dir, "container_meta.csv"), schema=StructType([
        StructField("container_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("time_stamp", LongType(), True),
        StructField("app_du", StringType(), True),
        StructField("status", StringType(), True),
        StructField("cpu_request", LongType(), True),
        StructField("cpu_limit", LongType(), True),
        StructField("mem_size", DoubleType(), True)
    ]))

    container_usage = spark.read.csv(os.path.join(path_to_dir, "container_usage.csv"), schema=StructType([
        StructField("container_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("time_stamp", DoubleType(), True),
        StructField("cpu_util_percent", LongType(), True),
        StructField("mem_util_percent", LongType(), True),
        StructField("cpi", DoubleType(), True),
        StructField("mem_gps", DoubleType(), True),
        StructField("mpki", LongType(), True),
        StructField("net_in", DoubleType(), True),
        StructField("net_out", DoubleType(), True),
        StructField("disk_io_percent", DoubleType(), True)
    ]))

    batch_task = spark.read.csv(os.path.join(path_to_dir, "batch_task.csv"), schema=StructType([
        StructField("task_name", StringType(), True),
        StructField("instance_num", LongType(), True),
        StructField("job_name", StringType(), True),
        StructField("task_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("start_time", LongType(), True),
        StructField("end_time", LongType(), True),
        StructField("plan_cpu", DoubleType(), True),
        StructField("plan_mem", DoubleType(), True)
    ]))

    batch_instance = spark.read.csv(os.path.join(path_to_dir, "batch_instance.csv"), schema=StructType([
        StructField("instance_name", StringType(), True),
        StructField("task_name", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("task_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("start_time", LongType(), True),
        StructField("end_time", LongType(), True),
        StructField("machine_id", StringType(), True),
        StructField("seq_no", LongType(), True),
        StructField("total_seq_no", LongType(), True),
        StructField("cpu_avg", DoubleType(), True),
        StructField("cpu_max", DoubleType(), True),
        StructField("mem_avg", DoubleType(), True),
        StructField("mem_max", DoubleType(), True)
    ]))

    @F.pandas_udf(returnType=Task.get_spark_type(), functionType=F.PandasUDFType.GROUPED_MAP)
    def clean_tasks_of_workflow(df):
        tasks = dict()
        raw_id_to_instances = dict()

        job_name = df.loc[0, "job_name"]
        workflow_id = mmh3.hash64(job_name)[1]

        invalid_task_raw_ids = set()

        # group by task name
        # - count number of instances
        # - compare with row.instance_num

        # Check to inspect if the data is noisy
        # def check(pdf):
        #     a = pdf["instance_name"].nunique()
        #     b = pdf["instance_name"].astype(np.int64).min()
        #     c = pdf["instance_name"].astype(np.int64).max()
        #     d = pdf["instance_num"].min()
        #     e = pdf["instance_num"].max()
        #     f = pdf["instance_name"].count()
        #     if d != e or b < 0 or c >= e or a != d or a != f:
        #         print("Noisy data! {}, {}, {}, {}, {}, {}".format(a, b, c, d, e, f))
        #
        # df.groupby("task_name").apply(check)

        for row in df.itertuples(index=False):
            if None in row:
                print(row, flush=True)
            task_name = row.task_name
            instance_name = str(row.instance_name)
            memory_requested = row.plan_mem
            resources_requested = row.plan_cpu
            resource_id = row.machine_id

            splits = task_name.split("_")

            if splits[0] == "task":
                cleaned_task_name = splits[1]
                task_type = "bag"
                raw_parents = []
            else:
                cleaned_task_name = splits[0][1:]
                task_type = str(splits[0][0])
                raw_parents = [x for x in splits[1:] if x.isdigit()]

            if resource_id is None:
                resource_id = -1
            else:
                resource_id = mmh3.hash64(row.machine_id)[1]

            if row.end_time is None or math.isnan(row.end_time):
                invalid_task_raw_ids.add(cleaned_task_name)
                continue

            if row.start_time is None or math.isnan(row.start_time):
                invalid_task_raw_ids.add(cleaned_task_name)
                continue

            if memory_requested is None or math.isnan(memory_requested):
                memory_requested = -1

            if resources_requested is None or math.isnan(resources_requested):
                avg_cpu = row.cpu_avg
                if avg_cpu is None or math.isnan(avg_cpu):
                    invalid_task_raw_ids.add(cleaned_task_name)
                    continue
                else:
                    resources_requested = avg_cpu

            this_task_id = mmh3.hash64(job_name + "@" + cleaned_task_name + "@" + instance_name)[1]

            if cleaned_task_name not in raw_id_to_instances:
                raw_id_to_instances[cleaned_task_name] = row.instance_num

            if row.instance_num > 10:
                # Create parent and child tasks
                raw_parent_id = cleaned_task_name + "_p"
                parent_task_id = mmh3.hash64(job_name + "@" + raw_parent_id + "@" + "0")[1]
                if parent_task_id not in tasks:
                    tasks[parent_task_id] = Task(id=parent_task_id, type="dummy", submission_site=0,
                                                 runtime=0,
                                                 ts_submit=row.start_time * 1000,
                                                 # We convert time from seconds to milliseconds.
                                                 resource_amount_requested=1, parents=raw_parents,
                                                 workflow_id=workflow_id,
                                                 wait_time=0, resource_type='core', resource=-1, memory_requested=-1)
                    raw_id_to_instances[raw_parent_id] = 1

                raw_child_id = cleaned_task_name + "_c"
                child_task_id = mmh3.hash64(job_name + "@" + raw_child_id + "@" + "0")[1]
                if child_task_id not in tasks:
                    tasks[child_task_id] = Task(id=child_task_id, type="dummy", submission_site=0,
                                                runtime=0,
                                                ts_submit=row.start_time * 1000,
                                                # We convert time from seconds to milliseconds.
                                                resource_amount_requested=1, parents=[cleaned_task_name],
                                                workflow_id=workflow_id,
                                                wait_time=0, resource_type='core', resource=-1, memory_requested=-1,
                                                params="child")
                    raw_id_to_instances[raw_child_id] = 1

                raw_parents = [raw_parent_id]

            this_task = Task(id=this_task_id, type=task_type, submission_site=0,
                             runtime=(row.end_time - row.start_time) * 1000,
                             ts_submit=row.start_time * 1000,  # We convert time from seconds to milliseconds.
                             resource_amount_requested=resources_requested, parents=raw_parents,
                             workflow_id=workflow_id, params=task_name + " $ " + instance_name + " $ " + str(
                    row.instance_num) + " $ " + job_name,
                             wait_time=0, resource_type='core', resource=resource_id, memory_requested=memory_requested)

            tasks[this_task_id] = this_task

        for task_id, task in tasks.items():
            task.parents = [p for p in task.parents if p not in invalid_task_raw_ids]
            parents = []
            for raw_parent_id in task.parents:
                # If previous wave has a child and this task is not that child.
                # refer to the child instead of the wave.
                if raw_parent_id + "_c" in raw_id_to_instances and task.params is not "child":
                    raw_parent_id = raw_parent_id + "_c"

                # We might hit an edge case where a parent was not recorded by the system of Alibaba
                # (e.g. bug or the tracing stopped)
                if raw_parent_id not in raw_id_to_instances:
                    continue

                parent_instances = raw_id_to_instances[raw_parent_id]

                proper_parent_ids = []
                for x in range(parent_instances):
                    # Alibaba tasks specify instance_nums, however these tasks may not necesarrily be in the data
                    # So we need to check if they are actually encountered.
                    hash = mmh3.hash64(job_name + "@" + raw_parent_id + "@" + str(x))[1]
                    if hash in tasks:
                        proper_parent_ids.append(hash)

                parents.extend(proper_parent_ids)
                for proper_id in proper_parent_ids:
                    tasks[proper_id].children.add(task_id)

            # task.params = None
            task.parents = parents

        # ze_best = pd.concat(pandas_dataframes)
        parquet_dicts = [task.get_parquet_dict() for task in tasks.values()]
        if len(tasks) > 0:
            ret = pd.DataFrame(parquet_dicts)
        else:  # If no task was valid, return an empty DF with the columns set. Otherwise Spark goes boom.
            ret = pd.DataFrame(columns=Task.get_parquet_meta_dict().keys())
        return ret

    @F.pandas_udf(returnType=Task.get_spark_type(), functionType=F.PandasUDFType.GROUPED_MAP)
    def container_to_task(df):
        row = df.iloc[0, :]
        start_time = df["time_stamp"].min() * 1000
        stop_time = df["time_stamp"].max() * 1000
        task_id = mmh3.hash64(row["container_id"])[1]
        workflow_id = mmh3.hash64(row["app_du"])[1]

        task = Task(id=task_id, type="long running",
                    parents=[],
                    ts_submit=start_time,  # We convert time from seconds to milliseconds.
                    submission_site=0, runtime=(start_time - stop_time), resource_amount_requested=row["cpu_request"],
                    memory_requested=row["mem_size"], workflow_id=workflow_id, wait_time=0,
                    resource=mmh3.hash64(row["machine_id"])[1])

        return pd.DataFrame([task.get_parquet_dict()])

    if not os.path.exists(os.path.join(TARGET_DIR, Task.output_path())):
        # Rename instances
        # This allows instance names to be derived using just the task name and number of instances of the task.
        task_window = Window.partitionBy("job_name", "task_name").orderBy("start_time")
        # Subtracting 1 becasue row number starts at 0. Makes later iteration more intuitive.
        # We are using instance name as an index in a particular job and task.
        instances_renamed = batch_instance.withColumn("instance_name",
                                                      (F.row_number().over(task_window) - F.lit(1)).cast(StringType()))

        tasks_unconverted = instances_renamed.join(
            batch_task.select("job_name", "task_name", "instance_num", "plan_cpu", "plan_mem"),
            on=["job_name", "task_name"], how="inner")

        # 100% this line is the issue.
        tasks_converted = tasks_unconverted.groupby("job_name").apply(clean_tasks_of_workflow)

        # if not os.path.exists(os.path.join(TARGET_DIR, Task.output_path())):
        #     tasks_converted.write.parquet(os.path.join(TARGET_DIR, Task.output_path()), mode="overwrite")

        long_running_tasks = container_meta.groupBy("container_id").apply(container_to_task)

        all_tasks = tasks_converted.union(long_running_tasks).dropna()

        try:
            all_tasks.printSchema()
            all_tasks.write.parquet(os.path.join(TARGET_DIR, Task.output_path()), mode="overwrite")
        except Exception as e:
            print(e, flush=True)
            raise e

    @F.pandas_udf(returnType=TaskState.get_spark_type(), functionType=F.PandasUDFType.GROUPED_MAP)
    def task_states_from_instances(df):
        task_states = []

        workflow_id = mmh3.hash64(df.loc[0, "job_name"])[1]

        for index, row in df.iterrows():
            job_name = row["job_name"]
            task_name = row["task_name"]
            instance_name = row["instance_name"]

            splits = task_name.split("_")
            just_task_name = splits[0][
                             1:]  # The first letter is irrelevant as it corresponds to nature of task (map or reduce)
            # and has nothing to do with the structure of the workflow.

            this_task_id = mmh3.hash64(job_name + "@" + just_task_name + "@" + instance_name)[1]

            this_task_state = TaskState(ts_start=row["start_time"] * 1000, ts_end=row["end_time"] * 1000,
                                        workflow_id=workflow_id, task_id=this_task_id,
                                        resource_id=mmh3.hash64(row["machine_id"])[1], cpu_rate=row["cpu_avg"],
                                        canonical_memory_usage=row["mem_avg"], maximum_cpu_rate=row["cpu_max"],
                                        maximum_memory_usage=row["mem_max"])

            if None in this_task_state.get_parquet_dict().values() or np.isnan(
                    this_task_state.get_parquet_dict().values()):
                print(this_task_state.get_parquet_dict())
                raise RuntimeError(this_task_state.get_parquet_dict())
            task_states.append(this_task_state.get_parquet_dict())

        return pd.DataFrame(task_states)

    @F.pandas_udf(returnType=TaskState.get_spark_type(), functionType=F.PandasUDFType.GROUPED_MAP)
    def task_states_from_container_usage(df):
        machine_id = mmh3.hash64(df.loc[0, "machine_id"])[1]

        def convert(cont_df):
            task_states = []

            prev_end_time = cont_df.loc[0, "start_time"] * 1000
            container_id = mmh3.hash64(cont_df.loc[0, "container_id"])[1]
            app_id = mmh3.hash64(cont_df.loc[0, "app_du"])[1]

            sorted_df = df.sort_values("time_stamp")
            for index, row in sorted_df.iterrows():
                this_end_time = row["time_stamp"] * 1000

                this_task_state = TaskState(ts_start=prev_end_time, ts_end=this_end_time,
                                            workflow_id=app_id, task_id=container_id,
                                            resource_id=machine_id, cpu_rate=row["cpu_util_percent"],
                                            canonical_memory_usage=row["mem_util_percent"],
                                            maximum_disk_bandwidth=row["disk_io_percent"],
                                            network_in=row["net_in"],
                                            network_out=row["net_out"])

                prev_end_time = this_end_time

                task_states.append(this_task_state.get_parquet_dict())
                if None in this_task_state.get_parquet_dict().values() or np.isnan(
                        this_task_state.get_parquet_dict().values()):
                    print(this_task_state.get_parquet_dict())
                    raise ArithmeticError(this_task_state.get_parquet_dict())

            return pd.DataFrame(task_states)

        return df.groupby("container_id").apply(convert).reset_index(drop=True).fillna(-1)

    # Now, derive workflows from tasks
    @F.pandas_udf(returnType=Workflow.get_spark_type(), functionType=F.PandasUDFType.GROUPED_MAP)
    def compute_workflow_stats(df):
        tasks = []

        for index, row in df.iterrows():
            this_task = Task(id=row["id"], type=row["type"], ts_submit=row["ts_submit"],
                             # We convert time from seconds to milliseconds.
                             submission_site=0, runtime=row["runtime"],
                             resource_amount_requested=row["resource_amount_requested"],
                             memory_requested=row["memory_requested"],
                             parents=row["parents"], workflow_id=row["workflow_id"], wait_time=row["wait_time"],
                             resource=row["resource_used"])
            # print(this_task.get_parquet_dict())
            tasks.append(this_task)

        workflow = Workflow(id=df.loc[0, "workflow_id"],
                            ts_submit=df["ts_submit"].min(),
                            tasks=tasks,
                            scheduler_description="Fuxi",
                            workflow_domain="Industrial",
                            workflow_application_name="MapReduce",
                            workflow_appliation_field="Internet Services"
                            )

        try:
            workflow.compute_critical_path()
        except toposort.CircularDependencyError:  # TODO: Some have cyclic dependencies. Check if this is us, or the data (again)
            pass

        return pd.DataFrame([workflow.get_parquet_dict()])

    if not os.path.exists(os.path.join(TARGET_DIR, Workflow.output_path())):
        tasks_df = spark.read.parquet(os.path.join(TARGET_DIR, Task.output_path()))  # Spark doesn't understand it can now read from files, so tell him
        workflow_df = tasks_df.groupBy("workflow_id").apply(compute_workflow_stats)

        workflow_df.write.parquet(os.path.join(TARGET_DIR, Workflow.output_path()), mode="overwrite",
                                  compression="snappy")

    def machine_meta_to_resources(row):
        resource = Resource(id=mmh3.hash64(row["machine_id"])[1],
                            type="cpu",
                            num_resources=float(row["cpu_num"]),
                            memory=row["mem_size"],
                            )
        resource_dict = resource.get_json_dict()
        del resource_dict["events"]
        return SparkRow(**resource_dict)

    if not os.path.exists(os.path.join(TARGET_DIR, Resource.output_path())):
        print("######\n Start parsing Resource DF\n ######")
        resource_df = machine_meta.rdd.map(machine_meta_to_resources).toDF(Resource.get_spark_type())
        resource_df.write.parquet(os.path.join(TARGET_DIR, Resource.output_path()), mode="overwrite",
                                  compression="snappy")

    print("######\n Start parsing Workload\n ######")
    if "tasks_df" not in locals():
        tasks_df = spark.read.parquet(os.path.join(TARGET_DIR, Task.output_path()))  # Spark doesn't understand it can now read from parquet files, so tell him
    json_dict = Workload.get_json_dict_from_spark_task_dataframe(tasks_df,
                                                                 domain="Industrial",
                                                                 authors=["Alibaba 2018"])

    os.makedirs(os.path.join(TARGET_DIR, Workload.output_path()), exist_ok=True)
    with open(os.path.join(TARGET_DIR, Workload.output_path(), "generic_information.json"), "w") as file:
        # Need this on 32-bit python.
        def default(o):
            if isinstance(o, np.int64):
                return int(o)

        file.write(json.dumps(json_dict, default=default))
    print("######\n Done parsing Workload\n ######")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(1)

    if len(sys.argv) == 3:
        TARGET_DIR = sys.argv[2]

    start = time.perf_counter()
    parse(sys.argv[1])
    print(time.perf_counter() - start)
