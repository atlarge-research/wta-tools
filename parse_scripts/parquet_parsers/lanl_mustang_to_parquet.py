#!/usr/bin/env python3.5

"""
Parses the LANL Mustang trace file and converts it to WTA format.

First command line argument is the LANL trace file. Output is generated in
current working directory/LANL.
"""
import json
import os
import sys

import numpy as np
import pandas as pd


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from objects.workload import Workload
from objects.workflow import Workflow

USAGE = 'Usage: python(3) ./lanl_traces_to_parquet.py mustang_file'
NAME = 'Mustang'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', 'LANL', NAME)


def parse_lanl_file(lanl_file):
    df = pd.read_csv(lanl_file)

    # Obtain all completed tasks
    workflow_df = df[(df['job_status'] == "COMPLETED") | (df['job_status'] == "TIMEOUT")]

    # Some jobs seem to be malformed in the trace, having no start, submit or end time.
    # Even tough they show as COMPLETED in this trace, those are filtered out.
    workflow_df = workflow_df[workflow_df['start_time'].notnull()]
    workflow_df = workflow_df[workflow_df['submit_time'].notnull()]
    workflow_df = workflow_df[workflow_df['end_time'].notnull()]
    workflow_df.reset_index()

    workflow_df['submit_time'] = workflow_df['submit_time'].astype(str)
    workflow_df['end_time'] = workflow_df['end_time'].astype(str)

    start_date = workflow_df['submit_time'].min()
    end_date = workflow_df['end_time'].max()

    workflow_df = workflow_df.reset_index()

    workflow_df = workflow_df.rename(columns={"node_count": "total_resources", "submit_time": "ts_submit", "tasks_requested": "task_count",
                                "wallclock_limit": "nfrs", "index": "id"})
    workflow_df["critical_path_length"] = np.int32(-1)
    workflow_df["critical_path_task_count"] = np.int32(-1)
    workflow_df["approx_max_concurrent_tasks"] = np.int32(-1)
    workflow_df["scheduler"] = np.str("")
    workflow_df["total_memory_usage"] = np.int64(-1)
    workflow_df["total_network_usage"] = np.int64(-1)
    workflow_df["total_disk_space_usage"] = np.int64(-1)
    workflow_df["total_energy_consumption"] = np.int64(-1)

    print(workflow_df.columns)

    # Make sure the string is a JSON dump.
    workflow_df['nfrs'] = workflow_df['nfrs'].apply(lambda x: json.dumps({"deadline:": x}))

    # Drop all columns except which are provided
    workflow_df = workflow_df[list(Workflow.get_parquet_meta_dict().keys())]

    # Make sure the first workflow is submitted at time 0
    min_submit_time = workflow_df["ts_submit"].min()

    def offset_and_convert_submit_time(date):
        return (pd.to_datetime(date).tz_convert(None) - pd.to_datetime(min_submit_time).tz_convert(None)).total_seconds() * 1000

    workflow_df['ts_submit'] = workflow_df['ts_submit'].apply(offset_and_convert_submit_time)

    os.makedirs(os.path.join(TARGET_DIR, Workflow.output_path()), exist_ok=True)

    workflow_df.to_parquet(os.path.join(TARGET_DIR,  Workflow.output_path(), "part.0.parquet"), engine="pyarrow")

    json_dict = {
        "total_workflows": len(workflow_df),
        "total_tasks": workflow_df["task_count"].sum(),
        "domain": "Engineering",
        "date_start": str(start_date),
        "date_end": str(end_date),
        "num_sites": -1,
        "num_resources": workflow_df["total_resources"].sum(),
        "num_users": -1,
        "num_groups": -1,
        "total_resource_seconds": -1,
        "authors": ["George Amvrosiadis", "Jun Woo Park", "Gregory R. Ganger", "Garth A. Gibson", "Elisabeth Baseman",
                    "Nathan DeBardeleben"],
        "min_resource_task": -1,
        "max_resource_task": -1,
        "std_resource_task": 0,
        "mean_resource_task": -1,
        "median_resource_task": -1,
        "first_quartile_resource_task": -1,
        "third_quartile_resource_task": -1,
        "cov_resource_task": 0,
        "min_memory": -1,
        "max_memory": -1,
        "std_memory": 0,
        "mean_memory": -1,
        "median_memory": -1,
        "first_quartile_memory": -1,
        "third_quartile_memory": -1,
        "cov_memory": 0,
        "min_network_usage": -1,
        "max_network_usage": -1,
        "std_network_usage": 0,
        "mean_network_usage": -1,
        "median_network_usage": -1,
        "first_quartile_network_usage": -1,
        "third_quartile_network_usage": -1,
        "cov_network_usage": 0,
        "min_disk_space_usage": -1,
        "max_disk_space_usage": -1,
        "std_disk_space_usage": 0,
        "mean_disk_space_usage": -1,
        "median_disk_space_usage": -1,
        "first_quartile_disk_space_usage": -1,
        "third_quartile_disk_space_usage": -1,
        "cov_disk_space_usage": 0,
        "min_energy": -1,
        "max_energy": -1,
        "std_energy": 0,
        "mean_energy": -1,
        "median_energy": -1,
        "first_quartile_energy": -1,
        "third_quartile_energy": -1,
        "cov_energy": 0,
        "workload_description": "This workload was published by Amvrosiadis et al. as part of their ATC 2018 paper titled \"On the diversity of cluster workloads and its impact on research results\".",
    }

    os.makedirs(os.path.join(TARGET_DIR, Workload.output_path()), exist_ok=True)

    with open(os.path.join(TARGET_DIR, Workload.output_path(), "generic_information.json"), "w") as file:
        # Need this on 32-bit python.
        def default(o):
            if isinstance(o, np.int64): return int(o)
            raise TypeError

        file.write(json.dumps(json_dict, default=default))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    parse_lanl_file(sys.argv[1])
