#!/usr/bin/env python3.5

"""
Parses the LANL Trinity trace file and converts it to WTA format.

First command line argument is the LANL trace file. Output is generated in
current working directory/LANL.
"""
import json
import mmh3
import os
import re
import sys
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from objects.task import Task
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = 'Usage: python(3) ./lanl_traces_to_parquet.py lanl_file'
NAME = 'Trinity'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', 'LANL', NAME)


def parse_lanl_file(lanl_file):
    task_list = []
    task_by_id = {}

    df = pd.read_csv(lanl_file, parse_dates=["submission_time", "start_date", "end_date"], infer_datetime_format=True)
    task_df = df[df['object_event'] == "JOBEND"]

    earliest_date = df['submission_time'].min()
    latest_date = df["end_date"].max()

    for index, row in task_df.iterrows():
        id = str(mmh3.hash64("task:{}".format(str(row["object_id"]).strip()))[0])

        # Task time fields
        submission_time_task = row["submission_time"]
        start_time_task = row["start_date"]
        end_time_task = row["end_date"]

        # Task cpu consumption fields
        num_nodes = int(row["nodes_requested"])
        num_cpus_per_node = row["dedicated_processors_per_task"]

        # Task dependency fields
        extension_string = str(row["resource_manager_extension_string"])
        # Find dependencies
        match = re.search('DEPEND=([\w,:.]+);?', extension_string)
        if not match:
            dependencies = set()
        else:
            dependencies = match.group(1)
            dependencies = set(
                str(mmh3.hash64("task:{}".format(str(dep).strip()))[0]) for dep in dependencies.split("&"))

        task_wait_time = int((start_time_task - submission_time_task).total_seconds() * 1000)
        task_runtime = int((end_time_task - start_time_task).total_seconds() * 1000)

        task = Task(id, "Atomic", start_time_task, -1, task_runtime, num_nodes * num_cpus_per_node, dependencies, -1,
                    task_wait_time, resource_type="core", resource=-1)

        # Convert the ts_submit to seconds instead of a datetime string
        EPOCH = datetime(1970, 1, 1, tzinfo=task.ts_submit.tzinfo)
        task.ts_submit = int((task.ts_submit - EPOCH).total_seconds() * 1000)

        # Set the wallclock limit
        task.nfrs["runtime_limit"] = row["wallclock_limit"]

        task_by_id[id] = task
        task_list.append(task)

    min_ts_submit = min(task.ts_submit for task in task_list)

    # For every task, add itself the the children of its parents
    for task in task_list:
        task.ts_submit -= min_ts_submit  # Make sure the first task in the trace starts at 0
        invalid_parents = set()
        for parent_id in task.parents:
            # Chop of the prepend of *: (e.g. jobsuccess:)
            actual_parent_id = parent_id[str(parent_id).find(":") + 1:]
            if actual_parent_id in task_by_id:  # If this doesn't fire, the task probably failed, we filter those out.
                parent = task_by_id[actual_parent_id]
                parent.children.add(task.id)
            else:
                invalid_parents.add(parent_id)

        # Remove invalid parents
        if invalid_parents:
            task.parents -= invalid_parents

    # Find start tasks and assign workflow ids
    workflow_id = 0
    for task in task_list:
        if task.workflow_id == -1:
            root_parents = task.get_root_parents(task_by_id)
            if root_parents:  # If there are start tasks, propogate from them
                for root_parent_id in root_parents:
                    actual_root_id = root_parent_id[str(root_parent_id).find(":") + 1:]
                    task_by_id[actual_root_id].set_workflow_id_propagating(task_by_id, workflow_id)
            else:  # Else it's a single job so just set the property directly
                task.workflow_id = workflow_id
            workflow_id += 1

    # Now that every thing has been computed, we write the tasks to parquet files
    os.makedirs(os.path.join(TARGET_DIR, Task.output_path()), exist_ok=True)
    task_df = pd.DataFrame([task.get_parquet_dict() for task in task_list])

    # Make sure the first workflow is submitted at time 0
    min_submit_time = task_df["ts_submit"].min()
    task_df = task_df.assign(ts_submit=lambda x: x['ts_submit'] - min_submit_time)

    task_df.to_parquet(os.path.join(TARGET_DIR, Task.output_path(), "part.0.parquet"), engine="pyarrow")

    workflows = dict()
    # Based on workflow ids, constuct the workflow objects
    for task in task_list:
        if task.workflow_id in workflows:
            workflow = workflows[task.workflow_id]
        else:
            workflow = Workflow(workflow_id, None, [], "", "Scientific", "Uncategorized", "Uncategorized")
            workflows[task.workflow_id] = workflow

        if not workflow.ts_submit:
            workflow.ts_submit = task.ts_submit
        else:
            workflow.ts_submit = min(workflow.ts_submit, task.ts_submit)

        workflow.tasks.append(task)
        workflow.task_count = len(workflow.tasks)

    for w in workflows.values():
        w.compute_critical_path(strip_colon=True)

    os.makedirs(os.path.join(TARGET_DIR, Workflow.output_path()), exist_ok=True)
    workflow_df = pd.DataFrame([workflow.get_parquet_dict() for workflow in workflows.values()])
    workflow_df.to_parquet(os.path.join(TARGET_DIR, Workflow.output_path(), "part.0.parquet"), engine="pyarrow")

    # Write a json dict with the workload properties
    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(task_df,
                                                                  domain="Engineering",
                                                                  start_date=str(earliest_date),
                                                                  end_date=str(latest_date),
                                                                  authors=["George Amvrosiadis", "Jun Woo Park",
                                                                           "Gregory R. Ganger", "Garth A. Gibson",
                                                                           "Elisabeth Baseman", "Nathan DeBardeleben"],
                                                                  workload_description="This workload was published by Amvrosiadis et al. as part of their ATC 2018 paper titled \"On the diversity of cluster workloads and its impact on research results\". It is the Trinity trace from the Los Almos National Laboratory."
                                                                  )

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

    parse_lanl_file(sys.argv[1])
