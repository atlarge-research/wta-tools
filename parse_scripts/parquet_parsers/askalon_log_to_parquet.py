#!/usr/bin/env python3.7
"""
Parses askalon trace file and converts it to WTA format.
JobID , SubmitTime , RunTime , NProcs , ReqNProcs , Dependencies

First command line argument is the askalon trace file. Output is generated in
current working directory/askalon.

Uses comments as an indicator of a new workflow and uses it to calculate
workflow start timestamp:
# Started: Thu May 10 16:19:25 CEST 2007, Runtime: 97364 ms (~1min)
Gwf tasks are created with ts_submit relative to the workflow start timestamp.

If neither submission time nor wait time are positive then the workflow is skipped
altogether (a single task may be skipped but it might represent a dependency for another task).
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
from objects.task_state import TaskState
from objects.task import Task
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = 'Usage: python(3) ./askalon_log_to_parquet.py askalon_file'
DATETIME_FORMAT = '%a %b %d %H:%M:%S  %Y'
EPOCH = datetime(1970, 1, 1)
WORKFLOW_INDEX = 0
RESOURCE_INDEX = 0
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', 'askalon', 'ee')


def task_info_from_line(line, workflow_start, workflow_index):
    cols = line.split('\t')

    workflow_id = mmh3.hash64("workflow:{}".format(workflow_index))[0]
    task_id = mmh3.hash64("workflow:{}_task:{}".format(workflow_index, int(cols[0])))[0]

    task_submit_time = int(cols[1])
    wait_time = float(cols[2])

    # Weird bug in the trace, sometimes the task submit time is -1, but then the
    # wait time is the unix timestamp...
    if task_submit_time == -1 and wait_time > 1000000000000:
        task_submit_time = wait_time
        wait_time = -1

    submit_time = workflow_start - task_submit_time

    if not submit_time:
        return None, None

    site_id = mmh3.hash64("site:{}".format(str(cols[16]).strip()))[0]
    run_time = int(cols[3])
    n_procs = int(cols[4])
    req_n_procs = int(n_procs)
    used_memory = float(cols[6])
    used_network = float(cols[20])
    disk_space_used = float(cols[21])
    user_id = mmh3.hash64("user:{}".format(str(cols[11]).strip()))[0]
    group_id = mmh3.hash64("group:{}".format(str(cols[12]).strip()))[0]

    job_structure = cols[19]
    match = re.search('DAGPrev=([\d,]+);?', job_structure)
    if not match:
        dependencies = set()
    else:
        dependency_string = match.group(1)
        dependencies = set(
            mmh3.hash64("workflow:{}_task:{}".format(workflow_index, int(dep)))[0] for dep in dependency_string.split(","))

    task = Task(task_id, "composite", submit_time, site_id, run_time, max(n_procs, req_n_procs), dependencies,
                workflow_id, wait_time=wait_time, user_id=user_id, group_id=group_id, resource=-1)

    task_state = TaskState(submit_time, submit_time + run_time, workflow_id, task_id, -1,
                           canonical_memory_usage=used_memory, local_disk_space_usage=disk_space_used,
                           maximum_network_bandwidth=used_network)

    match = re.search('DAGNext=([\d,]+);?', job_structure)
    if not match:
        children = set()
    else:
        children_string = match.group(1)
        children = set(
            mmh3.hash64("workflow:{}_task:{}".format(workflow_index, int(dep)))[0] for dep in children_string.split(","))

    task.children = children

    return task, task_state


def get_workflow_from_tasks(tasks, workflow_start, askalon_file, workflow_index):

    workflow_id = mmh3.hash64("workflow:{}".format(workflow_index))[0]

    application_name = "BWA" if "bwa" in askalon_file.lower() else "Wien2k"

    workflow = Workflow(workflow_id, workflow_start, tasks, "Askalon", "Engineering", application_name, "Chemical Engineering")
    workflow.compute_critical_path()

    return workflow


def parse_askalon_file(askalon_file):
    os.makedirs(TARGET_DIR, exist_ok=True)

    workflow_index = 0
    invalid_workflow_count = 0
    workflow_start = None
    invalid_workflow = False

    final_task_list = []
    final_taskstate_list = []
    final_workflow_list = []

    tasks = []
    task_by_id = dict()
    task_state_list = []
    with open(askalon_file, 'r') as asklon_trace:
        for line in asklon_trace.readlines():
            if line.startswith('#'):
                if not line.startswith('# Started:'):
                    continue

                workflow_date = re.search('# Started: (.+),', line).group(1)

                if int(workflow_date[
                       -4:]) == 1970:  # filter out "# Started: Thu Jan 01 01:00:00 CET 1970, and did not finish (yet)"
                    invalid_workflow = True
                    continue

                if workflow_date.find('CEST ') >= 0:
                    timezone_diff = 7200
                    workflow_date = workflow_date.replace('CEST ', '')
                elif workflow_date.find('CET') >= 0:
                    timezone_diff = 3600
                    workflow_date = workflow_date.replace('CET ', '')
                else:
                    raise Exception('Line "{}"" does not follow expected CEST or CET format'.format(line))

                # Create a workflow based on observed tasks before starting a new.
                if tasks:
                    # Since we cannot trust the logs (our validator proved this)
                    # We will have to add both the children and parents manually to be safe.
                    for t in tasks:
                        for child_id in t.children:
                            task_by_id[child_id].parents.add(t.id)
                        for parent_id in t.parents:
                            task_by_id[parent_id].children.add(t.id)

                    workflow = get_workflow_from_tasks(tasks, workflow_start, askalon_file, workflow_index)

                    final_workflow_list.append(workflow)

                    final_task_list.extend(tasks)
                    final_taskstate_list.extend(task_state_list)

                    workflow_index += 1

                    tasks = []
                    task_state_list = []

                workflow_start = int(((datetime.strptime(workflow_date, DATETIME_FORMAT) - EPOCH).total_seconds()
                                      - timezone_diff) * 1000)
                invalid_workflow = False  # new workflow begins, reset flag
            else:
                if invalid_workflow:  # skip reading tasks, advance to the next workflow
                    # print("Found invalid workflow, skipping")
                    invalid_workflow_count += 1
                    continue

                task, task_state = task_info_from_line(line, workflow_start, workflow_index)

                if task:
                    if task.runtime < 0:
                        invalid_workflow = True
                        invalid_workflow_count += 1
                        tasks = []
                        task_state_list = []
                        continue

                    tasks.append(task)
                    task_by_id[task.id] = task
                    task_state_list.append(task_state)
                else:
                    invalid_workflow = True
                    invalid_workflow_count += 1
                    tasks = []
                    task_state_list = []

    print(workflow_index, invalid_workflow_count)

    # Flush the last workflow, if any.
    if tasks and not invalid_workflow:
        # Since we cannot trust the logs (our validator proved this)
        # We will have to add both the children and parents manually to be safe.
        for t in tasks:
            for child_id in t.children:
                task_by_id[child_id].parents.add(t.id)
            for parent_id in t.parents:
                task_by_id[parent_id].children.add(t.id)

        final_task_list.extend(tasks)
        final_taskstate_list.extend(task_state_list)

        workflow = get_workflow_from_tasks(tasks, workflow_start, askalon_file, workflow_index)
        final_workflow_list.append(workflow)

        workflow_index += 1

    task_df = pd.DataFrame([t.get_parquet_dict() for t in final_task_list])

    # Offset the first task arriving at 0 (and thus the thus the first workflow)
    min_ts_submit = task_df["ts_submit"].min()
    task_df["ts_submit"] = task_df["ts_submit"] - min_ts_submit

    os.makedirs(os.path.join(TARGET_DIR, Task.output_path()), exist_ok=True)
    task_df.to_parquet(os.path.join(TARGET_DIR, Task.output_path(), "part.0.parquet"), engine="pyarrow")

    # Write task states
    task_state_df = pd.DataFrame([ts.get_parquet_dict() for ts in final_taskstate_list])
    # offset the times
    task_state_df["ts_start"] = task_state_df["ts_start"] - min_ts_submit
    task_state_df["ts_end"] = task_state_df["ts_end"] - min_ts_submit

    os.makedirs(os.path.join(TARGET_DIR, TaskState.output_path()), exist_ok=True)
    task_state_df.to_parquet(os.path.join(TARGET_DIR, TaskState.output_path(), "part.0.parquet"), engine="pyarrow")

    # Write the workflow dataframe
    workflow_df = pd.DataFrame([w.get_parquet_dict() for w in final_workflow_list])

    # Also offset workflow_df
    workflow_df["ts_submit"] = workflow_df["ts_submit"] - min_ts_submit

    os.makedirs(os.path.join(TARGET_DIR, Workflow.output_path()), exist_ok=True)
    workflow_df.to_parquet(os.path.join(TARGET_DIR, Workflow.output_path(), "part.0.parquet"), engine="pyarrow")

    workload_description = ""
    if "bwa" in askalon_file.lower():
        workload_description = "BWA (short for Burroughs-Wheeler Alignment tool) is a genomics analysis workflow, courtesy of Scott Emrich and Notre Dame Bioinformatics Laboratory. It maps low-divergent sequences against a large reference genome, such as the human genome."
    elif "wien2k" in askalon_file.lower():
        workload_description = "Wien2k uses a full-potential Linearized Augmented Plane Wave (LAPW) approach for the computation of crystalline solids."

    # Write a json dict with the workload properties
    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(task_df,
                                                                  domain="Engineering",
                                                                  authors=["Radu Prodan", "Alexandru Iosup"],
                                                                  workload_description=workload_description)

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

    parse_askalon_file(sys.argv[1])
