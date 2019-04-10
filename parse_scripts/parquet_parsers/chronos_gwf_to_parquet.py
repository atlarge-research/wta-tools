import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from parquet_helper_functions.workflow_characteristics import compute_characteristics
from objects.task import Task
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = 'Usage: python(3) ./chronos_gwf_to_parquet.py chronos_file'
NAME = 'chronos'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', NAME)


def compute_workflow_features(task_group):
    wf_agnostic_df = compute_characteristics(task_group)

    tasks = []
    for row in task_group.itertuples():
        task = Task(
            getattr(row, "id"),
            "Composite",
            getattr(row, "ts_submit"),
            0,  # There is just one submissions site
            getattr(row, "runtime"),
            getattr(row, "resource_amount_requested"),
            getattr(row, "parents"),
            0,  # Not able to get the workflow_id attribute for some reason.
            -1,
            "thread",
            resource=-1,
        )

        task.children = getattr(row, "children")

        tasks.append(task)

    workflow_ts_submit = task_group["ts_submit"].min()

    workflow = Workflow(0, workflow_ts_submit, tasks, "ANANKE", "Industrial", "Chronos", "IoT")
    workflow.compute_critical_path()

    wf_df = pd.concat([wf_agnostic_df, pd.DataFrame(
        {"ts_submit": pd.Series([np.int64(workflow_ts_submit)], dtype=np.int64),
         "critical_path_length": pd.Series([np.int32(workflow.critical_path_length)], dtype=np.int32),
         "critical_path_task_count": pd.Series([np.int32(workflow.critical_path_task_count)], dtype=np.int32),
         "approx_max_concurrent_tasks": pd.Series([np.int32(workflow.max_concurrent_tasks)], dtype=np.int32),
         "scheduler": pd.Series([np.str("ANANKE")], dtype=np.str),
         })], axis=1)

    wf_df["nfrs"] = np.str("{}")

    wf_df = wf_df[sorted(wf_df.columns)]

    return wf_df


def compute_children(task_group):
    task_to_children_map = {}

    for row in task_group.itertuples():
        for parent in getattr(row, "parents"):
            if parent not in task_to_children_map:
                task_to_children_map[parent] = np.empty(0, dtype=np.int64)

            task_to_children_map[parent] = np.append(task_to_children_map[parent], getattr(row, "id"))

    children_of_tasks_inorder = []
    for task_id in task_group["id"]:
        if task_id in task_to_children_map:
            children_of_tasks_inorder.append(task_to_children_map[task_id])
        else:
            children_of_tasks_inorder.append([])

    task_group_with_children = task_group.assign(children=pd.Series(children_of_tasks_inorder,
                                                                    dtype=np.object,
                                                                    index=task_group.index))

    return task_group_with_children[sorted(task_group_with_children.columns)]


def parse(gwf_filename):
    os.makedirs(TARGET_DIR, exist_ok=True)

    gwf_tasks = pd.read_csv(gwf_filename, skipinitialspace=True, dtype={
        "WorkflowID": np.int64,
        "JobID": np.int64,
        "SubmitTime": np.int64,
        "Runtime": np.int64,
        "NProcs": np.int32,
        "Dependencies": np.str,
    })
    del gwf_tasks["ReqNProcs "]
    gwf_tasks.columns = ["workflow_id", "id", "ts_submit", "runtime", "resource_amount_requested", "dependencies"]

    gwf_tasks = gwf_tasks.assign(ts_submit=lambda x: x['ts_submit'] * 1000)  # Convert the submit time to milliseconds.

    gwf_tasks_with_parents = gwf_tasks.assign(parents=gwf_tasks["dependencies"].str.split()
                                              .apply(lambda l: [np.int64(i) for i in l] if type(l) is list else []))

    del gwf_tasks_with_parents["dependencies"]  # We need to recompute these (and the column name is wrong), so delete.

    # Add columns not present in the trace.
    gwf_tasks_with_parents["type"] = np.str("composite")
    gwf_tasks_with_parents["resource_type"] = np.str("thread")
    gwf_tasks_with_parents["submission_site"] = np.int32(0)
    gwf_tasks_with_parents["user_id"] = np.int32(-1)
    gwf_tasks_with_parents["group_id"] = np.int32(-1)
    gwf_tasks_with_parents["nfrs"] = np.str(" ")
    gwf_tasks_with_parents["wait_time"] = np.int64(-1)
    gwf_tasks_with_parents["params"] = np.str("{}")
    gwf_tasks_with_parents["memory_requested"] = np.int64(-1)
    gwf_tasks_with_parents["disk_io_time"] = np.int64(-1)
    gwf_tasks_with_parents["disk_space_requested"] = np.int64(-1)
    gwf_tasks_with_parents["energy_consumption"] = np.int64(-1)
    gwf_tasks_with_parents["network_io_time"] = np.int64(-1)
    gwf_tasks_with_parents["resource_used"] = np.str("[]")

    # We need to make sure that all pandas dataframes follow the same column order.
    # The dask dataframe is build up using different pandas dataframes. So they must match.
    gwf_tasks_with_parents = gwf_tasks_with_parents[sorted(gwf_tasks_with_parents.columns)]

    gwf_tasks_with_children = gwf_tasks_with_parents.groupby("workflow_id").apply(compute_children) \
        .reset_index(drop=True)

    # Make sure the first task has ts_submit of zero.
    min_submit_time = gwf_tasks_with_children["ts_submit"].min()
    task_df_final = gwf_tasks_with_children.assign(ts_submit=lambda x: x['ts_submit'] - min_submit_time)

    os.makedirs(os.path.join(TARGET_DIR, Task.output_path()), exist_ok=True)
    task_df_final.to_parquet(os.path.join(TARGET_DIR, Task.output_path(), "part.0.parquet"))

    # Compute workflow properties specific to this trace
    workflow_df = task_df_final.groupby("workflow_id").apply(compute_workflow_features).reset_index(drop=True)

    workflow_df = workflow_df.rename(columns={"workflow_id": "id"})

    workflow_df = workflow_df[sorted(workflow_df.columns)]

    os.makedirs(os.path.join(TARGET_DIR, Workflow.output_path()), exist_ok=True)
    workflow_df.to_parquet(os.path.join(TARGET_DIR, Workflow.output_path(), "part.0.parquet"))

    # Write a json dict with the workload properties
    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(task_df_final,
                                                                domain="Industrial",
                                                                authors=["Shenjun Ma", "Alexey Ilyushkin",
                                                                         "Alexander Stegehuis", "Alexandru Iosup"],
                                                                workload_description="Chronos is a trace from Shell's Chronos IoT production system. It contains pipelines where sensor data is obtained, checked if values are within range (e.g. temperature, operational status, etc.), and the outcomes are written to persistent storage."
                                                                )

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

    file_path = sys.argv[1]
    parse(file_path)
