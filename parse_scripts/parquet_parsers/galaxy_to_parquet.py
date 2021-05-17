import json
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

from objects.task import Task
from objects.workflow import Workflow
from objects.workload import Workload
pd.set_option('display.max_columns', None)


USAGE = 'Usage: python(3) ./galaxy_to_parquet.py galaxy_folder'
NAME = 'Galaxy'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', NAME)
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'
EPOCH = datetime(1970, 1, 1)
JOBS = None
METRICS = None
WORKFLOWS = None
WORKFLOW_INVOCATIONS = None
WORKFLOW_STEPS = None
WORKFLOW_INVOKE_STEPS = None
WORKFLOW_CONNECTIONS = None
WORKFLOW_STEP_INPUT = None


def read_files(folder_path):
    global METRICS
    METRICS = pd.read_csv(os.path.join(folder_path, 'job_metrics_numeric.csv'),
                          names=["id", "job_id", "plugin", "metric_name", "metric_value"], dtype={
            "id": np.float,
            "job_id": np.float,
            "plugin": np.str,
            "metric_name": np.str,
            "metric_value": np.float,
        })
    print("Done with reading metrics")
    global WORKFLOWS
    WORKFLOWS = pd.read_csv(os.path.join(folder_path, 'workflows.csv'),
                            names=["id", "create_time", "update_time", "stored_workflow_id", "has_cycles", "has_errors",
                                   "parent_workflow_id", "uuid"], dtype={
            "id": np.float,
            "create_time": np.str,
            "update_time": np.str,
            "stored_workflow_id": np.float,
            "has_cycles": np.str,
            "has_errors": np.str,
            "parent_workflow_id": np.float,
            "uuid": np.str,
        })
    print("Done with reading workflows")

    global WORKFLOW_INVOCATIONS
    WORKFLOW_INVOCATIONS = pd.read_csv(os.path.join(folder_path, 'workflow-invocations.csv'),
                                       names=["id", "create_time", "update_time", "workflow_id", "state", "scheduler",
                                              "handler"], dtype={
            "id": np.float,
            "create_time": np.str,
            "update_time": np.str,
            "workflow_id": np.float,
            "state": np.str,
            "scheduler": np.str,
            "handler": np.str,
        })
    print("Done with reading workflow invocations")

    global WORKFLOW_STEPS
    WORKFLOW_STEPS = pd.read_csv(os.path.join(folder_path, 'workflow-steps.csv'),
                                 names=["id", "create_time", "update_time", "workflow_id", "type", "tool_id",
                                        "tool_version", "order_index", "subworkflow_id", "dynamic_tool_id"], dtype={
            "id": np.float,
            "create_time": np.str,
            "update_time": np.str,
            "workflow_id": np.float,
            "type": np.str,
            "tool_id": np.str,
            "tool_version": np.str,
            "order_index": np.float,
            "subworkflow_id": np.str,
            "dynamic_tool_id": np.str,
        })
    print("Done with reading workflow steps")

    global WORKFLOW_INVOKE_STEPS
    WORKFLOW_INVOKE_STEPS = pd.read_csv(os.path.join(folder_path, 'workflow-invoke-steps.csv'), keep_default_na=True,
                                        names=["id", "create_time", "update_time", "workflow_invocation_id",
                                               "workflow_step_id", "job_id", "state"], dtype={
            "id": np.float,
            "create_time": np.str,
            "update_time": np.str,
            "workflow_invocation_id": np.float,
            "workflow_step_id": np.float,
            "job_id": np.float,
            "state": np.str,
        })
    print("Done with reading workflow invocation steps")

    global WORKFLOW_CONNECTIONS
    WORKFLOW_CONNECTIONS = pd.read_csv(os.path.join(folder_path, 'workflow-connections.csv'),
                                       names=["id", "output_step_id", "input_step_input_id", "output_name",
                                              "input_subworkflow_step_id"], dtype={
            "id": np.float,
            "output_step_id": np.float,
            "input_step_input_id": np.float,
            "output_name": np.str,
            "input_subworkflow_step_id": np.float,
        })
    print("Done with reading workflow connections")

    global WORKFLOW_STEP_INPUT
    WORKFLOW_STEP_INPUT = pd.read_csv(os.path.join(folder_path, 'workflow-step-input.csv'),
                                      names=["id", "workflow_step_id", "name"], dtype={
            "id": np.float,
            "workflow_step_id": np.float,
            "name": np.str,
        })
    print("Done with reading workflow step input")


def check_if_empty(*args):
    for field in args:
        if np.isnan(field):
            return True


def compute_children(step_job_ids, tasks_in_workflow):
    for task in tasks_in_workflow:
        step_id = None
        for pair in step_job_ids:
            # find task id's corresponding step id
            if pair[1] == task.id:
                step_id = pair[0]

        children = set()
        df = WORKFLOW_CONNECTIONS.loc[(WORKFLOW_CONNECTIONS["output_step_id"] == step_id)]

        if df.empty:
            task.children = children
            continue

        for wc_row in df.itertuples():

            # find id for subsequent connected step
            row = WORKFLOW_STEP_INPUT.loc[(WORKFLOW_STEP_INPUT["id"] == wc_row[3])]

            child_step_id = row.iloc[0]["workflow_step_id"]

            # find child_step_id in step-job pairs and add corresponding job_id to children set
            for pair2 in step_job_ids:
                if pair2[0] == child_step_id:
                    children.add(np.int64(pair2[1]))
                    for child in tasks_in_workflow:
                        if child.id == pair2[1]:
                            child.parents.append(np.int64(task.id))
                            break
                    break
        task.children = children
    for task2 in tasks_in_workflow:
        unique_parents = set(task2.parents)
        unique_parents_list = list(unique_parents)
        task2.parents = unique_parents_list

    return tasks_in_workflow


def parse():
    os.makedirs(TARGET_DIR, exist_ok=True)
    task_counter = 0
    workflow_counter = 0
    processed_workflows = []
    final_workflows = []
    final_tasks = []
    task_offset = 0
    workflow_offset = None

    for wi_row in WORKFLOW_INVOCATIONS.itertuples():
        flag = False
        # only use one execution of a workflow
        if wi_row[4] in processed_workflows:
            continue

        # check if workflow contains cycles
        workflow_row = WORKFLOWS.loc[(WORKFLOWS["id"] == getattr(wi_row, "workflow_id"))]
        if workflow_row.iloc[0]["has_cycles"] == "t":
            continue

        # workflows contain a number of workflow steps but this is not the ID of their actual execution
        # this list is used to tie the workflow steps to their actual execution ID
        step_job_ids = []

        tasks_in_workflow = []
        workflow_index = wi_row[4]
        # check if workflow id is null
        if pd.isnull(workflow_index):
            continue

        df = WORKFLOW_INVOKE_STEPS.loc[(WORKFLOW_INVOKE_STEPS["workflow_invocation_id"] == getattr(wi_row, "id"))]

        # check if workflow is not empty
        if df.empty:
            processed_workflows.append(workflow_index)
            continue

        for wis_row in df.itertuples():

            # check if entry in WF_INVOKE_STEPS has the same wf_invocation_id
            if getattr(wis_row, "workflow_invocation_id") == getattr(wi_row, "id"):

                # check if required fields are not empty
                if check_if_empty(getattr(wis_row, "workflow_step_id"), getattr(wis_row, "job_id")):
                    processed_workflows.append(workflow_index)
                    flag = True
                    break

                # get step id and corresponding execution id
                step_job_pair = [getattr(wis_row, "workflow_step_id"), getattr(wis_row, "job_id")]
                step_job_ids.append(step_job_pair)

                job_id = getattr(wis_row, "job_id")
                submit_time = int(((datetime.strptime(getattr(wis_row, "create_time"),DATETIME_FORMAT) - EPOCH).total_seconds()) * 1000)
                job_metrics = METRICS.loc[(METRICS["job_id"] == job_id)]
                runtime = job_metrics.loc[(job_metrics["metric_name"] == "runtime_seconds"), 'metric_value'] * 1000
                memory = job_metrics.loc[(job_metrics["metric_name"] == "memory.memsw.max_usage_in_bytes"), 'metric_value']
                cpu_time = job_metrics.loc[(job_metrics["metric_name"] == "cpuacct.usage"), 'metric_value']

                # check if any required fields are empty
                if runtime.empty or memory.empty or cpu_time.empty:
                    processed_workflows.append(workflow_index)
                    flag = True
                    break

                # used to find the task with lowest submit time, this time will be used ass offset
                if task_offset == 0:
                    task_offset = submit_time
                elif submit_time < task_offset:
                    task_offset = submit_time

                runtime = runtime.iloc[0]
                memory = memory.iloc[0]
                cpu_time = cpu_time.iloc[0] / 1000000

                if cpu_time > runtime:
                    cpu_time = runtime

                task = Task(np.int64(job_id), "Composite", submit_time, 0, runtime, 1, None, workflow_index, -1, "cpu-time",resource=cpu_time, memory_requested=memory)
                task_counter += 1
                tasks_in_workflow.append(task)
                flag = False

        # if flag is true, a task in the workflow is not usable to we skip it
        if flag:
            processed_workflows.append((workflow_index))
            continue

        # compute children of tasks
        final_tasks.extend(compute_children(step_job_ids, tasks_in_workflow))

        workflow_submit_time = int(((datetime.strptime(getattr(wi_row, "create_time"),DATETIME_FORMAT) - EPOCH).total_seconds()) * 1000)

        # find smallest workflow submit time as offset
        if workflow_offset is None:
            workflow_offset = workflow_submit_time
        elif workflow_submit_time < workflow_offset:
             workflow_offset = workflow_submit_time

        workflow = Workflow(workflow_index, workflow_submit_time, tasks_in_workflow, "core", "Engineering",
                            "Galaxy", "Biological Engineering")
        workflow.compute_critical_path()
        processed_workflows.append(workflow_index)
        final_workflows.append(workflow)
        workflow_counter += 1

    # apply offset
    for x in final_tasks:
        x.ts_submit = x.ts_submit - task_offset

    # apply offset
    for y in final_workflows:
        y.ts_submit = y.ts_submit - workflow_offset

    # make tasks dataframe
    task_df = pd.DataFrame([t.get_parquet_dict() for t in final_tasks])

    # create parquet file in specified folder
    os.makedirs(os.path.join(TARGET_DIR, Task.output_path()), exist_ok=True)
    task_df.to_parquet(os.path.join(TARGET_DIR, Task.output_path(), "part.0.parquet"), engine="pyarrow")

    # make workflows dataframe
    workflow_df = pd.DataFrame([w.get_parquet_dict() for w in final_workflows])

    # create parquet file in specified folder
    os.makedirs(os.path.join(TARGET_DIR, Workflow.output_path()), exist_ok=True)
    workflow_df.to_parquet(os.path.join(TARGET_DIR, Workflow.output_path(), "part.0.parquet"), engine="pyarrow")

    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(task_df,
                                                                  domain="Biological Engineering",
                                                                  authors=["Jaro Bosch", "Laurens Versluis"],
                                                                  workload_description="Traces from different biomedical research workflows, executed on the public Galaxy server in Europe."
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

    folder_path = sys.argv[1]
    read_files(folder_path)
    parse()
