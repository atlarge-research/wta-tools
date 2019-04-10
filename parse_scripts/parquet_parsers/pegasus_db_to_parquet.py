"""
Parses pegasus db files and converts them to WTA format.

"""

import hashlib
import json
import os
import sqlite3
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from objects.resource import Resource
from objects.task import Task
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = 'Usage: python(3) ./pegasus_db_to_parquet.py pegasus-folder'
NAME = 'Pegasus'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', NAME)


def string2numeric_hash(text):
    return int(hashlib.sha256(text.encode('utf-8')).hexdigest()[:15], 16)


def parse_workflows(c, workflow_domain="", workflow_application_field="", workflow_scheduler=""):
    workflows = []
    c.execute("SELECT wf_id,wf_uuid,timestamp,submit_hostname,dax_label FROM workflow")
    for wf_id, wf_uuid, timestamp, submit_hostname, dax_label in c.fetchall():
        tasks = parse_jobs(c, wf_id)
        # id, ts_submit, tasks, scheduler_description
        workflows.append(Workflow(wf_id, timestamp, tasks, workflow_scheduler, workflow_domain, dax_label,
                                  workflow_application_field))
    return workflows


def parse_jobs(c, wf_id):
    child_dict, parent_dict = parse_job_instance_dependencies(c, wf_id)
    resources = parse_resources(c)
    tasks = []
    c.execute(
        "SELECT job_instance_id,job_instance.job_id,host_id,site,user, exec_job_id,submit_file,type_desc,clustered,max_retries,executable,argv,task_count FROM job_instance JOIN job ON job_instance.job_id=job.job_id WHERE job.wf_id=?",
        (str(wf_id),))
    for job_instance_id, job_id, host_id, site, user, exec_job_id, submit_file, type_desc, clustered, max_retries, executable, argv, task_count in c.fetchall():
        events = parse_events_by_job_instance_id(c, job_instance_id)
        # task execution time
        runtime = (events["JOB_TERMINATED"] - events["EXECUTE"]) * 1000
        # waiting time between submission and execution
        waittime = (events["EXECUTE"] - events["SUBMIT"]) * 1000
        # id, type, ts_submit, submission_site, runtime, resource_amount_requested, parents, workflow_id, wait_time, resource_type="cpu", resource=None, datatransfer=None, params=None, events=None, requirements=None, user_id=-1, group_id=-1, memory_requested=-1, disk_space_requested=-1, disk_io_time=-1, network_io_time=-1, energy_consumption=-1
        task = Task(job_instance_id, type_desc, events["SUBMIT"], string2numeric_hash(site),
                    runtime, 1, parent_dict.get(job_instance_id,[]),
                    wf_id, waittime, "CPU",resources.get(host_id, None), events=events, user_id=string2numeric_hash(user))
        task.children = child_dict.get(job_instance_id, [])
        tasks.append(task)
    return tasks


def parse_events_by_job_instance_id(c, job_instance_id):
    events = {}
    c.execute("SELECT state,timestamp FROM jobstate WHERE job_instance_id=?", (str(job_instance_id),))
    for state, timestamp in c.fetchall():
        events[state] = timestamp
    return events


def parse_job_instance_dependencies(c, query_wf_id):
    child_dict = {}
    parent_dict = {}
    c.execute("SELECT * FROM job_edge WHERE wf_id=?", (str(query_wf_id),))
    for wf_id, parent_exec_job_id, child_exec_job_id in c.fetchall():
        parent_job_ids = parse_job_instance_ids_by_exec_job_id(c, parent_exec_job_id, query_wf_id)
        child_job_ids = parse_job_instance_ids_by_exec_job_id(c, child_exec_job_id, query_wf_id)
        for parent_job_id in parent_job_ids:
            child_dict[parent_job_id] = child_dict.get(parent_job_id, []) + child_job_ids
        for child_job_id in child_job_ids:
            parent_dict[child_job_id] = parent_dict.get(child_job_id, []) + parent_job_ids
    return child_dict, parent_dict


def parse_job_instance_ids_by_exec_job_id(c, exec_job_id, query_wf_id):
    job_ids = []
    c.execute(
        "SELECT job_instance_id,exec_job_id  FROM job_instance JOIN job ON job_instance.job_id=job.job_id WHERE exec_job_id=? AND wf_id=?",
        (str(exec_job_id), str(query_wf_id),))
    for job_instance_id, exec_job_id in c.fetchall():
        job_ids.append(job_instance_id)
    return job_ids


def parse_resources(c):
    resources = {}
    c.execute("SELECT * FROM host")
    for host_id, wf_id, site, hostname, ip, uname, total_memory in c.fetchall():
        details = {}
        details["ip"] = ip
        details["hostname"] = hostname
        resources[host_id] = Resource(host_id, site, -1, -1, total_memory, -1, -1, uname, details)
    return resources


def parse_workload(pegasus_db_path, filename=NAME, workload_domain="", workload_description=""):
    if not os.path.exists(TARGET_DIR):
        os.makedirs(TARGET_DIR)

    conn = sqlite3.connect(pegasus_db_path)
    c = conn.cursor()

    workflows = parse_workflows(c)

    for w in workflows:
        w.compute_critical_path()

    # Write the workflow objects to parquet
    os.makedirs(os.path.join(TARGET_DIR, Workflow.output_path()), exist_ok=True)
    workflow_df = pd.DataFrame([workflow.get_parquet_dict() for workflow in workflows])
    workflow_df.to_parquet(os.path.join(TARGET_DIR, Workflow.output_path(), "part.0.parquet"), engine="pyarrow")

    # Write all tasks to parquet
    os.makedirs(os.path.join(TARGET_DIR, Task.output_path()), exist_ok=True)
    task_df = pd.DataFrame([task.get_parquet_dict() for wf in workflows for task in wf.tasks])
    # Make sure the first workflow is submitted at time 0
    min_submit_time = task_df["ts_submit"].min()
    task_df = task_df.assign(ts_submit=lambda x: x['ts_submit'] - min_submit_time)

    pyarrow_task_schema = Task.get_pyarrow_schema()
    table = pa.Table.from_pandas(task_df, schema=pyarrow_task_schema, preserve_index=False)

    # Pandas does not know the different between an empty list and a list with integers
    # Thus, type mismatches will occur. We are writing the task tables using pyarrow directly
    # using a schema.
    pq.write_table(table, os.path.join(TARGET_DIR, Task.output_path(), "part.0.parquet"))

    # generate workload description
    authors_list = []

    w = Workload(workflows, workload_domain, authors_list, workload_description)

    # Write a json dict with the workload properties
    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(task_df,
                                                                  domain="Scientific",
                                                                  authors=["Pegasus Team"],
                                                                  workload_description=""  # TODO fill in
                                                                  )

    os.makedirs(os.path.join(TARGET_DIR, Workload.output_path()), exist_ok=True)

    with open(os.path.join(TARGET_DIR, Workload.output_path(), "generic_information.json"), "w") as file:
        # Need this on 32-bit python.
        def default(o):
            if isinstance(o, np.int64): return int(o)
            raise TypeError

        file.write(json.dumps(json_dict, default=default))

    conn.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    parse_workload(sys.argv[1])
