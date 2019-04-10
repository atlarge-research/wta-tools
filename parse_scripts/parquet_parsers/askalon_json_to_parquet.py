#!/usr/bin/env python3.6
"""
Created on Feb 20 2019
"""

import json
import hashlib
import os
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dateutil.parser import parse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from objects.resource import Resource
from objects.datatransfer import Datatransfer
from objects.task import Task
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = 'Usage: python(3) ./askalon_json_to_parquet.py askalon_file(.json)'
NAME = 'Askalon_new'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', NAME)

def string2numeric_hash(text):
    return int(hashlib.sha256(text.encode('utf-8')).hexdigest()[:15], 16)

def parse_events(ev):
    events = {}
    for e in ev:
        events[e['state']] = e['time']
    return events


def parse_params(par):
    params = {}
    for p in par:
        params[p['name']] = p['value']
    return params


def parse_datatransfers(dt):
    datatransfers = []
    for ft in dt:  # parse file transfers
        transfertime = int((parse(ft['endTime']) - parse(ft['startTime'])).total_seconds() * 1000)
        # id, type, ts_submit, transfertime, source, destination, size, size_unit="B", events={}
        datatransfers.append(
            Datatransfer(ft['name'], ft['type'], int(parse(ft['startTime']).timestamp() * 1000), transfertime, ft['source'], ft['destination'],
                         ft['size'].replace('B', ''), "B", parse_events(ft['events'])))
    return datatransfers


def parse_workflow(wf, filename):
    workflow_id = string2numeric_hash(wf['name'] + '-(' + wf['id'] + ')')
    workflow_domain = ""  # The domain the workflow belongs to, e.g. industry, science, etc.
    workflow_application_name = ""  # The name of the application, e.g. Montage, SIPHT
    workflow_appliation_field = ""  # The field of the application, e.g. bioinformatics, astronomy
    if "bwa" in filename.lower():
        workflow_id = string2numeric_hash("bwa" + '-(' + wf['id'] + ')')
        workflow_domain = "science"
        workflow_application_name = "Burroughs-Wheeler Alignment tool"
        workflow_appliation_field = "bioinformatics"
    elif "wien2k" in filename.lower():
        workflow_id = string2numeric_hash("wien2k" + '-(' + wf['id'] + ')')
        workflow_domain = "science"
        workflow_application_name = "Wien2k"
        workflow_appliation_field = "materials chemistry"
    resources = {}
    for r in wf['resources']:  # parse resources for tasks later
        r_details = r['details']
        os = "Linux"
        details = {}
        details['provider'] = r_details['provider']
        details['instanceType'] = r_details['instanceType']
        events = parse_events(r['events'])
        # id, type, num_resources, proc_model_name, memory, disk_space, network_bandwidth, operating_system, details=None, events=None
        resources[string2numeric_hash(r['id'])] = Resource(string2numeric_hash(r['name']), r['type'], r_details['vCPUs'], r_details['physicalProcessor'],
                                      r_details['memory'], r_details['storage'], r_details['networkPerformance'], os,
                                      details, events)
    # create list of tasks for parents                                
    if "wien2k" in filename.lower():
        first = []
        second = []
        third = []
        fourth = []
        last = []
        for t in wf['tasks']:
            if "first" in t['name'].lower():
                first.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "second" in t['name'].lower():
                second.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "third" in t['name'].lower():
                third.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "fourth" in t['name'].lower():
                fourth.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "last" in t['name'].lower():
                last.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
    elif "bwa" in filename.lower():
        bwaindex_split1_2 = []
        bwa1aln = []
        bwaconcat = []
        for t in wf['tasks']:
            if "bwa:bwaindex" in t['name'].lower():
                bwaindex_split1_2.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "bwa:split1" in t['name'].lower():
                bwaindex_split1_2.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "bwa:split2" in t['name'].lower():
                bwaindex_split1_2.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "bwa:bwa1aln" in t['name'].lower():
                bwa1aln.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
            if "bwa:concat" in t['name'].lower():
                bwaconcat.append(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')))
    tasks = []
    for t in wf['tasks']:  # parse tasks
        if "cloud init" not in t['name'].lower() and "cloud instances" not in t[
            'name'].lower() and "parforiteration" not in t['type'].lower() and "parallel" not in t[
            'type'].lower() and "section" not in t['type'].lower():
            parents = []
            if "wien2k" in filename.lower():
                if "second" in t['name'].lower():
                    parents = first
                if "third" in t['name'].lower():
                    parents = second
                if "fourth" in t['name'].lower():
                    parents = third
                if "last" in t['name'].lower():
                    parents = fourth
            elif "bwa" in filename.lower():
                if "bwa:bwa1aln" in t['name'].lower():
                    parents = bwaindex_split1_2
                if "bwa:concat" in t['name'].lower():
                    parents = bwa1aln
            # print(parents)
            submission_site = string2numeric_hash(t['submissionSite'])
            res = None
            if submission_site != '':
                res = resources[submission_site]
            params = parse_params(t['params'])
            events = parse_events(t['events'])
            wait_time = 0
            if "ACTIVE" in events:
                wait_time = int((parse(events["ACTIVE"]) - parse(t['startTime'])).total_seconds() * 1000)
            # id, type, ts_submit, 
            # submission_site, runtime, resource_amount_requested, parents, 
            # workflow_id, wait_time, resource_type="cpu", resource=None, datatransfer=None, params=None, events=None, requirements=None, user_id=-1, group_id=-1, memory_requested=-1, disk_space_requested=-1, disk_io_time=-1, network_io_time=-1, energy_consumption=-1
            tasks.append(
                Task(string2numeric_hash(str(t['name'] + '-(' + t['id'] + ')')), t['type'], int(parse(t['startTime']).timestamp() * 1000),
                     submission_site, int((parse(t['endTime']) - parse(t['startTime'])).total_seconds() * 1000), 1, parents,
                     workflow_id, wait_time, "CPU" , res, parse_datatransfers(t['fileTransfers']), params,
                     events))
    ts_start = min(t['startTime'] for t in wf['tasks'])
    # id, ts_submit, tasks, scheduler_description
    return Workflow(workflow_id, int(parse(wf['beginTime']).timestamp() * 1000), tasks, wf['scheduler'], workflow_domain, workflow_application_name,
                    workflow_appliation_field)


def parse_askalon_file(askalon_file):
    if not os.path.exists(TARGET_DIR):
        os.makedirs(TARGET_DIR)

    workflows = []
    with open(askalon_file, 'r') as asklon_trace:
        data = json.load(asklon_trace)
        for wf in data:
            workflows.append(parse_workflow(wf, askalon_file))

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
    authors_list = ["Roland Matha", "Radu Prodan"]

    # generate workload description
    workload_description = ""
    if "bwa" in askalon_file.lower():
        workload_description = "BWA (short for Burroughs-Wheeler Alignment tool) is a genomics analysis workflow, courtesy of Scott Emrich and Notre Dame Bioinformatics Laboratory. It maps low-divergent sequences against a large reference genome, such as the human genome."
    elif "wien2k" in askalon_file.lower():
        workload_description = "Wien2k uses a full-potential Linearized Augmented Plane Wave (LAPW) approach for the computation of crystalline solids."

    workload_domain = "Scientific"

    w = Workload(workflows, workload_domain, authors_list, workload_description)

    # Write a json dict with the workload properties
    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(task_df,
                                                                  domain=workload_domain,
                                                                  authors=authors_list,
                                                                  workload_description=workload_description 
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
    parse_askalon_file(sys.argv[1])
