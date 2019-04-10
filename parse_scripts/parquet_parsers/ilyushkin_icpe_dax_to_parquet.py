#!/usr/bin/env python3.5

"""
Converts the workfload of Ilyushkin et al. (a folder filled with DAX .xml files) into WTA format.
Requires all .xml files to be in the source_dir and required a file called interarrivals.txt in the source_dir
containing the interarrival times per workflow.
Outputs the parquet files to the target_dir.
"""
import json
import os
import re
import sys
from collections import OrderedDict

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from parquet_helper_functions.workflow_characteristics import compute_characteristics
from objects.datatransfer import Datatransfer
from objects.task import Task
from objects.workflow import Workflow
from objects.workload import Workload

import pandas as pd
import os
import xml.etree.ElementTree as ET
import mmh3

import numpy as np

non_decimal = re.compile(r'[^\d]+')

USAGE = 'Usage: python ./ilyushkin_icpe_dax_to_parquet.py directory_with_dax_files path_to_interarrival.txt'
TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', "icpe", 'trace_1')


def read_file(source_dir, dax_file, count, arrival_time, target_dir):
    tree = ET.parse(os.path.join(source_dir, dax_file))
    adag = tree.getroot()

    dependencies = {}

    def id_to_int(id):
        return int(non_decimal.sub('', id))

    for child in adag.findall('{http://pegasus.isi.edu/schema/DAX}child'):
        task_id = mmh3.hash64("workflow:{}_task:{}".format(count, id_to_int(child.attrib['ref'])))[0]

        if task_id not in dependencies:
            dependencies[task_id] = []

        for parent in child:
            parent_id = id_to_int(parent.attrib['ref'])
            dependencies[task_id].append(parent_id)

    tasks = adag.findall('{http://pegasus.isi.edu/schema/DAX}job')
    task_list = []

    inputs_per_taskid = dict()  # Contains input file names per task id
    outputs_per_taskid = dict()  # Contains output file names per task id
    outputs_matched = dict()  # A dictionary to check of outputs have been matched with an input

    input_file_data_per_task_id = dict()  # Dict with file sizes per file name for input files
    output_file_data_per_task_id = dict()  # Dict with file sizes per file name for output files

    task_per_taskid = dict()

    for task in tasks:
        # Since all tasks start at 0, in every file, we add the workflow id and hash it to get task id
        task_id = mmh3.hash64("workflow:{}_task:{}".format(count, id_to_int(task.attrib['id'])))[0]
        # Ilyushkin et al. added an attribute field called profile, containing the runtime of that particular task.
        runtime = int(float(task.attrib['runtime']) * 1000)

        if task_id in dependencies:
            task_dependencies = [mmh3.hash64("workflow:{}_task:{}".format(count, dependency))[0] for dependency in
                                 dependencies[task_id]]
        else:
            task_dependencies = []

        # Site ids are unknown, all tasks in the work to use 1 core.
        t = Task(task_id, "", arrival_time, 0, runtime, 1, task_dependencies, count, -1, resource_type="core",
                 resource=-1)
        task_list.append(t)
        task_per_taskid[task_id] = t

        # Parse the data transfers
        for data_item in task.findall("{http://pegasus.isi.edu/schema/DAX}uses"):
            # Store the incoming and outgoing data to this task in separate dicts
            if data_item.attrib['link'] == "input":
                if task_id not in inputs_per_taskid:
                    inputs_per_taskid[task_id] = set()
                    input_file_data_per_task_id[task_id] = dict()

                inputs_per_taskid[task_id].add(data_item.attrib['file'])
                input_file_data_per_task_id[task_id][data_item.attrib['file']] = data_item.attrib['size']

            elif data_item.attrib['link'] == "output":
                if task_id not in outputs_per_taskid:
                    outputs_per_taskid[task_id] = set()
                    outputs_matched[task_id] = dict()
                    output_file_data_per_task_id[task_id] = dict()
                    output_file_data_per_task_id[task_id] = dict()

                outputs_per_taskid[task_id].add(data_item.attrib['file'])
                outputs_matched[task_id][data_item.attrib['file']] = False
                output_file_data_per_task_id[task_id][data_item.attrib['file']] = data_item.attrib['size']

    # Set children
    for task in task_list:
        for parent_id in task.parents:
            task_per_taskid[parent_id].children.add(task.id)

    data_transfer_id = 0
    # Since tasks can output files with the same name as other tasks, we must loop over a task's parents
    # and match the output names against input names.
    for task in task_list:  # For every task we have
        inputs = inputs_per_taskid[task.id]
        for dep in task.parents:  # We loop over the parents (no need to check children, they will come later)
            outputs = outputs_per_taskid[dep]
            overlap = inputs.intersection(outputs)  # Check for overlap
            if len(overlap) > 0:  # We have input - output pairs, loop over them to construct datatransfers
                for file_name in overlap:
                    # Get the size and construct a datatransfer object.
                    data_size = output_file_data_per_task_id[dep][file_name]
                    datatransfer = Datatransfer(data_transfer_id, "local", -1, -1, dep, task.id, data_size)

                    # Assign it to the tasks
                    task_per_taskid[dep].datatransfers.append(datatransfer)
                    task.datatransfers.append(datatransfer)
                    outputs_matched[dep][file_name] = True

                    # Remove the file from the input as it's covered. Do NOT remove it from output, the same output
                    # file may be used by another task (fan-out structure).
                    inputs.remove(file_name)
                    data_transfer_id += 1

        # Now, loop over the remaining input files. Since we do not have a source, we assume them are present
        # on the filesystem beforehand.

        for file_name in inputs:
            data_size = input_file_data_per_task_id[task.id][file_name]
            datatransfer = Datatransfer(data_transfer_id, "local", -1, -1, -1, task.id, data_size)
            task.datatransfers.append(datatransfer)
            data_transfer_id += 1

    # Loop over the outputs and create a datatransfer for those that are not matched yet
    # These are likely files with final results, not having an destination.
    for task_id in outputs_matched.keys():
        for file_name in outputs_matched[task_id].keys():
            if not outputs_matched[task_id][file_name]:
                task = task_per_taskid[task_id]
                data_size = output_file_data_per_task_id[task_id][file_name]
                datatransfer = Datatransfer(data_transfer_id, "local", -1, -1, -1, task.id, data_size)
                task.datatransfers.append(datatransfer)

    filename_for_this_partition = "part.{}.parquet".format(count)

    os.makedirs(os.path.join(target_dir, Task.output_path()), exist_ok=True)
    task_df = pd.DataFrame([task.get_parquet_dict() for task in task_list])

    # Make sure the first workflow is submitted at time 0
    min_submit_time = task_df["ts_submit"].min()
    task_df = task_df.assign(ts_submit=lambda x: x['ts_submit'] - min_submit_time)

    # Make sure the columns are in the right order
    task_df = task_df[sorted(task_df.columns)]

    task_df.to_parquet(os.path.join(target_dir, Task.output_path(), filename_for_this_partition), engine="pyarrow")
    os.makedirs(os.path.join(target_dir, Datatransfer.output_path()), exist_ok=True)
    datatransfer_df = pd.DataFrame(
        [datatransfer.get_parquet_dict() for task in task_list for datatransfer in task.datatransfers])
    datatransfer_df.to_parquet(os.path.join(target_dir, Datatransfer.output_path(), filename_for_this_partition),
                               engine="pyarrow")

    wf_agnostic_df = compute_characteristics(task_df)
    workflow_ts_submit = task_df["ts_submit"].min()

    application_names = {
        "_lig": ("LIGO", "Physics"),
        "_sip": ("SIPHT", "Bioinformatics"),
        "_mon": ("Montage", "Astronomy"),
    }
    application_name = ""
    application_field = ""
    for key in application_names.keys():
        if key in dax_file:
            application_name = application_names[key][0]
            application_field = application_names[key][1]
            break

    workflow = Workflow(count, workflow_ts_submit, task_list, "", "Scientific", application_name, application_field)
    workflow.compute_critical_path()

    wf_df = pd.concat([wf_agnostic_df, pd.DataFrame(
        {"id": pd.Series([np.int64(count)], dtype=np.int64),
         "ts_submit": pd.Series([np.int64(workflow_ts_submit)], dtype=np.int64),
         "critical_path_length": pd.Series([np.int32(workflow.critical_path_length)], dtype=np.int32),
         "critical_path_task_count": pd.Series([np.int32(workflow.critical_path_task_count)], dtype=np.int32),
         "approx_max_concurrent_tasks": pd.Series([np.int32(workflow.max_concurrent_tasks)], dtype=np.int32),
         "scheduler": pd.Series([np.str("")], dtype=np.str),
         })], axis=1)

    wf_df["nfrs"] = np.str("{}")

    wf_df = wf_df[sorted(wf_df.columns)]

    os.makedirs(os.path.join(target_dir, Workflow.output_path()), exist_ok=True)
    wf_df.to_parquet(os.path.join(target_dir, Workflow.output_path(), filename_for_this_partition), engine="pyarrow")


def parse(source_dir, interarrival_times):
    inter_arrivals = []

    with open(interarrival_times, 'r') as arrival_file:
        for line in arrival_file:
            inter_arrivals.append(int(line))

    dax_files = os.listdir(source_dir)

    files_to_read = []
    arrival_time = 0
    for index, f in enumerate(sorted(dax_files)):
        # Ilyushkin et al. parsed the first two hundred DAX files.
        # The folder should contain 200 files, but one folder contains more than 200 files,
        # of which just the first 200 were used by Ilyushkin et al.
        if index == 200:
            break

        files_to_read.append(read_file(source_dir, f, index, arrival_time, TARGET_DIR))

        arrival_time += inter_arrivals[index]

    meta_dict = Workflow.get_parquet_meta_dict()
    meta_dict["id"] = np.int64  # Add the id because we are not using a grouped dataframe here.
    meta_dict = OrderedDict(sorted(meta_dict.items(), key=lambda t: t[0]))

    wta_tasks = pd.read_parquet(os.path.join(TARGET_DIR, Task.output_path()))

    # Write a json dict with the workload properties
    json_dict = Workload.get_json_dict_from_pandas_task_dataframe(wta_tasks, domain="Scientific",
                                                                authors=["Alexey Ilyushkin", "Ahmed Ali-Eldin",
                                                                         "Nikolas Herbst",
                                                                         "Alessandro Vittorio Papadopoulos",
                                                                         "Bogdan Ghit", "Dick H. J. Epema",
                                                                         "Alexandru Iosup"],
                                                                workload_description="This workload was used in the 2017 ICPE paper titles \"An experimental performance evaluation of autoscaling policies for complex workflows\" by Ilyushkin et al. It features a combination of LIGO, SIPHT, and Montage executes on the DAS5 supercomputer."
                                                                )

    os.makedirs(os.path.join(TARGET_DIR, Workload.output_path()), exist_ok=True)

    with open(os.path.join(TARGET_DIR, Workload.output_path(), "generic_information.json"), "w") as file:
        # Need this on 32-bit python.
        def default(o):
            if isinstance(o, np.int64): return int(o)
            raise TypeError

        file.write(json.dumps(json_dict, default=default))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(USAGE)
        sys.exit(1)

    source_dir = sys.argv[1]
    parse(source_dir, sys.argv[2])
