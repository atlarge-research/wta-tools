"""
Parses a file containing execution traces in the WorkflowHub schema version 1.0 format.
"""
import json
import mmh3
import os
import sys
from collections import OrderedDict
from datetime import datetime

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask import delayed
from dateutil import parser as dateparser

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from parquet_helper_functions.workflow_characteristics import compute_characteristics

from objects.datatransfer import Datatransfer
from objects.resource import Resource
from objects.task import Task
from objects.task_state import TaskState
from objects.workflow import Workflow
from objects.workload import Workload

USAGE = 'Usage: python(3) ./workflowhub_to_parquet.py <path_to_workflowhub_root_directory> <path_to_trace_from_root>'
NAME = 'workflowhub'

TARGET_DIR = None


@delayed
def parse_and_return_task_dataframe(file_path):
    global TARGET_DIR
    with open(file_path) as trace:
        json_data = json.load(trace)

        workflow = json_data['workflow']
        tasks = workflow['jobs']
        machines = workflow['machines']
        date = json_data['createdAt']

        # Convert the ts_submit to seconds instead of a datetime string
        task_date = dateparser.parse(date)
        EPOCH = datetime(1970, 1, 1, tzinfo=task_date.tzinfo)
        ts_submit = int((task_date - EPOCH).total_seconds() * 1000)

        resource_by_id = dict()

        for machine in machines:
            machine_id = mmh3.hash64("machine:{0}".format(machine['machine_code'].strip()))[0]
            machine = machine["machine"]
            num_cpus = machine['cpu']['count']
            details = {
                "cpu_vendor": machine['cpu']['vendor'],
                "architecture": machine['architecture']
            }
            memory_in_gb = int(machine['memory']) / float(1024 * 1024)
            res = Resource(machine_id, "cluster_node", num_cpus, machine['release'], memory_in_gb, -1, -1,
                           machine['system'], details)
            resource_by_id[machine_id] = res

        task_list = []
        task_state_list = []
        inputs_per_taskid = dict()
        outputs_per_taskid = dict()
        outputs_matched = dict()

        task_per_taskid = dict()

        input_file_data_per_task_id = dict()
        output_file_data_per_task_id = dict()
        for task in tasks:
            task_id = mmh3.hash64("task:{}".format(str(task['name']).strip()))[0]
            print(task_id)
            task_files = task['files'] if 'files' in task else []
            task_type = task['type']
            task_cores = task['cores'] if 'cores' in task else 1
            task_memory = task['memory'] if 'memory' in task else -1
            task_runtime = task['runtime'] * 1000 if 'runtime' in task else -1
            task_dependencies = [mmh3.hash64("task:{}".format(str(p).strip()))[0] for p in
                                 task['parents']]
            task_parameters = {"arguments": task['arguments']} if 'arguments' in task else {}
            task_machine = mmh3.hash64("machine:{0}".format(task['machine'].strip()))[0] if 'machine' in task else None
            task_resource = resource_by_id[task_machine].id if 'machine' in task else -1
            # Convert energy to Wh from KWh
            task_total_energy_consumption = float(task['energy']) * 1000 if 'energy' in task else -1

            t = Task(task_id, task_type, ts_submit, -1, task_runtime, task_cores, task_dependencies, 0, -1,
                     params=task_parameters, resource=task_resource, energy_consumption=task_total_energy_consumption,
                     resource_type="core")

            task_per_taskid[task_id] = t
            task_list.append(t)

            # Parse the data transfers
            for file_item in task_files:
                # Apparently not all traces were parsed into version 0.2 despite them being in the
                # folders for 0.2. To this end we need a check for the file name and size fields.
                file_name = file_item['name'] if 'name' in file_item else file_item['fileId']
                file_size = file_item['size'] if 'size' in file_item else -1

                # Store the incoming and outgoing data to this task in separate dicts
                if file_item['link'] == "input":
                    if task_id not in inputs_per_taskid:
                        inputs_per_taskid[task_id] = set()
                        input_file_data_per_task_id[task_id] = dict()
                        inputs_per_taskid[task_id].add(file_name)

                    try:
                        input_file_data_per_task_id[task_id][file_name] = file_size
                    except:
                        print(file_item)
                        exit(-1)

                elif file_item['link'] == "output":
                    if task_id not in outputs_per_taskid:
                        outputs_per_taskid[task_id] = set()
                        outputs_matched[task_id] = dict()
                        output_file_data_per_task_id[task_id] = dict()
                        output_file_data_per_task_id[task_id] = dict()

                    outputs_per_taskid[task_id].add(file_name)
                    outputs_matched[task_id][file_name] = False
                    output_file_data_per_task_id[task_id][file_name] = file_size

            # Create a task state for the entire duration with
            task_state = TaskState(ts_submit, ts_submit + task_runtime, 0, task_id, -1,
                                   canonical_memory_usage=task_memory)
            task_state_list.append(task_state)

        # Make sure the earliest task starts at 0.
        min_ts_submit = min(task.ts_submit for task in task_list)
        for task in task_list:
            # Update the time
            task.ts_submit -= min_ts_submit
            for parent in task.parents:  # Also since we have all parent info, set them in the same loop
                task_per_taskid[parent].children.add(task.id)

        # Offset task states too
        for taskstate in task_state_list:
            taskstate.ts_start -= min_ts_submit
            taskstate.ts_end -= min_ts_submit

        data_transfer_id = 0
        # Since tasks can output files with the same name as other tasks, we must loop over a task's parents
        # and match the output names against input names.
        for task in task_list:  # For every task we have
            if task.id not in inputs_per_taskid: continue
            inputs = inputs_per_taskid[task.id]
            # We loop over the parents (no need to check children, they will come later)
            for dep in task.parents:
                outputs = outputs_per_taskid[dep] if dep in outputs_per_taskid else set()
                overlap = inputs.intersection(outputs)  # Check for overlap
                if len(overlap) > 0:  # We have input-output pairs, loop to construct datatransfers
                    for file_name in overlap:
                        # Get the size and construct a datatransfer object.
                        data_size = output_file_data_per_task_id[dep][file_name]
                        datatransfer = Datatransfer(data_transfer_id, "local", -1, -1, dep, task.id,
                                                    data_size)
                        # Assign it to the tasks
                        task_per_taskid[dep].datatransfers.append(datatransfer)
                        task.datatransfers.append(datatransfer)
                        outputs_matched[dep][file_name] = True

                        # Remove the file from the input as it's covered. Do NOT remove it from output,
                        # the same output file may be used by another task (fan-out structure).
                        inputs.remove(file_name)
                        data_transfer_id += 1

            # Loop over the remaining input files. Since we do not have a source, we assume them are present
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

        filename_for_this_partition = "part.0.parquet"

        # Write all tasks to parquet
        os.makedirs(os.path.join(TARGET_DIR, Task.output_path()), exist_ok=True)
        task_df = pd.DataFrame([task.get_parquet_dict() for task in task_list])
        task_df.to_parquet(os.path.join(TARGET_DIR, Task.output_path(), filename_for_this_partition), engine="pyarrow")

        # Write all task states to parquet
        os.makedirs(os.path.join(TARGET_DIR, TaskState.output_path()), exist_ok=True)
        task_state_df = pd.DataFrame([task_state.get_parquet_dict() for task_state in task_state_list])
        task_state_df.to_parquet(os.path.join(TARGET_DIR, TaskState.output_path(), filename_for_this_partition),
                                 engine="pyarrow")

        # Write all data transfers to parquet
        if any(len(task.datatransfers) for task in task_list):
            os.makedirs(os.path.join(TARGET_DIR, Datatransfer.output_path()), exist_ok=True)
            datatransfer_df = pd.DataFrame(
                [datatransfer.get_parquet_dict() for task_item in task_list for datatransfer in
                 task_item.datatransfers])

            datatransfer_df.to_parquet(
                os.path.join(TARGET_DIR, Datatransfer.output_path(), filename_for_this_partition),
                engine="pyarrow")

        # Write the workflows to parquet
        wf_agnostic_df = compute_characteristics(task_df)
        workflow_ts_submit = task_df["ts_submit"].min()

        # Determine the application name and field
        application_names = {
            "epigenomics": ("Epigenomics", "Bioinformatics"),
            "montage": ("Montage", "Astronomy"),
            "soykb": ("SoyKB", "Bioinformatics"),
        }

        application_name = ""
        application_field = ""
        for key in application_names.keys():
            if key in file_path:
                application_name = application_names[key][0]
                application_field = application_names[key][1]

        workflow = Workflow(0, workflow_ts_submit, task_list, "Pegasus", "Scientific", application_name,
                            application_field)
        workflow.compute_critical_path()

        wf_df = pd.DataFrame([workflow.get_parquet_dict()])

        return wf_df


def parse(root, workflowhub_file_path_from_root):
    path = os.path.join(root, workflowhub_file_path_from_root)

    # Set the target dir to use the same folder structure as WorkflowHub.
    global TARGET_DIR
    TARGET_DIR = os.path.join(os.path.dirname(os.getcwd()), 'output_parquet', 'workflowhub',
                              workflowhub_file_path_from_root.rstrip(".json"))

    with open(path) as json_file:
        json_data = json.load(json_file)
        authors = [json_data['author']['name']]

    meta_dict = Workflow.get_parquet_meta_dict()
    meta_dict["id"] = np.int64  # Add the id because we are not using a grouped dataframe here.
    meta_dict = OrderedDict(sorted(meta_dict.items(), key=lambda t: t[0]))

    workflow_df = dd.from_delayed(parse_and_return_task_dataframe(path), meta=meta_dict)
    workflow_df.to_parquet(os.path.join(TARGET_DIR, Workflow.output_path()),
                           engine="pyarrow",
                           compute=True)

    wta_tasks = dd.read_parquet(os.path.join(TARGET_DIR, Task.output_path()),
                                engine="pyarrow",
                                index=False)

    # Write a json dict with the workload properties
    json_dict = Workload.get_json_dict_from_dask_task_dataframe(wta_tasks, domain="Scientific",
                                                                authors=authors,
                                                                workload_description="Workload downloaded from WorkflowHub, see http://workflowhub.isi.edu/."
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

    parse(sys.argv[1], sys.argv[2])
