# This script (wta-tools) is based on the work of Laurens Versluis [@lfdversluis](https://github.com/lfdversluis) and [@JaroAmsterdam](https://github.com/JaroAmsterdam)
# The github repo for the original script: https://github.com/atlarge-research/wta-tools


import json
import os
from collections import OrderedDict
from datetime import datetime

import toposort
from pyspark.sql.types import *


class Workflow(object):
    _version = "1.0"

    def __init__(self, id, ts_submit, tasks, scheduler_description, workflow_domain, workflow_application_name,
                 workflow_appliation_field):
        self.id = id  # The ID of the workflow
        self.ts_submit = ts_submit  # Time of the workflow that entered the system, in milliseconds.
        self.tasks = tasks  # The list of tasks belonging to this workflow
        self.task_count = len(tasks)  # Amount of tasks this workflow comprises
        self.critical_path_length = -1  # The length of the CP of this workflow
        self.critical_path_task_count = -1  # The amount of tasks the CP consists of
        self.max_concurrent_tasks = -1  # The approximate maximum number of tasks that can be run concurrently using Alexey's LoP approximation.
        self.nfrs = dict()  # A dictionary containing workflow-level non-function requirements
        self.scheduler = scheduler_description  # Scheduler name/description, if known
        self.domain = workflow_domain  # The domain the workflow belongs to, e.g. industrial, scientific, etc.
        self.application_name = workflow_application_name  # The name of the application, e.g. Montage, SIPHT
        self.application_field = workflow_appliation_field  # The field of the application, e.g. bioinformatics, astronomy
        # TODO or resource_amount_assgined ?
        self.total_resources = sum(t.resource_amount_requested for t in tasks)  # Total number of resources
        self.total_memory_usage = sum(max(t.memory_requested, 0.0) for t in
                                      tasks)  # Total amount of memory allocated/consumed by all tasks in this workflow

        self.total_network_usage = sum(max(t.network_io_time, 0.0) for t in tasks)  # Total network used by this workflow
        self.total_disk_space_usage = sum(
            max(t.disk_space_requested, 0.0) for t in tasks)  # Total amount of disk space used by this workflow
        self.total_energy_consumption = sum(
            max(t.energy_consumption, 0.0) for t in tasks)  # Total amount of energy consumed by this workflow

    def compute_critical_path(self, strip_colon=False):
        """
        Returns a dict mapping task id to the runtime in seconds, converts datetime objects, otherwise assumes int/float/long.

        stip_colon: If True, it will scan the task id for a colon and uses the part after the colon as ID.
        """

        def get_runtime_map():
            return_dict = dict()
            for task in self.tasks:
                if isinstance(task.runtime, datetime):
                    EPOCH = datetime(1970, 1, 1, tzinfo=task.runtime.tzinfo)
                    runtime_int = int((task.runtime - EPOCH).total_seconds() * 1000)
                else:
                    runtime_int = task.runtime
                return_dict[task.id] = runtime_int
            return return_dict

        def get_submit_map():
            """
            Returns a dict mapping task id to the submit time in seconds, converts datetime objects, otherwise assumes int/float/long.
            """
            return_dict = dict()
            for task in self.tasks:
                if isinstance(task.ts_submit, datetime):
                    EPOCH = datetime(1970, 1, 1, tzinfo=task.ts_submit.tzinfo)
                    ts_submit_int = int((task.ts_submit - EPOCH).total_seconds() * 1000)
                else:
                    ts_submit_int = task.ts_submit
                return_dict[task.id] = ts_submit_int
            return return_dict

        def get_dependencies_map():
            return_dict = dict()
            for task in self.tasks:
                task_id = task.id[str(task.id).find(":") + 1:] if strip_colon else task.id
                return_dict[task_id] = set(
                    [parent[parent.find(":") + 1:] if strip_colon else parent for parent in task.parents])
            return return_dict

        def get_reverse_dependencies_map():
            return_dict = dict()
            for task in self.tasks:
                task_id = task.id[str(task.id).find(":") + 1:] if strip_colon else task.id
                return_dict[task_id] = set(
                    [parent[parent.find(":") + 1:] if strip_colon else parent for parent in task.parents])
            return return_dict

        get_id_from_finish_time = lambda finish_time: list(finish_times.keys())[
            list(finish_times.values()).index(finish_time)]

        id_dependencies_map = get_dependencies_map()

        id_runtime_map = get_runtime_map()
        id_submit_map = get_submit_map()
        sorted_ids = toposort.toposort(id_dependencies_map)
        finish_times = {}
        path_lengths = {}

        # For LoP estimation
        child_reached = {}
        current_wave = []

        for batch_of_ids in sorted_ids:
            for id in batch_of_ids:
                parents = id_dependencies_map[id]
                runtime = id_runtime_map[id]
                submit_time = id_submit_map[id]

                if parents:
                    critical_parent_finish_time = max(
                        finish_times[parent if not strip_colon else parent[str(parent).find(":") + 1:]] for parent in
                        parents)
                    path_lengths[id] = path_lengths[get_id_from_finish_time(critical_parent_finish_time)] + 1

                    for parent in parents:
                        child_reached[parent] = True
                else:
                    critical_parent_finish_time = 0
                    path_lengths[id] = 1

                finish_time = max(critical_parent_finish_time, submit_time) + runtime
                finish_times[id] = finish_time

                new_wave = []
                for node in current_wave:
                    if node not in child_reached:
                        new_wave.append(node)
                new_wave.append(id)
                current_wave = new_wave

                self.max_concurrent_tasks = max(self.max_concurrent_tasks, len(current_wave))

        max_finish_time = max(finish_times.values())
        task_count = path_lengths[get_id_from_finish_time(max_finish_time)]

        # Account for the earliest submit time, substract that from the last ending.
        self.critical_path_length = max_finish_time - min(id_submit_map.values())
        self.critical_path_task_count = task_count

    def compute_critical_path_datetimes(self):
        pass

    def add_task(self, task):
        self.tasks.append(task)
        self.task_count += 1

    def get_json_dict(self):
        return {
            "id": self.id,
            "ts_submit": self.ts_submit,
            "tasks": [task.get_json_dict() for task in self.tasks],
            "task_count": self.task_count,
            "critical_path_length": self.critical_path_length,
            "critical_path_task_count": self.critical_path_task_count,
            "approx_max_concurrent_tasks": self.max_concurrent_tasks,
            "nfrs": self.nfrs,
            "scheduler": self.scheduler,
            "total_resources": self.total_resources,
            "total_memory_usage": self.total_memory_usage,
            "total_network_usage": self.total_network_usage,
            "total_disk_space_usage": self.total_disk_space_usage,
            "total_energy_consumption": self.total_energy_consumption,
            "domain": self.domain,
            "application_name": self.application_name,
            "application_field": self.application_field,
            "version": self._version,
        }

    @staticmethod
    def get_spark_type():
        type_info = StructType([
            StructField("id", LongType(), False),
            StructField("ts_submit", LongType(), False),
            StructField("task_count", IntegerType(), False),
            StructField("critical_path_length", LongType(), False),
            StructField("critical_path_task_count", IntegerType(), False),
            StructField("approx_max_concurrent_tasks", IntegerType(), False),
            StructField("nfrs", StringType(), False),
            StructField("scheduler", StringType(), False),
            StructField("total_resources", DoubleType(), False),
            StructField("total_memory_usage", DoubleType(), False),
            StructField("total_network_usage", LongType(), False),
            StructField("total_disk_space_usage", LongType(), False),
            StructField("total_energy_consumption", LongType(), False),
        ])

        sorted_type_info = sorted(type_info, key=lambda x: x.name)
        return StructType(sorted_type_info)

    @staticmethod
    def get_parquet_meta_dict():
        type_info = {
            "id": int,
            "ts_submit": int,
            "task_count": int,
            "critical_path_length": int,
            "critical_path_task_count": int,
            "approx_max_concurrent_tasks": int,
            "nfrs": str,
            "scheduler": str,
            "total_resources": float,
            "total_memory_usage": float,
            "total_network_usage": float,
            "total_disk_space_usage": float,
            "total_energy_consumption": int,
        }

        ordered_dict = OrderedDict(sorted(type_info.items(), key=lambda t: t[0]))

        return ordered_dict

    def get_parquet_dict(self):
        simple_dict = {
            "id": int(self.id),
            "ts_submit": int(self.ts_submit),
            "task_count": int(self.task_count),
            "critical_path_length": int(self.critical_path_length),
            "critical_path_task_count": int(self.critical_path_task_count),
            "approx_max_concurrent_tasks": int(self.max_concurrent_tasks),
            "nfrs": str(json.dumps(self.nfrs)),
            "scheduler": str(self.scheduler),
            "total_resources": float(self.total_resources),
            "total_memory_usage": float(self.total_memory_usage),
            "total_network_usage": float(self.total_network_usage),
            "total_disk_space_usage": float(self.total_disk_space_usage),
            "total_energy_consumption": int(self.total_energy_consumption),
        }

        ordered_dict = OrderedDict(sorted(simple_dict.items(), key=lambda t: t[0]))

        return ordered_dict

    @staticmethod
    def versioned_dir_name():
        return "schema-{}".format(Workflow._version)

    @staticmethod
    def output_path():
        return os.path.join("workflows", Workflow.versioned_dir_name())
