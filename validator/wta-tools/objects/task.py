import json
import os
from collections import OrderedDict

import pyarrow as pa
from pyspark.sql.types import *


class Task(object):
    _version = "1.0"

    def __init__(self, id, type, ts_submit, submission_site, runtime, resource_amount_requested, parents, workflow_id,
                 wait_time, resource_type="cpu", resource=None, datatransfer=None, params=None, events=None,
                 requirements=None, user_id=-1, group_id=-1, memory_requested=-1, disk_space_requested=-1,
                 disk_io_time=-1, network_io_time=-1, energy_consumption=-1):

        # Do not set these as default parameters in the constructor, they will become class variables if you do.
        if datatransfer is None:
            datatransfer = []
        if requirements is None:
            requirements = dict()
        if params is None:
            params = dict()
        if events is None:
            events = []
        if parents is None:
            parents = list()

        self.id = id  # an ID that identifies this task
        self.type = type  # atomic activity, composite activity (for control structures etc.)
        self.ts_submit = ts_submit  # Time when this task arrived in the system, in milliseconds
        self.submission_site = submission_site  # Site where the task was submitted to, if known.
        self.runtime = max(runtime, 0)  # Runtime in milliseconds
        self.resource_type = resource_type  # The type of resource needed for this task (singular), e.g., "cpu", "core", "gpu", etc.
        self.resource_amount_requested = resource_amount_requested  # Amount of CPUs this task requests / needs
        self.parents = parents  # The parents this task depends on
        self.children = set()  # Tasks that have this task as dependency
        self.user_id = user_id  # User that submitted this task
        self.group_id = group_id  # Group ID of the user, if any
        self.nfrs = requirements  # Non-functional requirements this task contains.
        self.workflow_id = workflow_id  # The ID of the workflow this task belongs to
        self.wait_time = wait_time  # Time spent waiting in the , in milliseconds
        self.params = params  # dict with name:value, if known
        self.memory_requested = memory_requested  # The amount of memory requested by this task
        self.disk_io_time = disk_io_time  # Total time spent on disk io
        self.disk_space_requested = disk_space_requested  # The amount of disk space requested by this task in MB
        self.energy_consumption = energy_consumption  # Amount of energy consumed by this task in Watt
        self.network_io_time = network_io_time  # Total time spent on network io
        self.resource_used = resource  # The resource object associated
        self.datatransfers = datatransfer  # list of data transfers
        self.events = events  # list of task events

    def set_workflow_id_propagating(self, task_dict, workflow_id):
        self.workflow_id = workflow_id
        for child_id in self.children:
            task_dict[child_id].set_workflow_id_propagating(task_dict, workflow_id)

    def get_root_parents(self, task_dict):
        """
        Returns all root parents of this task, i.e. all start tasks.
        :return: A set with all root parents
        :rtype: set of tasks
        """
        if not self.parents:
            return set()
        else:
            return_set = set()
            for parent_id in self.parents:
                parents_of_parents = task_dict[parent_id].get_root_parents(task_dict)
                if parents_of_parents:
                    return_set.update(parents_of_parents)
                else:
                    return_set.add(parent_id)
            return return_set

    def get_json_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "ts_submit": self.ts_submit,
            "submission_site": self.submission_site,
            "runtime": self.runtime,
            "resource_type": self.resource_type,
            "resource_amount_requested": self.resource_amount_requested,
            "parents": list(self.parents),
            "children": list(self.children),
            "user_id": self.user_id,
            "group_id": self.group_id,
            "nfrs": self.nfrs,
            "workflow_id": self.workflow_id,
            "wait_time": self.wait_time,
            "params": self.params,
            "memory_requested": self.memory_requested,
            "network_io_time": self.network_io_time,
            "disk_io_time": self.disk_io_time,
            "disk_space_requested": self.disk_space_requested,
            "energy_consumption": self.energy_consumption,
            "resource_used": self.resource_used,
            "datatransfers": [datatransfer.get_json_dict() for datatransfer in self.datatransfers],
            "events": [event.get_json_dict for event in self.events],
            "version": self._version,
        }

    @staticmethod
    def get_spark_type():
        type_info = [
            StructField("id", LongType(), False),
            StructField("ts_submit", LongType(), False),
            StructField("type", StringType(), False),
            StructField("submission_site", IntegerType(), False),
            StructField("runtime", LongType(), False),
            StructField("resource_type", StringType(), False),
            StructField("resource_amount_requested", DoubleType(), False),
            StructField("parents", ArrayType(LongType()), False),
            StructField("children", ArrayType(LongType()), False),
            StructField("user_id", IntegerType(), False),
            StructField("group_id", IntegerType(), False),
            StructField("nfrs", StringType(), False),
            StructField("workflow_id", LongType(), False),
            StructField("wait_time", LongType(), False),
            StructField("params", StringType(), False),
            StructField("memory_requested", DoubleType(), False),
            StructField("network_io_time", LongType(), False),
            StructField("disk_io_time", LongType(), False),
            StructField("disk_space_requested", DoubleType(), False),
            StructField("energy_consumption", LongType(), False),
            StructField("resource_used", LongType(), False),
        ]

        sorted_type_info = sorted(type_info, key=lambda x: x.name)
        return StructType(sorted_type_info)

    @staticmethod
    def get_parquet_meta_dict():
        type_info = {
            "workflow_id": int,
            "id": int,
            "type": str,
            "ts_submit": int,
            "submission_site": int,
            "runtime": int,
            "resource_type": str,
            "resource_amount_requested": float,
            "parents": object,
            "children": object,
            "user_id": int,
            "group_id": int,
            "nfrs": str,
            "wait_time": int,
            "params": str,
            "memory_requested": float,
            "network_io_time": int,
            "disk_io_time": int,
            "disk_space_requested": float,
            "energy_consumption": int,
            "resource_used": int,
        }

        ordered_dict = OrderedDict(sorted(type_info.items(), key=lambda t: t[0]))

        return ordered_dict

    def get_parquet_dict(self):
        simple_dict = {
            "id": int(self.id),
            "type": str(self.type),
            "ts_submit": int(self.ts_submit),
            "submission_site": int(self.submission_site),
            "runtime": int(self.runtime),
            "resource_type": str(self.resource_type),
            "resource_amount_requested": float(self.resource_amount_requested),
            "parents": list(self.parents),
            "children": list(self.children),
            "user_id": int(self.user_id),
            "group_id": int(self.group_id),
            "nfrs": str(json.dumps(self.nfrs)),
            "workflow_id": int(self.workflow_id),
            "wait_time": int(self.wait_time),
            "params": str(json.dumps(self.params)),
            "memory_requested": float(self.memory_requested),
            "network_io_time": int(self.network_io_time),
            "disk_io_time": int(self.disk_io_time),
            "disk_space_requested": float(self.disk_space_requested),
            "energy_consumption": int(self.energy_consumption),
            "resource_used": int(self.resource_used),
        }

        ordered_dict = OrderedDict(sorted(simple_dict.items(), key=lambda t: t[0]))

        return ordered_dict


    @staticmethod
    def get_pyarrow_schema():
        fields = [
            pa.field('id', pa.int64()),
            pa.field('ts_submit', pa.int64()),
            pa.field('submission_site', pa.int32()),
            pa.field('runtime', pa.int64()),
            pa.field('resource_type', pa.string()),
            pa.field('resource_amount_requested', pa.float64()),
            pa.field('parents', pa.list_(pa.int64())),
            pa.field('children', pa.list_(pa.int64())),
            pa.field('user_id', pa.int32()),
            pa.field('group_id', pa.int32()),
            pa.field('nfrs', pa.string()),
            pa.field('workflow_id', pa.int64()),
            pa.field('wait_time', pa.int64()),
            pa.field('params', pa.string()),
            pa.field('memory_requested', pa.float64()),
            pa.field('network_io_time', pa.int64()),
            pa.field('disk_io_time', pa.int64()),
            pa.field('disk_space_requested', pa.float64()),
            pa.field('energy_consumption', pa.int64()),
            pa.field('resource_used', pa.int64()),
        ]

        return pa.schema(fields)

    @staticmethod
    def versioned_dir_name():
        return "schema-{}".format(Task._version)

    @staticmethod
    def output_path():
        return os.path.join("tasks", Task.versioned_dir_name())
