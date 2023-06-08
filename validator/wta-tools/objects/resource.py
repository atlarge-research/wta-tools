import json
import os

from collections import OrderedDict

from pyspark.sql.types import *


class Resource(object):
    """
    The resource object contains information about the resource a task ran on.
    """
    _version = "1.0"

    def __init__(self, id, type, num_resources, proc_model_name="", memory=-1, disk_space=-1, network_bandwidth=-1,
                 operating_system="", details={}, events={}):

        if events is None:
            events = dict()
        assert (isinstance(events, dict))
        if details is None:
            details = dict()
        self.id = id  # an ID that identifies this resource
        self.type = type  # virtual machine, cluster node, mobile device, gpu, cpu, etc.
        self.num_resources = num_resources  # number of resources, e.g., 1 VM, 8 cores, 32 threads, etc.
        self.proc_model = proc_model_name  # Name of the resource, e.g., Titan X
        self.memory = memory  # Amount of available memory in GB
        self.disk_space = disk_space  # Amount of available disk space in GB
        self.network = network_bandwidth  # Network bandwidth in Gbps
        self.os = operating_system  # Description of the installed operating system
        self.details = details  # String with some further information, which might be resource type specific and not listed above, if known
        self.events = events  # Object that can be used for storing event dicts when parsing to JSON format

    def get_json_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "num_resources": self.num_resources,
            "proc_model": self.proc_model,
            "memory": self.memory,
            "disk_space": self.disk_space,
            "network": self.network,
            "os": self.os,
            "details": json.dumps(self.details),
            "events": json.dumps(self.events),
            "version": self._version,
        }

    @staticmethod
    def get_spark_type():
        type_info = [
            StructField("id", LongType(), False),
            StructField("type", StringType(), False),
            StructField("num_resources", DoubleType(), False),
            StructField("proc_model", StringType(), False),
            StructField("memory", LongType(), False),
            StructField("disk_space", LongType(), False),
            StructField("network", LongType(), False),
            StructField("os", StringType(), False),
            StructField("details", StringType(), False),
        ]

        sorted_type_info = sorted(type_info, key=lambda x: x.name)
        return StructType(sorted_type_info)

    def get_parquet_dict(self):
        simple_dict = {
            "id": int(self.id),
            "type": str(self.type),
            "num_resources": float(self.num_resources),
            "proc_model": str(self.proc_model),
            "memory": int(self.memory),
            "disk_space": int(self.disk_space),
            "network": int(self.network),
            "os": str(self.os),
            "details": str(json.dumps(self.details)),
        }

        ordered_dict = OrderedDict(sorted(simple_dict.items(), key=lambda t: t[0]))

        return ordered_dict

    @staticmethod
    def versioned_dir_name():
        return "schema-{}".format(Resource._version)

    @staticmethod
    def output_path():
        return os.path.join("resources", Resource.versioned_dir_name())
