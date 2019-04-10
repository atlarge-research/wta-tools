import os

import numpy as np
import pyarrow as pa


class ResourceState(object):
    """
    A resource event depicts the state of a resource in time.
    """
    _version = "1.0"

    def __init__(self, resource_id, timestamp, event_type=None, platform_id=None, available_resources=None,
                 available_memory=None, available_disk_space=None, available_disk_io_bandwidth=None,
                 available_network_bandwidth=None, average_utilization_1_minute=-1, average_utilization_5_minute=-1,
                 average_utilization_15_minute=-1):
        self.resource_id = resource_id  # The ID of the machine
        self.timestamp = timestamp  # The timestamp of the state snapshot
        # An event type associated with the resource, the different types should be described in the description
        # of the workload. Examples are 0: resource booted, 1: resource active, 2: resource shutting down
        self.event_type = event_type
        # The platform the resource belongs to, the meaning can be descriped in the workload description
        self.platform_id = platform_id
        self.available_resources = available_resources  # The number of resources (e.g., cpus, threads, etc.) available
        self.available_memory = available_memory  # Available memory, if applicable/known else -1
        self.available_disk_space = available_disk_space  # Available disk space, if applicable/known else -1
        self.available_disk_io_bandwidth = available_disk_io_bandwidth  # Available disk io bandwidth, if applicable/known else -1
        self.available_network_bandwidth = available_network_bandwidth  # Available network io bandwidth, if applicable/known else -1
        # The average utilization of the resource as fraction over different periods in time
        # For example, a CPU a utilization of 0.5 in the past minute, depicting on average 50% of the CPU
        # (its cores/threads) was being utilized.
        self.average_utilization_1_minute = average_utilization_1_minute
        self.average_utilization_5_minute = average_utilization_5_minute
        self.average_utilization_15_minute = average_utilization_15_minute

    def get_json_dict(self):
        return {
            "resource_id": self.resource_id,
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "platform_id": self.platform_id,
            "available_resources": self.available_resources,
            "available_memory": self.available_memory,
            "available_disk_space": self.available_disk_space,
            "available_disk_io_bandwidth": self.available_disk_io_bandwidth,
            "available_network_bandwidth": self.available_network_bandwidth,
            "average_utilization_1_minute": self.average_utilization_1_minute,
            "average_utilization_5_minute": self.average_utilization_5_minute,
            "average_utilization_15_minute": self.average_utilization_15_minute,
        }

    @staticmethod
    def get_parquet_meta_dict():
        return {
            "resource_id": np.int64,
            "timestamp": np.int64,
            "event_type": np.int64,
            "platform_id": np.int64,
            "available_resources": np.float64,
            "available_memory": np.float64,
            "available_disk_space": np.float64,
            "available_disk_io_bandwidth": np.float64,
            "available_network_bandwidth": np.float64,
            "average_utilization_1_minute": np.float64,
            "average_utilization_5_minute": np.float64,
            "average_utilization_15_minute": np.float64
        }

    def get_parquet_dict(self):
        return {
            "resource_id": np.int64(self.resource_id),
            "timestamp": np.int64(self.timestamp),
            "event_type": np.int64(self.event_type),
            "platform_id": np.int64(self.platform_id),
            "available_resources": np.float64(self.available_resources),
            "available_memory": np.float64(self.available_memory),
            "available_disk_space": np.float64(self.available_disk_space),
            "available_disk_io_bandwidth": np.float64(self.available_disk_io_bandwidth),
            "available_network_bandwidth": np.float64(self.available_network_bandwidth),
            "average_utilization_1_minute": np.float64(self.average_utilization_1_minute),
            "average_utilization_5_minute": np.float64(self.average_utilization_5_minute),
            "average_utilization_15_minute": np.float64(self.average_utilization_15_minute)
        }

    @staticmethod
    def get_pyarrow_schema():
        fields = [
            pa.field("resource_id", pa.int64()),
            pa.field("timestamp", pa.int64()),
            pa.field("event_type", pa.int64()),
            pa.field("platform_id", pa.int64()),
            pa.field("available_resources", pa.float64()),
            pa.field("available_memory", pa.float64()),
            pa.field("available_disk_space", pa.float64()),
            pa.field("available_disk_io_bandwidth", pa.float64()),
            pa.field("available_network_bandwidth", pa.float64()),
            pa.field("average_utilization_1_minute", pa.float64()),
            pa.field("average_utilization_5_minute", pa.float64()),
            pa.field("average_utilization_15_minute", pa.float64())
        ]

        return pa.schema(fields)

    @staticmethod
    def versioned_dir_name():
        return "schema-{}".format(ResourceState._version)

    @staticmethod
    def output_path():
        return os.path.join("resource-states", ResourceState.versioned_dir_name())
