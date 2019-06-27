import json
import os
from collections import OrderedDict

import numpy as np


class Datatransfer(object):
    """
    The DataTransfer object contains information about datatransfers that happened between tasks.
    """

    _version = "1.0"

    def __init__(self, id, type, ts_start, transfertime, source, destination, size, size_unit="B", events=None):
        if events is None:
            events = dict()

        self.id = id  # An ID that identifies this data transfer
        self.type = type  # Network in/out, local cp
        self.ts_submit = ts_start  # Data transfer start time stamp, in milliseconds
        self.transfertime = max(transfertime, 0)  # Data transfer time in milliseconds
        self.source = source  # The ID of the source
        self.destination = destination  # The ID of the destination
        self.size = size  # The size of transferred data
        self.size_unit = size_unit  # B(yte), KB, MB, GB, TB
        self.events = events  # Dict with event time stamps, such as submitted, active, (pause), done , if known

    def get_json_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "ts_submit": self.ts_submit,
            "transfertime": self.transfertime,
            "source": self.source,
            "destination": self.destination,
            "size": self.size,
            "size_unit": self.size_unit,
            "events": self.events,
            "version": self._version,
        }

    @staticmethod
    def get_parquet_meta_dict():
        type_info = {
            "id": np.int64,
            "type": np.str,
            "ts_submit": np.int64,
            "transfertime": np.int64,
            "source": np.int64,
            "destination": np.int64,
            "size": np.int64,
            "size_unit": np.str,
            "events": np.str,
        }

        ordered_dict = OrderedDict(sorted(type_info.items(), key=lambda t: t[0]))

        return ordered_dict

    def get_parquet_dict(self):
        return {
            "id": np.int64(self.id),
            "type": np.str(self.type),
            "ts_submit": np.int64(self.ts_submit),
            "transfertime": np.int64(self.transfertime),
            "source": np.int64(self.source),
            "destination": np.int64(self.destination),
            "size": np.int64(self.size),
            "size_unit": np.str(self.size_unit),
            "events": np.str(json.dumps(self.events)),
        }

    @staticmethod
    def versioned_dir_name():
        return "schema-{}".format(Datatransfer._version)

    @staticmethod
    def output_path():
        return os.path.join("datatransfers", Datatransfer.versioned_dir_name())
