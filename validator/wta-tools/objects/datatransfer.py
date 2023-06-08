import json
import os
from collections import OrderedDict


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
            "id": int,
            "type": str,
            "ts_submit": int,
            "transfertime": int,
            "source": int,
            "destination": int,
            "size": int,
            "size_unit": str,
            "events": str,
        }

        ordered_dict = OrderedDict(sorted(type_info.items(), key=lambda t: t[0]))

        return ordered_dict

    def get_parquet_dict(self):
        return {
            "id": int(self.id),
            "type": str(self.type),
            "ts_submit": int(self.ts_submit),
            "transfertime": int(self.transfertime),
            "source": int(self.source),
            "destination": int(self.destination),
            "size": int(self.size),
            "size_unit": str(self.size_unit),
            "events": str(json.dumps(self.events)),
        }

    @staticmethod
    def versioned_dir_name():
        return "schema-{}".format(Datatransfer._version)

    @staticmethod
    def output_path():
        return os.path.join("datatransfers", Datatransfer.versioned_dir_name())
