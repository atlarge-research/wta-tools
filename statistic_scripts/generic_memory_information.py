class GenericMemoryInformation(object):

    def __init__(self, workload_name, json_workload):
        self.workload_name = workload_name
        self.json_workload = json_workload

    def generate_content(self):
        return {
            "mean-memory": self.json_workload["mean_memory"],
            "median-memory": self.json_workload["median_memory"],
            "min-memory": self.json_workload["min_memory"],
            "max-memory": self.json_workload["max_memory"],
            "25th-percentile-memory": self.json_workload["first_quartile_memory"],
            "75th-percentile-memory": self.json_workload["third_quartile_memory"],
            "std-memory": self.json_workload["std_memory"],
            "cov-memory": self.json_workload["cov_memory"]

        }
