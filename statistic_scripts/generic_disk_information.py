class GenericDiskInformation(object):

    def __init__(self, workload_name, json_workload):
        self.workload_name = workload_name
        self.json_workload = json_workload

    def generate_content(self):
        return {
            "mean-disk-space-usage": self.json_workload["mean_disk_space_usage"],
            "median-disk-space-usage": self.json_workload["median_disk_space_usage"],
            "min-disk-space-usage": self.json_workload["min_disk_space_usage"],
            "max-disk-space-usage": self.json_workload["max_disk_space_usage"],
            "25th-percentile-disk-space-usage": self.json_workload["first_quartile_disk_space_usage"],
            "75th-percentile-disk-space-usage": self.json_workload["third_quartile_disk_space_usage"],
            "std-disk-space-usage": self.json_workload["std_disk_space_usage"],
            "cov-disk-space-usage": self.json_workload["cov_disk_space_usage"]

        }
