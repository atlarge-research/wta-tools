class GenericEnergyInformation(object):

    def __init__(self, workload_name, json_workload):
        self.workload_name = workload_name
        self.json_workload = json_workload

    def generate_content(self):
        return {
            "mean-energy": self.json_workload["mean_energy"],
            "median-energy": self.json_workload["median_energy"],
            "min-energy": self.json_workload["min_energy"],
            "max-energy": self.json_workload["max_energy"],
            "25th-percentile-energy": self.json_workload["first_quartile_energy"],
            "75th-percentile-energy": self.json_workload["third_quartile_energy"],
            "std-energy": self.json_workload["std_energy"],
            "cov-energy": self.json_workload["cov_energy"]

        }
