import ujson


class GenericCPUInformation(object):

    def __init__(self, workload_name, json_workload):
        self.workload_name = workload_name
        self.json_workload = json_workload

    def generate_content(self):
        return {
            "mean-cpu-task": self.json_workload["mean_resource_task"],
            "median-cpu-task": self.json_workload["median_resource_task"],
            "min-cpu-task": self.json_workload["min_resource_task"],
            "max-cpu-task": self.json_workload["max_resource_task"],
            "25th-percentile-cpu-task": self.json_workload["first_quartile_resource_task"],
            "75th-percentile-cpu-task": self.json_workload["third_quartile_resource_task"],
            "std-cpu-task": self.json_workload["std_resource_task"],
            "cov-cpu-task": self.json_workload["cov_resource_task"],
        }


if __name__ == '__main__':
    wl = "../parse_scripts/output_parquet/chronos.wtf"
    with open(wl, "r") as file:
        wl_data = ujson.load(file)

    gti = GenericCPUInformation("Chronos", wl_data)
