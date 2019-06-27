
class GenericNetworkInformation(object):

    def __init__(self, workload_name, json_workload):
        self.workload_name = workload_name
        self.json_workload = json_workload

    def generate_content(self):
        return {
            "mean-network-usage": self.json_workload["mean_network_usage"],
            "median-network-usage": self.json_workload["median_network_usage"],
            "min-network-usage": self.json_workload["min_network_usage"],
            "max-network-usage": self.json_workload["max_network_usage"],
            "25th-percentile-network-usage": self.json_workload["first_quartile_network_usage"],
            "75th-percentile-network-usage": self.json_workload["third_quartile_network_usage"],
            "std-network-usage": self.json_workload["std_network_usage"],
            "cov-network-usage": self.json_workload["cov_network_usage"]

        }
