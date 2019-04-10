class GenericTraceInformation(object):

    def __init__(self, workload_name, json_workload):
        self.workload_name = workload_name
        self.json_workload = json_workload

    def generate_content(self):
        return {
            "id": self.workload_name,
            "total-workflows": self.json_workload["total_workflows"],
            "total-tasks": self.json_workload["total_tasks"],
            "num-sites": self.json_workload["num_sites"],
            "num-users": self.json_workload["num_users"],
            "num-cpus": self.json_workload["num_resources"],
            "num-cpu-seconds": self.json_workload["total_resource_seconds"],
            "authors": self.json_workload["authors"]
        }
