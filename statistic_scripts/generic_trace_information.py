import ujson


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
            "authors": self.json_workload["authors"],
            "workload_description": self.json_workload["workload_description"],
            "workload_domain": self.json_workload["domain"],
        }


if __name__ == '__main__':
    wl = "../parse_scripts/chronos/chronos.wtf"
    with open(wl, "r") as file:
        wl_data = ujson.load(file)

    gti = GenericTraceInformation("Chronos", wl_data)
    print(gti.generate_content())
