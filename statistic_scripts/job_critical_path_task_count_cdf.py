import os

from statistic_scripts.util import generate_cdf


class JobCriticalPathTaskCountCDF(object):
    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "job_cp_task_count_{0}.png".format(self.workload_name)
        if not os.path.isfile(os.path.join(self.folder, filename)):
            generate_cdf(self.df, "critical_path_task_count", os.path.join(self.folder, filename),
                         "Num. tasks on critical path {}", "Num. workflows (CDF)", show)

        return filename
