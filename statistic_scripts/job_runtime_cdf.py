import os

from statistic_scripts.util import generate_cdf


class JobRuntimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "job_runtime_cdf_{0}.png".format(self.workload_name)
        if not os.path.isfile(os.path.join(self.folder, filename)):
            generate_cdf(self.df, "critical_path_length", os.path.join(self.folder, filename),
                         "Critical Path{} [ms]", "Num. workflows (CDF)", show)

        return filename
