import os

import pyspark.sql.functions as F

from statistic_scripts.util import generate_cdf


class TaskResourceTimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "task_resource_time_cdf_{0}.png".format(self.workload_name)
        if not os.path.isfile(os.path.join(self.folder, filename)):
            self.df = self.df.withColumn("task_resource_computation_time", F.col("resource_amount_requested") * F.col("runtime"))
            generate_cdf(self.df, "task_resource_computation_time", os.path.join(self.folder, filename),
                         "Task Core Time (ms)", "Num. tasks (CDF)", show)

        return filename

