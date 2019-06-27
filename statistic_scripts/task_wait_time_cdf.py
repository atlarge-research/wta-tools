import os

import matplotlib
from pyspark.sql import SparkSession

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pyspark.sql.functions as F


import numpy as np
import statsmodels.api as sm
import pandas as pd


class TaskWaitTimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "task_wait_time_cdf_{0}.png".format(self.workload_name)
        if os.path.isfile(os.path.join(self.folder, filename)):
            return filename

        plt.figure()
        df = self.df.filter(F.col("wait_time") >= 0)
        if df.count() > 1000:
            permilles = self.df.approxQuantile("wait_time", [float(i) / 1000 for i in range(0, 1001)], 0.001)
            task_wait_times = sorted(pd.DataFrame({"wait_time": permilles})["wait_time"].tolist())
        else:
            task_wait_times = sorted(df.toPandas()["wait_time"].tolist())

        if len(task_wait_times) == 0 or max(task_wait_times) == -1:
            plt.text(0.5, 0.5, 'Not available;\nTrace does not contain this information.', horizontalalignment='center',
                     verticalalignment='center', transform=plt.axes().transAxes, fontsize=16)
            plt.grid(False)
        else:
            ecdf = sm.distributions.ECDF(task_wait_times)

            # Change min to 0 to make it start at 0
            x = np.linspace(min(task_wait_times), max(task_wait_times))
            y = ecdf(x)
            plt.step(x, y)

        plt.xlabel('Wait time (s)', fontsize=18)
        plt.ylabel('P', fontsize=18)

        plt.axes().set_xlim(0, None)
        plt.margins(0.05)
        plt.tight_layout()

        plt.savefig(os.path.join(self.folder, filename), dpi=200, format='png')
        if show:
            plt.show()

        return filename


if __name__ == '__main__':
    tasks_loc = "/media/lfdversluis/datastore/SC19-data/parquet-flattened/pegasus_P1_parquet/tasks/schema-1.0"
    spark = (SparkSession.builder
                  .master("local[5]")
                  .appName("WTA Analysis")
                  .config("spark.executor.memory", "3G")
                  .config("spark.driver.memory", "12G")
                  .getOrCreate())

    task_df = spark.read.parquet(tasks_loc)

    gne = TaskWaitTimeCDF("test", task_df, ".")
    gne.generate_graphs(show=True)
