import os

import matplotlib
from matplotlib.ticker import ScalarFormatter
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

        fig, ax = plt.subplots()
        df = self.df.filter(F.col("wait_time") >= 0)
        if df.count() > 1000:
            permilles = self.df.approxQuantile("wait_time", [float(i) / 1000 for i in range(0, 1001)], 0.001)
            task_wait_times = sorted(pd.DataFrame({"wait_time": permilles})["wait_time"].tolist())
        else:
            task_wait_times = sorted(df.toPandas()["wait_time"].tolist())

        if len(task_wait_times) == 0 or max(task_wait_times) == -1:
            fig.text(0.5, 0.5, 'Not available;\nTrace does not contain this information.', horizontalalignment='center',
                     verticalalignment='center', transform=plt.axes().transAxes, fontsize=16)
            ax.grid(False)
        else:
            ecdf = sm.distributions.ECDF(task_wait_times)

            # Change min to 0 to make it start at 0
            x = np.linspace(min(task_wait_times), max(task_wait_times))
            y = ecdf(x)
            fig.step(x, y)

        ax.tick_params(axis='both', which='major', labelsize=16)
        ax.tick_params(axis='both', which='minor', labelsize=14)

        ax.get_xaxis().get_offset_text().set_visible(False)
        formatter = ScalarFormatter(useMathText=True)
        formatter.set_powerlimits((-4, 5))
        ax.get_xaxis().set_major_formatter(formatter)
        fig.tight_layout()  # Need to set this to be able to get the offset... for whatever reason
        offset_text = ax.get_xaxis().get_major_formatter().get_offset()

        ax.set_xlabel('Wait Time{} [s]'.format(f' {offset_text}' if len(offset_text) else ""), fontsize=18)
        ax.set_ylabel('Number of Tasks [CDF]', fontsize=18)

        ax.set_xlim(0, None)
        ax.margins(0.05)
        fig.tight_layout()

        fig.savefig(os.path.join(self.folder, filename), dpi=600, format='png')
        if show:
            fig.show()

        return filename


if __name__ == '__main__':
    tasks_loc = "C:/Users/L/Downloads/Galaxy/tasks/schema-1.0"
    spark = (SparkSession.builder
                  .master("local[5]")
                  .appName("WTA Analysis")
                  .config("spark.executor.memory", "3G")
                  .config("spark.driver.memory", "12G")
                  .getOrCreate())

    task_df = spark.read.parquet(tasks_loc)

    gne = TaskWaitTimeCDF("test", task_df, ".")
    gne.generate_graphs(show=True)
