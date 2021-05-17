import math

import matplotlib
from matplotlib.ticker import ScalarFormatter
from pyspark.sql import SparkSession
from sortedcontainers import SortedDict

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import pyspark.sql.functions as F
import pyspark.sql.types as T


class TaskArrivalGraph(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "task_arrival_{0}.png".format(self.workload_name)
        if os.path.isfile(os.path.join(self.folder, filename)):
            return filename

        fig = plt.figure(figsize=(9, 7))
        granularity_order = [
            "Second",
            "Minute",
            "Hour",
            "Day"
        ]

        granularity_lambdas = {
            "Second": 1000,
            "Minute": 60 * 1000,
            "Hour": 60 * 60 * 1000,
            "Day": 60 * 60 * 24 * 1000,
        }

        plot_count = 0

        for granularity in granularity_order:
            task_arrivals = SortedDict()
            df = self.df.withColumn('ts_submit', F.col('ts_submit') / granularity_lambdas[granularity])
            df = df.withColumn('ts_submit', F.col('ts_submit').cast(T.LongType()))
            submit_times = df.groupBy("ts_submit").count().toPandas()

            for task in submit_times.itertuples():
                submit_time = int(task.ts_submit)

                if submit_time not in task_arrivals:
                    task_arrivals[submit_time] = 0

                task_arrivals[submit_time] += task.count

            ax = plt.subplot2grid((2, 2), (int(math.floor(plot_count / 2)), (plot_count % 2)))
            if max(task_arrivals.keys()) >= 1:
                ax.plot(task_arrivals.keys(), task_arrivals.values(), color="black", linewidth=1.0)
                ax.grid(True)
            else:
                ax.text(0.5, 0.5, 'Not available;\nTrace too small.', horizontalalignment='center',
                        verticalalignment='center', transform=ax.transAxes, fontsize=16)
                ax.grid(False)

            # Rotates and right aligns the x labels, and moves the bottom of the
            # axes up to make room for them
            # fig.autofmt_xdate()

            ax.set_xlim(0)
            ax.set_ylim(0)

            ax.locator_params(nbins=3, axis='y')

            ax.margins(0.05)
            ax.tick_params(axis='both', which='major', labelsize=16)
            ax.tick_params(axis='both', which='minor', labelsize=14)

            ax.get_xaxis().get_offset_text().set_visible(False)
            formatter = ScalarFormatter(useMathText=True)
            formatter.set_powerlimits((-4, 5))
            ax.get_xaxis().set_major_formatter(formatter)
            fig.tight_layout()  # Need to set this to be able to get the offset... for whatever reason
            offset_text = ax.get_xaxis().get_major_formatter().get_offset()

            ax.set_xlabel('Time{0} [{1}]'.format(f' {offset_text}' if len(offset_text) else "", granularity.lower()),
                          fontsize=18)
            ax.set_ylabel('Number of Tasks', fontsize=18)

            plot_count += 1

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

    gne = TaskArrivalGraph("test", task_df, ".")
    gne.generate_graphs(show=True)
