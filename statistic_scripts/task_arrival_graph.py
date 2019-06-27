import math

import matplotlib
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

        plt.figure()
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
                        verticalalignment = 'center', transform = ax.transAxes, fontsize=16)
                ax.grid(False)

            # Rotates and right aligns the x labels, and moves the bottom of the
            # axes up to make room for them
            # fig.autofmt_xdate()
            ax.set_xlabel('Time ({0})'.format(granularity.lower()), fontsize=16)
            ax.set_ylabel('# Tasks', fontsize=16)

            ax.set_xlim(0)
            ax.set_ylim(0)

            ax.locator_params(nbins=3, axis='y')

            ax.margins(0.05)
            plot_count += 1

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

    gne = TaskArrivalGraph("test", task_df, ".")
    gne.generate_graphs(show=True)
