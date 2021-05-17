import math

import matplotlib
from matplotlib.ticker import ScalarFormatter
from pyspark.sql import Window, SparkSession

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
from sortedcontainers import SortedDict
import pyspark.sql.functions as F
import pyspark.sql.types as T


class TaskCompletionGraph(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "task_completion_graph_{0}.png".format(self.workload_name)
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

        plot_number = 0

        self.df = self.df.withColumn('completion_time',
                                     F.col("ts_submit") + F.col("runtime") + F.when((F.col("wait_time") > 0),
                                                                                    F.col("wait_time")).otherwise(0))
        for granularity in granularity_order:
            task_completion_times = SortedDict()

            df = self.df.withColumn('completion_time', F.col('completion_time') / granularity_lambdas[granularity])
            df = df.withColumn('completion_time', F.col('completion_time').cast(T.LongType()))

            if df.count() > 1000:
                # We compute the cumulative sum of amounts of jobs completed per timestamp.
                # Next, we assign row ids per timestamp and based on
                df = df.groupBy("completion_time").count()

                windowval = Window.orderBy('completion_time').rangeBetween(Window.unboundedPreceding, 0)
                df = df.withColumn('cum_sum', F.sum('count').over(windowval))
                window_row_number = Window.orderBy("completion_time")
                df = df.withColumn("row_number", F.row_number().over(window_row_number))

                num_rows = df.count()
                indices = [int(num_rows * (float(i) / 1000)) for i in
                           range(0, 1001)]  # Compute the indices needed to fetch 1000 linearly spaced elements
                completion_times = df.where(df.row_number.isin(indices)).select(
                    ["completion_time", "cum_sum"]).toPandas()
            else:
                df = df.groupBy("completion_time").count()
                windowval = Window.orderBy('completion_time').rangeBetween(Window.unboundedPreceding, 0)
                completion_times = df.withColumn('cum_sum', F.sum('count').over(windowval)).select(
                    ["completion_time", "cum_sum"]).toPandas()

            for task in completion_times.itertuples():
                completion_time = int(task.completion_time)
                count = int(task.cum_sum)

                if completion_time not in task_completion_times:
                    task_completion_times[completion_time] = count

                task_completion_times[completion_time] += count

            ax = plt.subplot2grid((2, 2), (int(math.floor(plot_number / 2)), (plot_number % 2)))

            if max(task_completion_times.keys()) >= 1:
                ax.plot(task_completion_times.keys(), task_completion_times.values(), color="black", linewidth=1.0)
                ax.grid(True)
            else:
                ax.text(0.5, 0.5, 'Not available;\nTrace too small.', horizontalalignment='center',
                        verticalalignment='center', transform=ax.transAxes, fontsize=16)
                ax.grid(False)

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
            ax.set_ylabel('# Tasks', fontsize=18)

            ax.set_xlim(0)
            ax.set_ylim(0)

            ax.margins(0.1)
            plot_number += 1

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

    gne = TaskCompletionGraph("test", task_df, ".")
    gne.generate_graphs(show=True)
