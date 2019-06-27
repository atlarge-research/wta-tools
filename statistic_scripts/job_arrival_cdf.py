import os

from pyspark.sql import SparkSession

from statistic_scripts.util import generate_cdf


class JobArrivalCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "job_arrival_cdf_{0}.png".format(self.workload_name)
        if not os.path.isfile(os.path.join(self.folder, filename)):
            generate_cdf(self.df, "ts_submit", os.path.join(self.folder, filename),
                         "Arrival time (ms)", "Num. workflows (CDF)", show)

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

    gne = JobArrivalCDF("test", task_df, ".")
    gne.generate_graphs(show=True)
