import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from statistic_scripts.util import generate_cdf


class JobWaitTimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "job_wait_time_cdf_{0}.png".format(self.workload_name)
        if not os.path.isfile(os.path.join(self.folder, filename)):
            # Add a column so we can compute the CDF based on that
            # Null values should be filtered out, but just in case, set ts_start to -2 billion and ts_submit to -1 billion
            # if they are null, so they will be filtered out.
            self.df.withColumn("wf_wait_time",
                               F.when(F.col("ts_start").isNull(), F.lit(-2000000000)).otherwise(
                                   F.col("ts_start")) - F.when(F.col("ts_submit").isNull(), F.lit(-1000000000)).otherwise(
                                   F.col("ts_submit")))

            generate_cdf(self.df, "wf_wait_time", os.path.join(self.folder, filename),
                         "Wait time{} [ms]", "Num. workflows (CDF)", show)

        return filename


if __name__ == '__main__':
    tasks_loc = "/media/lfdversluis/datastore/SC19-data/parquet-flattened/pegasus_P1_parquet/workflows/schema-1.0"
    spark = (SparkSession.builder
                  .master("local[5]")
                  .appName("WTA Analysis")
                  .config("spark.executor.memory", "3G")
                  .config("spark.driver.memory", "12G")
                  .getOrCreate())

    task_df = spark.read.parquet(tasks_loc)

    gne = JobWaitTimeCDF("test", task_df, ".")
    gne.generate_graphs(show=True)