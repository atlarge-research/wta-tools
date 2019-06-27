import os

import matplotlib.pyplot as plt
import numpy as np
import pyspark.sql.functions as F
import statsmodels.api as sm
import pandas as pd


class TaskMemoryConsumptionCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()
        return None, plot_location

    def generate_graphs(self, show=False):
        filename = "task_memory_consumption_cdf_{0}.png".format(self.workload_name)
        if os.path.isfile(os.path.join(self.folder, filename + ".pdf")):
            return filename

        plt.figure()
        df = self.df.filter(F.col("memory_requested") >= 0)
        if df.count() > 1000:
            permilles = self.df.approxQuantile("memory_requested", [float(i) / 1000 for i in range(0, 1001)], 0.001)
            memory_consumptions = sorted(pd.DataFrame({"memory_requested": permilles})["memory_requested"].tolist())
        else:
            memory_consumptions = sorted(df.toPandas()["memory_requested"].tolist())

        # If no values are found, we cannot generate a CDF.
        if not memory_consumptions:
            plt.text(0.5, 0.5, 'Not available;\nTrace does not contain this information.', horizontalalignment='center',
                     verticalalignment='center', transform=plt.axes().transAxes, fontsize=16)
            plt.grid(False)
        else:
            ecdf = sm.distributions.ECDF(memory_consumptions)

            # Change min to 0 to make it start at 0
            x = np.linspace(min(memory_consumptions), max(memory_consumptions))
            y = ecdf(x)
            plt.step(x, y)

        plt.xlabel('Memory (MB)', fontsize=18)
        plt.ylabel('P', fontsize=18)

        plt.xlim(0)
        plt.margins(0.05)
        plt.tight_layout()

        plt.savefig(os.path.join(self.folder, filename), dpi=200, format='png')
        if show:
            plt.show()

        return filename
