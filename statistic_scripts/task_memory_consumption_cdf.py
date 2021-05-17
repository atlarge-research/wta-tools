import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import statsmodels.api as sm
from matplotlib.ticker import ScalarFormatter


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

        fig, ax = plt.subplots()
        df = self.df.filter(F.col("memory_requested") >= 0)
        if df.count() > 1000:
            permilles = self.df.approxQuantile("memory_requested", [float(i) / 1000 for i in range(0, 1001)], 0.001)
            memory_consumptions = sorted(pd.DataFrame({"memory_requested": permilles})["memory_requested"].tolist())
        else:
            memory_consumptions = sorted(df.toPandas()["memory_requested"].tolist())

        # If no values are found, we cannot generate a CDF.
        if not memory_consumptions:
            fig.text(0.5, 0.5, 'Not available;\nTrace does not contain this information.', horizontalalignment='center',
                     verticalalignment='center', transform=plt.axes().transAxes, fontsize=16)
            ax.grid(False)
        else:
            ecdf = sm.distributions.ECDF(memory_consumptions)

            # Change min to 0 to make it start at 0
            x = np.linspace(min(memory_consumptions), max(memory_consumptions))
            y = ecdf(x)
            fig.step(x, y)

        ax.set_xlim(0,None)
        ax.margins(0.05)

        ax.tick_params(axis='both', which='major', labelsize=16)
        ax.tick_params(axis='both', which='minor', labelsize=14)

        ax.get_xaxis().get_offset_text().set_visible(False)
        formatter = ScalarFormatter(useMathText=True)
        formatter.set_powerlimits((-4, 5))
        ax.get_xaxis().set_major_formatter(formatter)
        fig.tight_layout()  # Need to set this to be able to get the offset... for whatever reason
        offset_text = ax.get_xaxis().get_major_formatter().get_offset()

        ax.set_xlabel('Memory{} [B]'.format(f' {offset_text}' if len(offset_text) else ""), fontsize=18)
        ax.set_ylabel('Number of Tasks [CDF]', fontsize=18)

        fig.tight_layout()
        fig.savefig(os.path.join(self.folder, filename), dpi=600, format='png')
        if show:
            fig.show()

        return filename
