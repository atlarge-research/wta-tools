import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import statsmodels.api as sm

import numpy as np


class JobWaitTimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        plt.figure()
        job_arrivals = sorted((self.df["ts_start"] - self.df["ts_submit"]).tolist())

        ecdf = sm.distributions.ECDF(job_arrivals)

        # Change min to 0 to make it start at 0
        x = np.linspace(min(job_arrivals), max(job_arrivals))
        y = ecdf(x)
        plt.step(x, y)

        plt.xlabel('Wait time (ms)', fontsize=18)
        plt.ylabel('P', fontsize=18)

        plt.axes().set_xlim(0, None)
        plt.margins(0.05)
        plt.tight_layout()

        filename = "job_wait_time_cdf_{0}".format(self.workload_name)
        plt.savefig(os.path.join(self.folder, filename), dpi=200)
        if show:
            plt.show()

        return filename
