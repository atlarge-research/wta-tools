import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import numpy as np
import statsmodels.api as sm


class TaskRuntimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        plt.figure()
        task_runtimes = sorted(self.df["runtime"].tolist())

        ecdf = sm.distributions.ECDF(task_runtimes)

        # Change min to 0 to make it start at 0
        x = np.linspace(min(task_runtimes), max(task_runtimes))
        y = ecdf(x)
        plt.step(x, y)

        # Rotates and right aligns the x labels, and moves the bottom of the
        # axes up to make room for them
        # fig.autofmt_xdate()
        plt.xlabel('Runtime (s)', fontsize=18)
        plt.ylabel('P', fontsize=18)

        plt.axes().set_xlim(0, None)
        # plt.axes().set_ylim(0, 1)

        # plt.locator_params(nbins=3, axis='y')

        plt.margins(0.05)
        # plt.grid(True)
        plt.tight_layout()

        filename = "task_runtime_cdf_{0}".format(self.workload_name)
        plt.savefig(os.path.join(self.folder, filename), dpi=200)
        if show:
            plt.show()

        return filename
