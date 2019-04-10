import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

import numpy as np
import statsmodels.api as sm


class TaskWaitTimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        plt.figure()
        task_wait_times = sorted(self.df["wait_time"].tolist())

        if max(task_wait_times) == -1:
            plt.text(0.5, 0.5, 'Not available;\nTrace does not contain this information.', horizontalalignment='center',
                    verticalalignment='center', transform=plt.axes().transAxes, fontsize=16)
            plt.grid(False)
        else:
            ecdf = sm.distributions.ECDF(task_wait_times)

            # Change min to 0 to make it start at 0
            x = np.linspace(min(task_wait_times), max(task_wait_times))
            y = ecdf(x)
            plt.step(x, y)

        plt.xlabel('Wait time (s)', fontsize=18)
        plt.ylabel('P', fontsize=18)

        plt.axes().set_xlim(0, None)
        plt.margins(0.05)
        plt.tight_layout()

        filename = "task_wait_time_cdf_{0}".format(self.workload_name)
        plt.savefig(os.path.join(self.folder, filename), dpi=200)
        if show:
            plt.show()

        return filename
