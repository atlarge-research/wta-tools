import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os
import statsmodels.api as sm

import numpy as np
from sortedcontainers import SortedList


class TaskCPUTimeCDF(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        plt.figure()
        cpu_times = SortedList()

        for task in self.df.itertuples():
            runtime = int(task.runtime)
            num_cpus = int(task.resource_amount_requested)
            cpu_times.add(runtime * num_cpus)

        ecdf = sm.distributions.ECDF(cpu_times)

        # Change min to 0 to make it start at 0
        x = np.linspace(min(cpu_times), max(cpu_times))
        y = ecdf(x)
        plt.step(x, y)

        plt.ylim(0, None)
        plt.xlim(0)
        plt.xlabel('CPU Time (s)', fontsize=18)
        plt.ylabel('P', fontsize=18)

        plt.margins(0.05)
        plt.tight_layout()

        filename = "task_cpu_time_cdf_{0}".format(self.workload_name)
        plt.savefig(os.path.join(self.folder, filename), dpi=200)
        if show:
            plt.show()

        return filename
