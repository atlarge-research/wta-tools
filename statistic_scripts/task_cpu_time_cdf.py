import matplotlib
from matplotlib.ticker import ScalarFormatter

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
        fig, ax = plt.subplots()
        cpu_times = SortedList()

        for task in self.df.itertuples():
            runtime = int(task.runtime)
            num_cpus = int(task.resource_amount_requested)
            cpu_times.add(runtime * num_cpus)

        ecdf = sm.distributions.ECDF(cpu_times)

        # Change min to 0 to make it start at 0
        x = np.linspace(min(cpu_times), max(cpu_times))
        y = ecdf(x)
        fig.step(x, y)

        ax.set_ylim(0, None)
        ax.set_xlime(0, None)

        ax.tick_params(axis='both', which='major', labelsize=16)
        ax.tick_params(axis='both', which='minor', labelsize=14)

        ax.get_xaxis().get_offset_text().set_visible(False)
        formatter = ScalarFormatter(useMathText=True)
        formatter.set_powerlimits((-4, 5))
        ax.get_xaxis().set_major_formatter(formatter)
        fig.tight_layout()  # Need to set this to be able to get the offset... for whatever reason
        offset_text = ax.get_xaxis().get_major_formatter().get_offset()

        ax.set_xlabel('CPU Time{} [s]'.format(f' {offset_text}' if len(offset_text) else ""), fontsize=18)
        ax.set_ylabel('Number of Tasks [CDF]', fontsize=18)

        ax.margins(0.05)
        fig.tight_layout()

        filename = "task_cpu_time_cdf_{0}".format(self.workload_name)
        fig.savefig(os.path.join(self.folder, filename), dpi=600, format='png')
        if show:
            fig.show()

        return filename
