import math

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

import numpy as np
from sortedcontainers import SortedDict


class TaskCompletionGraph(object):

    def __init__(self, workload_name, df, image_folder_location):
        self.workload_name = workload_name
        self.df = df
        self.folder = image_folder_location

    def generate_content(self):
        plot_location = self.generate_graphs()

        return None, plot_location

    def generate_graphs(self, show=False):
        plt.figure()
        granularity_order = [
            "Second",
            "Minute",
            "Hour",
            "Day"
        ]

        granularity_lambdas = {
            "Second": lambda x: x,
            "Minute": lambda x: x / 60,
            "Hour": lambda x: x / (60 * 60),
            "Day": lambda x: x / (60 * 60 * 24),
        }

        plot_number = 0
        for granularity in granularity_order:
            task_completion_times = SortedDict()

            for task in self.df.itertuples():
                submit_time = int(task.ts_submit)
                runtime = int(task.runtime)
                wait_time = int(task.wait_time)

                completion_time = submit_time + runtime

                if wait_time > 0:
                    completion_time += wait_time

                completion_time = granularity_lambdas[granularity](completion_time)

                if completion_time not in task_completion_times:
                    task_completion_times[completion_time] = 0

                task_completion_times[completion_time] += 1

            cummulative_times = np.cumsum(task_completion_times.values())
            ax = plt.subplot2grid((2, 2), (int(math.floor(plot_number / 2)), (plot_number % 2)))

            if max(task_completion_times.keys()) >= 1:
                ax.plot(task_completion_times.keys(), cummulative_times, color="black", linewidth=1.0)
                ax.grid(True)
            else:
                ax.text(0.5, 0.5, 'Not available;\nTrace too small.', horizontalalignment='center',
                verticalalignment = 'center', transform = ax.transAxes, fontsize=16)
                ax.grid(False)

            # Rotates and right aligns the x labels, and moves the bottom of the
            # axes up to make room for them
            ax.set_xlabel('Time ({0})'.format(granularity.lower()), fontsize=14)
            ax.set_ylabel('# Tasks', fontsize=14)

            ax.set_xlim(0)
            ax.set_ylim(0)

            ax.margins(0.1)
            plot_number += 1

        plt.tight_layout()
        filename = "task_completion_graph_{0}".format(self.workload_name)
        plt.savefig(os.path.join(self.folder, filename), dpi=200)
        if show:
            plt.show()

        return filename
