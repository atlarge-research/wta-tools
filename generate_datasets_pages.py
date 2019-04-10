"""
Parses a directory with folders containing parquet data in the workflow trace format.
The <dir_with_traces> should be the directory containing the traces.
The <output_dir> is the root directory of the WTA website.

Usage:
  generate_datasets_pages.py <dir_with_traces> <output_dir>

Options:
  -h --help                         Show this screen.
"""
import glob
import json
import os
import pandas as pd

from docopt import docopt
import yaml
import matplotlib.pyplot as plt

from statistic_scripts.generic_cpu_information import GenericCPUInformation
from statistic_scripts.generic_memory_information import GenericMemoryInformation
from statistic_scripts.generic_trace_information import GenericTraceInformation
from statistic_scripts.job_arrival_cdf import JobArrivalCDF
from statistic_scripts.job_arrival_graph import JobArrivalGraph
from statistic_scripts.job_critical_path_task_count_cdf import JobCriticalPathTaskCountCDF
from statistic_scripts.job_runtime_cdf import JobRuntimeCDF
from statistic_scripts.job_wait_time_cdf import JobWaitTimeCDF
from statistic_scripts.task_arrival_cdf import TaskArrivalCDF
from statistic_scripts.task_arrival_graph import TaskArrivalGraph
from statistic_scripts.task_completion_graph import TaskCompletionGraph
from statistic_scripts.task_cpu_time_cdf import TaskCPUTimeCDF
from statistic_scripts.task_memory_consumption_cdf import TaskMemoryConsumptionCDF
from statistic_scripts.task_runtime_cdf import TaskRuntimeCDF
from statistic_scripts.task_wait_time_cdf import TaskWaitTimeCDF


class TraceParser:
    generic_statistic_scripts = [
        ("generic-info", GenericTraceInformation),
        ("generic-cpu", GenericCPUInformation),
        ("generic-memory", GenericMemoryInformation),
    ]

    plot_workflow_statistic_scripts = [
        ("job-arrival", JobArrivalGraph),
        ("job-arrival-cdf", JobArrivalCDF),
        ("job-cp-count-cdf", JobCriticalPathTaskCountCDF),
        ("job-runtime-cdf", JobRuntimeCDF),
        # ("job-waitime-cdf", JobWaitTimeCDF),
    ]
    plot_task_statistics_scripts = [
        ("task-arrival", TaskArrivalGraph),
        ("task-arrival-cdf", TaskArrivalCDF),
        ("task-completion", TaskCompletionGraph),
        ("task-cputime-cdf", TaskCPUTimeCDF),
        ("task-memory-cdf", TaskMemoryConsumptionCDF),
        ("task-runtime-cdf", TaskRuntimeCDF),
        ("task-waittime-cdf", TaskWaitTimeCDF),
    ]

    def __init__(self, trace_dir, output_dir):
        self.trace_dir = trace_dir
        self.output_dir = output_dir

        if not os.path.exists(trace_dir):
            print("Trace directory {0} does not exist!".format(trace_dir))
            exit(-1)

        if not os.path.isdir(output_dir):
            print("Output directory {0} does not exist, creating it...".format(output_dir))
            os.mkdir(output_dir)

        if not os.path.isdir(os.path.join(self.output_dir, "wta-data", "images", "graphs")):
            print("Creating data dirs in the output dir...")
            os.makedirs(os.path.join(self.output_dir, "wta-data", "images", "graphs"), exist_ok=True)

        graph_dir = os.path.join(self.output_dir, "wta-data", "images", "graphs")
        if not os.path.isdir(graph_dir):
            print("Output directory {0} does not exist, creating it...".format(graph_dir))
            os.mkdir(graph_dir)

        print("Cleaning the graph data folder...")
        for the_file in os.listdir(graph_dir):
            file_path = os.path.join(graph_dir, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)

        traces_yml_location = os.path.join(self.output_dir, "wta-data", "traces.yml")
        if os.path.exists(traces_yml_location):
            print("Deleting {0} to generate a new one...".format(traces_yml_location))
            os.remove(traces_yml_location)

        print("Parsing traces...")
        self.parse_wta_traces()

    def parse_wta_traces(self):

        trace_dicts = []

        for file in glob.iglob(os.path.join(self.trace_dir, '*')):
            if file.startswith("alibaba"): continue  # TODO parse Alibaba (and others?) with Spark.
            if os.path.isdir(file):
                file_name, trace_dict = self.parse_trace(file)
                trace_dicts.append(trace_dict)

        with open(os.path.join(self.output_dir, "wta-data", "traces.yml"), "w+") as trace_info_file:
            trace_info_file.write(yaml.safe_dump(trace_dicts, default_flow_style=False))

    def parse_trace(self, file_path):
        # Get the file name without the extension
        file_name = os.path.basename(file_path).split(".")[0]

        trace_yaml_dict = {}
        trace_yaml_dict['id'] = file_name

        # Load the file's JSON contents
        with open(os.path.join(file_path, "workload","schema-1.0", "generic_information.json"), "r") as file:
            wl_data = json.load(file)

        for key, parse_script in self.generic_statistic_scripts:
            parser = parse_script(file_name, wl_data)
            text_dict = parser.generate_content()

            trace_yaml_dict[key] = {
                "text": text_dict,
                "graph": None
            }

        if os.path.exists(os.path.join(file_path, "workflows", "schema-1.0")):
            workflow_df = pd.read_parquet(os.path.join(file_path, "workflows", "schema-1.0"))
            for key, plot_script in self.plot_workflow_statistic_scripts:
                parser = plot_script(file_name, workflow_df, os.path.join(self.output_dir, "wta-data", "images", "graphs"))
                text_dict, graph_name = parser.generate_content()
                print(graph_name)
                plt.close('all')  # Clear up memory by closing all plots from matplotlib memory stack.

                trace_yaml_dict[key] = {
                    "text": None,
                    "graph": None
                }

                if text_dict:
                    trace_yaml_dict[key]["text"] = text_dict
                if graph_name:
                    trace_yaml_dict[key]["graph"] = graph_name

        if os.path.exists(os.path.join(file_path, "tasks", "schema-1.0")):
            task_df = pd.read_parquet(os.path.join(file_path, "tasks", "schema-1.0"))
            for key, plot_script in self.plot_task_statistics_scripts:
                parser = plot_script(file_name, task_df, os.path.join(self.output_dir, "wta-data", "images", "graphs"))
                text_dict, graph_name = parser.generate_content()
                print(graph_name)
                plt.close('all')  # Clear up memory by closing all plots from matplotlib memory stack.

                trace_yaml_dict[key] = {
                    "text": None,
                    "graph": None
                }

                if text_dict:
                    trace_yaml_dict[key]["text"] = text_dict
                if graph_name:
                    trace_yaml_dict[key]["graph"] = graph_name

        return file_name, trace_yaml_dict


if __name__ == '__main__':
    arguments = docopt(__doc__)

    trace_dir = arguments['<dir_with_traces>']
    output_dir = arguments['<output_dir>']

    TraceParser(trace_dir, output_dir)
