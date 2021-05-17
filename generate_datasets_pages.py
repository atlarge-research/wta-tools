"""
Parses a directory with folders containing parquet data in the workflow trace format.
The <dir_with_traces> should be the directory containing the traces.
The <output_dir> is the root directory of the WTA website.

Usage:
  generate_datasets_pages.py <dir_with_traces> <output_dir>

Options:
  -u --upload                       Flag indicating if traces should be uploaded to Zenodo.
  -h --help                         Show this screen.
"""
import glob
import json
import os

import matplotlib.pyplot as plt
import yaml
from docopt import docopt
from pyspark.sql import SparkSession

from statistic_scripts.generic_cpu_information import GenericCPUInformation
from statistic_scripts.generic_disk_information import GenericDiskInformation
from statistic_scripts.generic_energy_information import GenericEnergyInformation
from statistic_scripts.generic_memory_information import GenericMemoryInformation
from statistic_scripts.generic_network_information import GenericNetworkInformation
from statistic_scripts.generic_trace_information import GenericTraceInformation
from statistic_scripts.job_arrival_cdf import JobArrivalCDF
from statistic_scripts.job_arrival_graph import JobArrivalGraph
from statistic_scripts.job_critical_path_task_count_cdf import JobCriticalPathTaskCountCDF
from statistic_scripts.job_runtime_cdf import JobRuntimeCDF
from statistic_scripts.task_arrival_cdf import TaskArrivalCDF
from statistic_scripts.task_arrival_graph import TaskArrivalGraph
from statistic_scripts.task_completion_graph import TaskCompletionGraph
from statistic_scripts.task_memory_consumption_cdf import TaskMemoryConsumptionCDF
from statistic_scripts.task_resource_time_cdf import TaskResourceTimeCDF
from statistic_scripts.task_runtime_cdf import TaskRuntimeCDF
from statistic_scripts.task_wait_time_cdf import TaskWaitTimeCDF
from zenodo_scripts.ZenodoAPI import ZenodoAPI


class TraceParser:
    generic_statistic_scripts = [
        ("generic-info", GenericTraceInformation),
        ("generic-cpu", GenericCPUInformation),
        ("generic-memory", GenericMemoryInformation),
        ("generic-disk", GenericDiskInformation),
        ("generic-network", GenericNetworkInformation),
        ("generic-energy", GenericEnergyInformation),
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
        ("task-cputime-cdf", TaskResourceTimeCDF),
        ("task-memory-cdf", TaskMemoryConsumptionCDF),
        ("task-runtime-cdf", TaskRuntimeCDF),
        ("task-waittime-cdf", TaskWaitTimeCDF),
    ]

    def __init__(self, trace_dir, output_dir, upload_data=False):
        self.trace_dir = trace_dir
        self.output_dir = output_dir

        if "DAS5" in os.environ:  # If we want to execute it on the DAS-5 super computer
            print("We are on DAS5, {0} is master.".format(os.environ["HOSTNAME"] + ".ib.cluster"))
            self.spark = SparkSession.builder \
                .master("spark://" + os.environ['HOSTNAME'] + ".ib.cluster:7077") \
                .appName("WTA parser") \
                .config("spark.executor.memory", "28G") \
                .config("spark.executor.cores", "8") \
                .config("spark.executor.instances", "10") \
                .config("spark.driver.memory", "50G") \
                .config("spark.driver.maxResultSize", "40G") \
                .config("spark.default.parallelism", "1000") \
                .config("spark.sql.execution.arrow.enabled", "true") \
                .config("spark.cleaner.periodicGC.interval", "30s") \
                .getOrCreate()
        else:
            self.spark = (SparkSession.builder
                          .master("local[5]")
                          .appName("WTA Analysis")
                          .config("spark.executor.memory", "3G")
                          .config("spark.driver.memory", "12G")
                          .getOrCreate())

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

        # print("Cleaning the graph data folder...")
        # for the_file in os.listdir(graph_dir):
        #     file_path = os.path.join(graph_dir, the_file)
        #     try:
        #         if os.path.isfile(file_path):
        #             os.unlink(file_path)
        #     except Exception as e:
        #         print(e)

        traces_yml_location = os.path.join(self.output_dir, "wta-data", "traces.yml")
        if os.path.exists(traces_yml_location):
            print("Deleting {0} to generate a new one...".format(traces_yml_location))
            os.remove(traces_yml_location)

        print("Parsing traces...")
        self.parse_wtf_traces(upload_data)

    def parse_wtf_traces(self, upload_data):

        trace_dicts = []

        for file in glob.iglob(os.path.join(self.trace_dir, '*')):
            if os.path.isdir(file):
                file_name, trace_dict = self.parse_trace(file, upload_data)
                trace_dicts.append(trace_dict)

        with open(os.path.join(self.output_dir, "wta-data", "traces.yml"), "w+") as trace_info_file:
            trace_info_file.write(yaml.safe_dump(trace_dicts, default_flow_style=False))

    def parse_trace(self, file_path, upload_to_zenodo):
        # Get the file name without the extension
        raw_file_name = str(os.path.basename(file_path).split(".")[0])
        file_name = str(os.path.basename(file_path).split(".")[0])
        if file_name.endswith("_parquet"):
            file_name = file_name[:-len("_parquet")]

        trace_yaml_dict = {}
        trace_yaml_dict['id'] = file_name

        # Load the file's JSON contents
        with open(os.path.join(file_path, "workload", "schema-1.0", "generic_information.json"), "r") as file:
            wl_data = json.load(file)
            trace_description = wl_data["workload_description"]
            trace_authors = wl_data["authors"]

            print(file_path)
            for key, parse_script in self.generic_statistic_scripts:
                parser = parse_script(file_name, wl_data)
                text_dict = parser.generate_content()

                trace_yaml_dict[key] = {
                    "text": text_dict,
                    "graph": None
                }

        if os.path.exists(os.path.join(file_path, "workflows", "schema-1.0")):
            workflow_df = self.spark.read.parquet(os.path.join(file_path, "workflows", "schema-1.0"))
            for key, plot_script in self.plot_workflow_statistic_scripts:
                parser = plot_script(file_name, workflow_df,
                                     os.path.join(self.output_dir, "wta-data", "images", "graphs"))
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
            task_df = self.spark.read.parquet(os.path.join(file_path, "tasks", "schema-1.0"))
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

        if upload_to_zenodo:
            return self.upload_to_zenodo(file_path, raw_file_name, file_name, trace_authors, trace_description,
                                         trace_yaml_dict)
        else:
            return file_name, trace_yaml_dict

    def upload_to_zenodo(self, file_path, raw_file_name, file_name, trace_authors, trace_description, trace_yaml_dict):
        traces_uploaded = []

        # Get information about previous uploaded traces
        if os.path.exists('trace_upload_info.json'):
            traces_uploaded = json.load(open('trace_upload_info.json'))

        uploaded_traces = dict()
        for trace_object in traces_uploaded:
            uploaded_traces[trace_object['file_name']] = trace_object

        # with open('zenodo_scripts/sandbox-api-key.txt') as f:
        #     key = f.readline().strip()  # strip is needed to remove the linebreak at the end of the line!
        # z = ZenodoAPI("https://sandbox.zenodo.org/api", key)

        with open('zenodo_scripts/api-key.txt') as f:
            key = f.readline().strip()  # strip is needed to remove the linebreak at the end of the line!
        z = ZenodoAPI("https://zenodo.org/api", key)

        def upload_file():
            # Check if the file exists
            if not os.path.exists("{}.zip".format(file_path)):
                print("{} does not exists.".format("{}.zip".format(file_path)))
                return

            # Datasets that exceed 100MB in size cannot be uploaded to Zenodo via the API
            if os.path.getsize("{}.zip".format(file_path)) >= (100 * (2 ** 20)):
                print("{} is too large".format(raw_file_name))
            else:
                # Compute the SHA-1 value
                import hashlib
                BLOCKSIZE = 65536
                hasher = hashlib.sha1()
                with open("{}.zip".format(file_path), 'rb') as afile:
                    buf = afile.read(BLOCKSIZE)
                    while len(buf) > 0:
                        hasher.update(buf)
                        buf = afile.read(BLOCKSIZE)
                sha1 = hasher.hexdigest()

                # Construct a filename and a files dict
                data = {'filename': "{}.zip".format(raw_file_name)}
                files = {"file": open("{}.zip".format(file_path), 'rb')}
                r = z.add_file_to_repository(repo_id, data, files)  # Upload the files

                if r.status_code != 201:  # Check the response of the server
                    print("FAILED TO ADD ZIP TO ZENODO REPOSITORY {}".format(raw_file_name))
                else:
                    file_id = r.json()['id']  # Grab the file_id for later use
                    uploaded_traces[file_name]['zenodo_file_id'] = file_id
                    uploaded_traces[file_name]['sha-1'] = sha1

        if not file_name in uploaded_traces:
            r = z.create_empty_repository()
            repo_id = r.json()['id']
            new_trace_object = {}
            new_trace_object['file_name'] = file_name
            new_trace_object['zenodo_repo_id'] = repo_id
            new_trace_object['zenodo_file_id'] = None
            new_trace_object['doi'] = None
            new_trace_object['sha-1'] = None

            uploaded_traces[file_name] = new_trace_object
        else:
            repo_id = uploaded_traces[file_name]['zenodo_repo_id']
            print("File was uploaded previously, repo id {}".format(repo_id))

        can_publish = True
        # We assume a .zip file with raw_file_name exists within the same directory to upload.
        if uploaded_traces[file_name]['zenodo_file_id'] is None:
            upload_file()
            # Since it's the first time uploading the dataset, upload meta data too
            metadata_trace_authors = []  # Get the author names in the format Zenodo expects
            for author in trace_authors:
                metadata_trace_authors.append({"name": author})

            data = {
                'metadata': {
                    'title': 'Workflow Trace Archive {} trace'.format(file_name),
                    'upload_type': 'dataset',
                    'description': trace_description if len(
                        trace_description) >= 3 else "Trace description unavailable.",
                    'creators': metadata_trace_authors
                }
            }

            r = z.add_metadata_to_repository(repo_id, data)
            if r.status_code != 200:  # Check if metadata was successfully changed
                print("FAILED TO ADD METADATA: {}".format(r.text))
                can_publish = False
        else:  # We have seen this trace before, check if it's newer.
            import hashlib
            BLOCKSIZE = 65536
            hasher = hashlib.sha1()
            with open("{}.zip".format(file_path), 'rb') as afile:
                buf = afile.read(BLOCKSIZE)
                while len(buf) > 0:
                    hasher.update(buf)
                    buf = afile.read(BLOCKSIZE)
            sha1 = hasher.hexdigest()

            if sha1 != uploaded_traces[file_name]['sha-1']:
                print("New version of {} detected (sha-1 difference). Re-uploading...".format(raw_file_name))
                # Delete the old zip file and upload the new one (cannot update files)
                r = z.delete_file_from_repository(uploaded_traces[file_name]['zenodo_repo_id'],
                                                  uploaded_traces[file_name]['zenodo_file_id'])

                if r.status_code != 204:  # Check if file was successfully deleted
                    print("FAILED TO DELETE PREVIOUS FILE {}/{}".format(raw_file_name, file_path))
                else:
                    upload_file()

        doi_url = uploaded_traces[file_name]['doi']
        # Publish if we 1) can publish and 2) haven't done so before (no DOI known).
        if can_publish and doi_url is None:  # If we can publish, publish the repo.
            r = z.publish_repository(repo_id)

            if r.status_code != 202:
                print("FAILED TO PUBLISH REPO")
            else:
                doi_url = r.json()['doi_url']
                uploaded_traces[file_name]['doi'] = doi_url
                print(doi_url)

        # Save the current state (latest info)
        with open('trace_upload_info.json', 'w') as upload_data_file:
            upload_data_file.write(json.dumps(list(uploaded_traces.values())))

        # Add the DOI to the yaml dict
        trace_yaml_dict['doi'] = doi_url

        return file_name, trace_yaml_dict


if __name__ == '__main__':
    arguments = docopt(__doc__)

    trace_dir = arguments['<dir_with_traces>']
    output_dir = arguments['<output_dir>']
    should_upload = arguments['--upload']

    TraceParser(trace_dir, output_dir, should_upload)
