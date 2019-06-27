import os

import pandas as pd


class Workload(object):
    _version = "1.0"

    def __init__(self, workflows, domain="", authors=None, workload_description="", ):
        if authors is None:
            authors = []
        self.workflows = workflows
        self.total_workflows = len(workflows)
        self.total_tasks = sum(wf.task_count for wf in workflows)
        self.domain = domain
        self.date_start = None
        self.date_end = None
        self.num_sites = len(
            set(task.submission_site for wf in workflows for task in wf.tasks if task.submission_site != -1))
        self.num_resources = sum(task.resource_amount_requested for wf in workflows for task in wf.tasks)
        self.num_users = len(set(task.user_id for wf in workflows for task in wf.tasks if task.user_id != -1))
        self.num_groups = len(set(task.group_id for wf in workflows for task in wf.tasks if task.group_id != -1))
        self.total_resource_seconds = sum(task.resource_amount_requested * task.runtime for wf in workflows for task in wf.tasks)
        self.authors = authors
        self.workload_description = workload_description

        # Resource consumption statistics
        resource_consumption_list = pd.Series([task.resource_amount_requested for wf in workflows for task in wf.tasks])
        self.min_resource_task = int(resource_consumption_list.min())
        self.max_resource_task = int(resource_consumption_list.max())
        self.std_resource_task = float(resource_consumption_list.std())
        self.mean_resource_task = float(resource_consumption_list.mean())
        self.median_resource_task = int(resource_consumption_list.median())
        self.first_quartile_resource_task = float(resource_consumption_list.quantile(.25))
        self.third_quartile_resource_task = float(resource_consumption_list.quantile(.75))
        self.cov_resource_task = self.std_resource_task / self.mean_resource_task

        # Memory consumption statistics
        memory_list = pd.Series(
            [task.memory_requested for wf in workflows for task in wf.tasks if task.memory_requested != -1])
        self.min_memory = float(memory_list.min()) if not memory_list.empty else -1
        self.max_memory = float(memory_list.max()) if not memory_list.empty else -1
        self.std_memory = float(memory_list.std()) if not memory_list.empty else -1
        self.mean_memory = float(memory_list.mean()) if not memory_list.empty else -1
        self.median_memory = float(memory_list.median()) if not memory_list.empty else -1
        self.first_quartile_memory = float(memory_list.quantile(.25)) if not memory_list.empty else -1
        self.third_quartile_memory = float(memory_list.quantile(.75)) if not memory_list.empty else -1
        self.cov_memory = self.std_memory / self.mean_memory if self.mean_memory > 0 else -1

        # Network consumption statistics
        network_usage_list = pd.Series(
            [task.network_io_time for wf in workflows for task in wf.tasks if task.network_io_time != -1])
        self.min_network_io_time = float(network_usage_list.min()) if not network_usage_list.empty else -1
        self.max_network_io_time = float(network_usage_list.max()) if not network_usage_list.empty else -1
        self.std_network_io_time = float(network_usage_list.std()) if not network_usage_list.empty else -1
        self.mean_network_io_time = float(network_usage_list.mean()) if not network_usage_list.empty else -1
        self.median_network_io_time = float(network_usage_list.median()) if not network_usage_list.empty else -1
        self.first_quartile_network_io_time = float(
            network_usage_list.quantile(.25)) if not network_usage_list.empty else -1
        self.third_quartile_network_io_time = float(
            network_usage_list.quantile(.75)) if not network_usage_list.empty else -1
        self.cov_network_io_time = self.std_network_io_time / self.mean_network_io_time if self.mean_network_io_time > 0 else -1

        # Disk space consumption statistics
        disk_space_usage_list = pd.Series(
            [task.disk_space_requested for wf in workflows for task in wf.tasks if task.disk_space_requested != -1])
        self.min_disk_space_usage = float(disk_space_usage_list.min()) if not disk_space_usage_list.empty else -1
        self.max_disk_space_usage = float(disk_space_usage_list.max()) if not disk_space_usage_list.empty else -1
        self.std_disk_space_usage = float(disk_space_usage_list.std()) if not disk_space_usage_list.empty else -1
        self.mean_disk_space_usage = float(disk_space_usage_list.mean()) if not disk_space_usage_list.empty else -1
        self.median_disk_space_usage = float(disk_space_usage_list.median()) if not disk_space_usage_list.empty else -1
        self.first_quartile_disk_space_usage = float(
            disk_space_usage_list.quantile(.25)) if not disk_space_usage_list.empty else -1
        self.third_quartile_disk_space_usage = float(
            disk_space_usage_list.quantile(.75)) if not disk_space_usage_list.empty else -1
        self.cov_disk_space_usage = self.std_disk_space_usage / self.mean_disk_space_usage if self.mean_disk_space_usage > 0 else -1

        # Energy consumption statistics
        energy_list = pd.Series(
            [task.energy_consumption for wf in workflows for task in wf.tasks if task.energy_consumption != -1])
        self.min_energy = float(energy_list.min()) if not energy_list.empty else -1
        self.max_energy = float(energy_list.max()) if not energy_list.empty else -1
        self.std_energy = float(energy_list.std()) if not energy_list.empty else -1
        self.mean_energy = float(energy_list.mean()) if not energy_list.empty else -1
        self.median_energy = float(energy_list.median()) if not energy_list.empty else -1
        self.first_quartile_energy = float(energy_list.quantile(.25)) if not energy_list.empty else -1
        self.third_quartile_energy = float(energy_list.quantile(.75)) if not energy_list.empty else -1
        self.cov_energy = self.std_energy / self.mean_energy if self.mean_energy > 0 else -1

    def zero_offset_data(self):
        # For the entire workload, set the first task to ts_submit 0, and correct all others.
        min_task_ts_submit = min(task.ts_submit for workflow in self.workflows for task in workflow.tasks)

        for workflow in self.workflows:
            for task in workflow.tasks:
                task.ts_submit -= min_task_ts_submit

    @staticmethod
    def get_json_dict_from_pandas_task_dataframe(task_dataframe, domain, start_date=None, end_date=None, authors=None,
                                                 workload_description=""):
        memory_usage_series = task_dataframe["memory_requested"]
        network_usage_series = task_dataframe["network_io_time"]
        disk_space_usage_series = task_dataframe["disk_space_requested"]
        energy_consumption_series = task_dataframe["energy_consumption"]

        resource_std = task_dataframe["resource_amount_requested"].std()
        resource_mean = task_dataframe["resource_amount_requested"].mean()

        network_std = network_usage_series.std()
        network_mean = network_usage_series.mean()

        memory_usage_std = memory_usage_series.std()
        memory_usage_mean = memory_usage_series.mean()

        disk_space_usage_std = disk_space_usage_series.std()
        disk_space_usage_mean = disk_space_usage_series.mean()

        energy_consumption_std = energy_consumption_series.std()
        energy_consumption_mean = energy_consumption_series.mean()

        return {
            "total_workflows": task_dataframe["workflow_id"].nunique(),
            "total_tasks": len(task_dataframe),
            "domain": domain,
            "date_start": start_date,
            "date_end": end_date,
            "num_sites": task_dataframe["workflow_id"].nunique(),
            "num_resources": task_dataframe["resource_amount_requested"].sum(),
            "num_users": task_dataframe["user_id"].nunique(),
            "num_groups": task_dataframe["group_id"].nunique(),
            "total_resource_seconds": (task_dataframe["resource_amount_requested"] * task_dataframe["runtime"]).sum() / 1000,
            "authors": authors,
            "min_resource_task": task_dataframe["resource_amount_requested"].min(),
            "max_resource_task": task_dataframe["resource_amount_requested"].max(),
            "std_resource_task": resource_std,
            "mean_resource_task": resource_mean,
            "median_resource_task": task_dataframe["resource_amount_requested"].quantile(),
            "first_quartile_resource_task": task_dataframe["resource_amount_requested"].quantile(.25),
            "third_quartile_resource_task": task_dataframe["resource_amount_requested"].quantile(.75),
            "cov_resource_task": resource_std / resource_mean,
            "min_memory": memory_usage_series.min(),
            "max_memory": memory_usage_series.max(),
            "std_memory": memory_usage_std,
            "mean_memory": memory_usage_mean,
            "median_memory": memory_usage_series.quantile() if memory_usage_mean > 0 else -1,
            "first_quartile_memory": memory_usage_series.quantile(.25) if memory_usage_mean > 0 else -1,
            "third_quartile_memory": memory_usage_series.quantile(.75) if memory_usage_mean > 0 else -1,
            "cov_memory": memory_usage_std / memory_usage_mean,
            "min_network_usage": network_usage_series.min(),
            "max_network_usage": network_usage_series.max(),
            "std_network_usage": network_std,
            "mean_network_usage": network_mean,
            "median_network_usage": network_usage_series.quantile() if network_mean > 0 else -1,
            "first_quartile_network_usage": network_usage_series.quantile(.25) if network_mean > 0 else -1,
            "third_quartile_network_usage": network_usage_series.quantile(.75) if network_mean > 0 else -1,
            "cov_network_usage": network_std / network_mean,
            "min_disk_space_usage": disk_space_usage_series.min(),
            "max_disk_space_usage": disk_space_usage_series.max(),
            "std_disk_space_usage": disk_space_usage_std,
            "mean_disk_space_usage": disk_space_usage_mean,
            "median_disk_space_usage": disk_space_usage_series.quantile() if disk_space_usage_mean > 0 else -1,
            "first_quartile_disk_space_usage": disk_space_usage_series.quantile(
                .25) if disk_space_usage_mean > 0 else -1,
            "third_quartile_disk_space_usage": disk_space_usage_series.quantile(
                .75) if disk_space_usage_mean > 0 else -1,
            "cov_disk_space_usage": disk_space_usage_std / disk_space_usage_mean,
            "min_energy": energy_consumption_series.min(),
            "max_energy": energy_consumption_series.max(),
            "std_energy": energy_consumption_std,
            "mean_energy": energy_consumption_mean,
            "median_energy": energy_consumption_series.quantile() if energy_consumption_mean > 0 else -1,
            "first_quartile_energy": energy_consumption_series.quantile(
                .25) if energy_consumption_mean > 0 else -1,
            "third_quartile_energy": energy_consumption_series.quantile(
                .75) if energy_consumption_mean > 0 else -1,
            "cov_energy": energy_consumption_std / energy_consumption_mean,
            "workload_description": workload_description,
        }

    @staticmethod
    def get_json_dict_from_dask_task_dataframe(task_dataframe, domain, start_date=None, end_date=None, authors=None,
                                               workload_description=""):
        memory_usage_series = task_dataframe["memory_requested"]
        network_usage_series = task_dataframe["network_io_time"]
        disk_space_usage_series = task_dataframe["disk_space_requested"]
        energy_consumption_series = task_dataframe["energy_consumption"]

        resouce_std = task_dataframe["resource_amount_requested"].std().compute()
        resource_mean = task_dataframe["resource_amount_requested"].mean().compute()

        network_std = network_usage_series.std().compute()
        network_mean = network_usage_series.mean().compute()

        memory_usage_std = memory_usage_series.std().compute()
        memory_usage_mean = memory_usage_series.mean().compute()

        disk_space_usage_std = disk_space_usage_series.std().compute()
        disk_space_usage_mean = disk_space_usage_series.mean().compute()

        energy_consumption_std = energy_consumption_series.std().compute()
        energy_consumption_mean = energy_consumption_series.mean().compute()

        return {
            "total_workflows": task_dataframe["workflow_id"].nunique().compute(),
            "total_tasks": len(task_dataframe),
            "domain": domain,
            "date_start": start_date,
            "date_end": end_date,
            "num_sites": task_dataframe["workflow_id"].nunique().compute(),
            "num_resources": task_dataframe["resource_amount_requested"].sum().compute(),
            "num_users": task_dataframe["user_id"].nunique().compute(),
            "num_groups": task_dataframe["group_id"].nunique().compute(),
            "total_resource_seconds": (task_dataframe["resource_amount_requested"] * task_dataframe["runtime"]).sum().compute() / 1000,
            "authors": authors,

            "min_resource_task": task_dataframe["resource_amount_requested"].min().compute(),
            "max_resource_task": task_dataframe["resource_amount_requested"].max().compute(),
            "std_resource_task": resouce_std,
            "mean_resource_task": resource_mean,
            "median_resource_task": task_dataframe["resource_amount_requested"].quantile().compute(),
            "first_quartile_resource_task": task_dataframe["resource_amount_requested"].quantile(.25).compute(),
            "third_quartile_resource_task": task_dataframe["resource_amount_requested"].quantile(.75).compute(),
            "cov_resource_task": resouce_std / resource_mean,

            "min_memory": memory_usage_series.min().compute(),
            "max_memory": memory_usage_series.max().compute(),
            "std_memory": memory_usage_std,
            "mean_memory": memory_usage_mean,
            "median_memory": memory_usage_series.quantile().compute() if memory_usage_mean > 0 else -1,
            "first_quartile_memory": memory_usage_series.quantile(.25).compute() if memory_usage_mean > 0 else -1,
            "third_quartile_memory": memory_usage_series.quantile(.75).compute() if memory_usage_mean > 0 else -1,
            "cov_memory": memory_usage_std / memory_usage_mean,

            "min_network_usage": network_usage_series.min().compute(),
            "max_network_usage": network_usage_series.max().compute(),
            "std_network_usage": network_std,
            "mean_network_usage": network_mean,
            "median_network_usage": network_usage_series.quantile().compute() if network_mean > 0 else -1,
            "first_quartile_network_usage": network_usage_series.quantile(.25).compute() if network_mean > 0 else -1,
            "third_quartile_network_usage": network_usage_series.quantile(.75).compute() if network_mean > 0 else -1,
            "cov_network_usage": network_std / network_mean,

            "min_disk_space_usage": disk_space_usage_series.min().compute(),
            "max_disk_space_usage": disk_space_usage_series.max().compute(),
            "std_disk_space_usage": disk_space_usage_std,
            "mean_disk_space_usage": disk_space_usage_mean,
            "median_disk_space_usage": disk_space_usage_series.quantile().compute() if disk_space_usage_mean > 0 else -1,
            "first_quartile_disk_space_usage": disk_space_usage_series.quantile(
                .25).compute() if disk_space_usage_mean > 0 else -1,
            "third_quartile_disk_space_usage": disk_space_usage_series.quantile(
                .75).compute() if disk_space_usage_mean > 0 else -1,
            "cov_disk_space_usage": disk_space_usage_std / disk_space_usage_mean,

            "min_energy": energy_consumption_series.min().compute(),
            "max_energy": energy_consumption_series.max().compute(),
            "std_energy": energy_consumption_std,
            "mean_energy": energy_consumption_mean,
            "median_energy": energy_consumption_series.quantile().compute() if energy_consumption_mean > 0 else -1,
            "first_quartile_energy": energy_consumption_series.quantile(
                .25).compute() if energy_consumption_mean > 0 else -1,
            "third_quartile_energy": energy_consumption_series.quantile(
                .75).compute() if energy_consumption_mean > 0 else -1,
            "cov_energy": energy_consumption_std / energy_consumption_mean,
            "workload_description": workload_description,
        }

    @staticmethod
    def get_json_dict_from_spark_task_dataframe(task_dataframe, domain, start_date=None, end_date=None, authors=None,
                                                workload_description=""):
        import pyspark.sql.functions as F

        column_name = 'memory_requested'  # TODO or assigned?
        memory_stats = task_dataframe.select(
            F.mean(F.col(column_name)).alias('mean'),
            F.stddev(F.col(column_name)).alias('std'),
            F.min(F.col(column_name)).alias('min'),
            F.max(F.col(column_name)).alias('max'),
        ).collect()
        memory_percentiles = task_dataframe.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.001)

        column_name = "resource_amount_requested"  # TODO: or assigned?
        resource_stats = task_dataframe.select(
            F.mean(F.col(column_name)).alias('mean'),
            F.stddev(F.col(column_name)).alias('std'),
            F.min(F.col(column_name)).alias('min'),
            F.max(F.col(column_name)).alias('max'),
        ).collect()
        resource_percentiles = task_dataframe.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.001)

        column_name = "network_io_time"
        network_stats = task_dataframe.select(
            F.mean(F.col(column_name)).alias('mean'),
            F.stddev(F.col(column_name)).alias('std'),
            F.min(F.col(column_name)).alias('min'),
            F.max(F.col(column_name)).alias('max'),
        ).collect()
        network_percentiles = task_dataframe.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.001)

        column_name = "disk_space_requested"
        disk_usage_stats = task_dataframe.select(
            F.mean(F.col(column_name)).alias('mean'),
            F.stddev(F.col(column_name)).alias('std'),
            F.min(F.col(column_name)).alias('min'),
            F.max(F.col(column_name)).alias('max'),
        ).collect()
        disk_usage_percentiles = task_dataframe.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.001)

        column_name = "energy_consumption"
        energy_consumption_stats = task_dataframe.select(
            F.mean(F.col(column_name)).alias('mean'),
            F.stddev(F.col(column_name)).alias('std'),
            F.min(F.col(column_name)).alias('min'),
            F.max(F.col(column_name)).alias('max'),
        ).collect()
        energy_consumption_percentiles = task_dataframe.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.001)

        sum_stats = task_dataframe.select(
            F.sum(F.col("runtime")).alias('sum_runtime'),
            F.sum(F.col("resource_amount_requested")).alias('sum_resources_requested'),
        ).collect()

        total_resources_requested = sum_stats[0]['sum_resources_requested']

        # Compute the total resources by row-wise multiplying resources times the runtime. If a value is null, assume
        # it is 0 so it is not included in the final result.
        task_subset_df = task_dataframe.select("runtime", "resource_amount_requested") \
                .fillna(0)

        resource_second_df = task_subset_df.withColumn("resource_second", task_subset_df.runtime * task_subset_df.resource_amount_requested)
        total_resource_seconds = resource_second_df.select(F.sum("resource_second")).collect()[0][0]

        return {
            "total_workflows": task_dataframe.select("workflow_id").distinct().count(),
            "total_tasks": task_dataframe.count(),
            "domain": domain,
            "date_start": start_date,
            "date_end": end_date,
            "num_sites": task_dataframe.select("submission_site").distinct().count(),
            "num_resources": total_resources_requested,
            "num_users": task_dataframe.select("user_id").distinct().count(),
            "num_groups": task_dataframe.select("group_id").distinct().count(),
            "total_resource_seconds": total_resource_seconds / 1000,
            "authors": authors,

            "min_resource_task": resource_stats[0]['min'],
            "max_resource_task": resource_stats[0]['max'],
            "std_resource_task": resource_stats[0]['std'],
            "mean_resource_task": resource_stats[0]['mean'],
            "median_resource_task": resource_percentiles[1],
            "first_quartile_resource_task": resource_percentiles[0],
            "third_quartile_resource_task": resource_percentiles[2],
            "cov_resource_task": float(resource_stats[0]['std']) / resource_stats[0]['mean'] if resource_stats[0][
                                                                                                    'mean'] > 0 else -1,

            "min_memory": memory_stats[0]['min'],
            "max_memory": memory_stats[0]['max'],
            "std_memory": memory_stats[0]['std'],
            "mean_memory": memory_stats[0]['mean'],
            "median_memory": memory_percentiles[1],
            "first_quartile_memory": memory_percentiles[0],
            "third_quartile_memory": memory_percentiles[2],
            "cov_memory": float(memory_stats[0]['std']) / memory_stats[0]['mean'] if memory_stats[0][
                                                                                         'mean'] > 0 else -1,
            "min_network_usage": network_stats[0]['min'],
            "max_network_usage": network_stats[0]['max'],
            "std_network_usage": network_stats[0]['std'],
            "mean_network_usage": network_stats[0]['mean'],
            "median_network_usage": network_percentiles[1],
            "first_quartile_network_usage": network_percentiles[0],
            "third_quartile_network_usage": network_percentiles[2],
            "cov_network_usage": float(network_stats[0]['std']) / network_stats[0]['mean'] if network_stats[0][
                                                                                                  'mean'] > 0 else -1,
            "min_disk_space_usage": disk_usage_stats[0]['min'],
            "max_disk_space_usage": disk_usage_stats[0]['max'],
            "std_disk_space_usage": disk_usage_stats[0]['std'],
            "mean_disk_space_usage": disk_usage_stats[0]['mean'],
            "median_disk_space_usage": disk_usage_percentiles[1],
            "first_quartile_disk_space_usage": disk_usage_percentiles[0],
            "third_quartile_disk_space_usage": disk_usage_percentiles[2],
            "cov_disk_space_usage": float(disk_usage_stats[0]['std']) / disk_usage_stats[0]['mean'] if
            disk_usage_stats[0]['mean'] > 0 else -1,

            "min_energy": energy_consumption_stats[0]['min'],
            "max_energy": energy_consumption_stats[0]['max'],
            "std_energy": energy_consumption_stats[0]['std'],
            "mean_energy": energy_consumption_stats[0]['mean'],
            "median_energy": energy_consumption_percentiles[1],
            "first_quartile_energy": energy_consumption_percentiles[0],
            "third_quartile_energy": energy_consumption_percentiles[2],
            "cov_energy": float(energy_consumption_stats[0]['std']) / energy_consumption_stats[0]['mean'] if
            energy_consumption_stats[0]['mean'] > 0 else -1,
            "workload_description": workload_description,
        }

    def get_json_dict(self):
        self.zero_offset_data()

        return {
            "workflows": [workflow.get_json_dict() for workflow in self.workflows],
            "total_workflows": self.total_workflows,
            "total_tasks": self.total_tasks,
            "domain": self.domain,
            "date_start": self.date_start,
            "date_end": self.date_end,
            "num_sites": self.num_sites,
            "num_resources": self.num_resources,
            "num_users": self.num_users,
            "num_groups": self.num_groups,
            "total_resource_seconds": self.total_resource_seconds,
            "authors": self.authors,

            "min_resource_task": self.min_resource_task,
            "max_resource_task": self.max_resource_task,
            "std_resource_task": self.std_resource_task,
            "mean_resource_task": self.mean_resource_task,
            "median_resource_task": self.median_resource_task,
            "first_quartile_resource_task": self.first_quartile_resource_task,
            "third_quartile_resource_task": self.third_quartile_resource_task,
            "cov_resource_task": self.cov_resource_task,

            "min_memory": self.min_memory,
            "max_memory": self.max_memory,
            "std_memory": self.std_memory,
            "mean_memory": self.mean_memory,
            "median_memory": self.median_memory,
            "first_quartile_memory": self.first_quartile_memory,
            "third_quartile_memory": self.third_quartile_memory,
            "cov_memory": self.cov_memory,

            "min_network_usage": self.min_network_io_time,
            "max_network_usage": self.max_network_io_time,
            "std_network_usage": self.std_network_io_time,
            "mean_network_usage": self.mean_network_io_time,
            "median_network_usage": self.median_network_io_time,
            "first_quartile_network_usage": self.first_quartile_network_io_time,
            "third_quartile_network_usage": self.third_quartile_network_io_time,
            "cov_network_usage": self.cov_network_io_time,

            "min_disk_space_usage": self.min_disk_space_usage,
            "max_disk_space_usage": self.max_disk_space_usage,
            "std_disk_space_usage": self.std_disk_space_usage,
            "mean_disk_space_usage": self.mean_disk_space_usage,
            "median_disk_space_usage": self.median_disk_space_usage,
            "first_quartile_disk_space_usage": self.first_quartile_disk_space_usage,
            "third_quartile_disk_space_usage": self.third_quartile_disk_space_usage,
            "cov_disk_space_usage": self.cov_disk_space_usage,

            "min_energy": self.min_energy,
            "max_energy": self.max_energy,
            "std_energy": self.std_energy,
            "mean_energy": self.mean_energy,
            "median_energy": self.median_energy,
            "first_quartile_energy": self.first_quartile_energy,
            "third_quartile_energy": self.third_quartile_energy,
            "cov_energy": self.cov_energy,
            "workload_description": self.workload_description,
            "version": self._version,
        }

    def get_parquet_dicts(self):
        pass

    @staticmethod
    def versioned_dir_name():
        return "schema-{}".format(Workload._version)

    @staticmethod
    def output_path():
        return os.path.join("workload", Workload.versioned_dir_name())
