import os
import dask.dataframe as dd
import pandas as pd
import numpy as np


def compute_characteristics(task_group):
    """
    Creates a dataframe with: number of cpus used, memory consumption, network usage, disk space usage, and energy
      consumption per task group.
    :param task_group: A group of tasks belonging to a worfklow
    :return: a dataframe containing workflow characteristics.
    """
    resource_count = task_group["resource_amount_requested"].sum()
    task_count = len(task_group)

    memory_consumption = task_group["memory_requested"].sum()
    if memory_consumption < 0:
        memory_consumption = -1

    network_consumption = task_group["network_io_time"].sum()
    if network_consumption < 0:
        network_consumption = -1

    disk_space_consumption = task_group["disk_space_requested"].sum()
    if disk_space_consumption < 0:
        disk_space_consumption = -1

    energy_consumption = task_group["energy_consumption"].sum()
    if energy_consumption < 0:
        energy_consumption = -1

    return pd.DataFrame(
        {"task_count": pd.Series([np.int32(task_count)], dtype=np.int32),
         "total_resources": pd.Series([np.int32(resource_count)], dtype=np.int32),
         "total_memory_usage": pd.Series([np.float64(memory_consumption)], dtype=np.float64),
         "total_network_usage": pd.Series([np.float64(network_consumption)], dtype=np.float64),
         "total_disk_space_usage": pd.Series([np.float64(disk_space_consumption)], dtype=np.float64),
         "total_energy_consumption": pd.Series([np.int64(energy_consumption)], dtype=np.int64),
         }
    )


def compute_workflow_characteristics(target_dir):
    dataframe_of_tasks = dd.read_parquet(os.path.join(target_dir, "tasks"),
                                         engine="pyarrow",
                                         index=False)

    grouped_df = dataframe_of_tasks.groupby(by="workflow_id")

    workflow_statistics_df = grouped_df.apply(compute_characteristics, meta={
        "task_count": np.int64,
        "total_resources": np.int32,
        "total_memory_usage": np.float64,
        "total_network_usage": np.float64,
        "total_disk_space_usage": np.float64,
        "total_energy_consumption": np.int64,
    }).reset_index(drop=True)

    return workflow_statistics_df
