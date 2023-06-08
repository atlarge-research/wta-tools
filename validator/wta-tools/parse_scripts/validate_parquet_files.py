#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mar 1 2019

Validation tool for parquet files

v.1.2
"""
import json
import os
import sys

import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from objects.datatransfer import Datatransfer
from objects.task import Task
from objects.workflow import Workflow

USAGE = 'Usage: python(3) ./validate_parquet_files.py <path_to_trace>'


class ParquetValidator(object):

    @staticmethod
    def validate_field(pdf, fieldname):
        try:
            if pdf[fieldname].isnull().values.any():
                print("\t{}: contains NaN or empty values".format(fieldname))
            elif pdf[fieldname].dtype in [np.float64, np.int64, np.int32]:
                if pdf[fieldname].isin({-1}).any():
                    print("\t{}: contains -1 values".format(fieldname))
                if pdf[fieldname].isin({0}).any():
                    print("\t{}: contains 0 values".format(fieldname))
        except KeyError:
            print("\t{}: field is not found in the trace file".format(fieldname))

    # Validate if all mandatory Workflow fields exist
    def validate_workflow_fields(self, workflow_pdf):
        for field_name, column_type in Workflow.get_parquet_meta_dict().items():
            self.validate_field(workflow_pdf, field_name)
        # Maybe not complete

    # Validate if all mandatory Task fields exist
    def validate_task_fields(self, task_pdf):
        for field_name, column_type in Task.get_parquet_meta_dict().items():
            self.validate_field(task_pdf, field_name)

    # Validate if all mandatory DataTransfer fields exist
    def validate_datatransfer_fields(self, datatransfer_pdf):
        for field_name, column_type in Datatransfer.get_parquet_meta_dict().items():
            self.validate_field(datatransfer_pdf, field_name)

    def validate_datatransfers(self, datatransfer_df, task_df):

        # check DataTransfers
        err_nr_ts_submit = 0
        nr_dt_df = 0
        err_nr_viol_size = 0
        err_nr_viol_transfertimes = 0
        nr_of_empty_src = 0
        nr_of_not_found_src_ids = 0
        nr_of_empty_dest = 0
        nr_of_not_found_dest_ids = 0

        id_set = set(task_df["id"])
        for index, row in datatransfer_df.iterrows():
            nr_dt_df = nr_dt_df + 1
            # Check ts_submit
            try:
                if int(row['ts_submit']) < -1:
                    if err_nr_ts_submit == 0 or err_nr_ts_submit == 1:
                        err_nr_ts_submit = 1
                    else:
                        err_nr_ts_submit = 3
            except ValueError:
                if err_nr_ts_submit == 0 or err_nr_ts_submit == 2:
                    err_nr_ts_submit = 2
                else:
                    err_nr_ts_submit = 3

            # size
            try:
                if int(row['size']) < 0:
                    if err_nr_viol_size == 0 or err_nr_viol_size == 1:
                        err_nr_viol_size = 1
                    else:
                        err_nr_viol_size = 3
            except ValueError:
                if err_nr_viol_size == 0 or err_nr_viol_size == 2:
                    err_nr_viol_size = 2
                else:
                    err_nr_viol_size = 3

            # transfertime
            try:
                if int(row['transfertime']) < 0:
                    if err_nr_viol_transfertimes == 0 or err_nr_viol_transfertimes == 1:
                        err_nr_viol_transfertimes = 1
                    else:
                        err_nr_viol_transfertimes = 3
            except ValueError:
                if err_nr_viol_transfertimes == 0 or err_nr_viol_transfertimes == 2:
                    err_nr_viol_transfertimes = 2
                else:
                    err_nr_viol_transfertimes = 3
            # destination
            dest = row['destination']
            if dest == -1:
                nr_of_empty_dest = nr_of_empty_dest + 1
            else:
                if dest not in id_set:
                    nr_of_not_found_dest_ids = nr_of_not_found_dest_ids + 1
            # source
            src = row['source']
            if src == -1:
                nr_of_empty_src = nr_of_empty_src + 1
            else:
                if src not in id_set:
                    nr_of_not_found_src_ids = nr_of_not_found_src_ids + 1
        # print
        print("\nValidating DataTransfers ...")
        # validate for empty fields
        self.validate_datatransfer_fields(datatransfer_df)
        if err_nr_ts_submit == 1:
            print('ts_submit: One or more values are not set, unknown or negative')
        elif err_nr_ts_submit == 2:
            print('ts_submit: One or more values are not an integer -> use unix time')
        elif err_nr_ts_submit == 3:
            print('ts_submit: One or more values are not set, unknown or negative, and not an integer -> use unix time')
        if err_nr_viol_size == 1:
            print('size: One or more data size values are smaller than 0')
        elif err_nr_viol_size == 2:
            print('size: One or more data size values are not integer')
        elif err_nr_viol_size == 3:
            print('size: One or more data size values are smaller than 0 not integer')
        if err_nr_viol_transfertimes == 1:
            print('transfertimes: One or more transfertime values are smaller than 0')
        elif err_nr_viol_transfertimes == 2:
            print('transfertimes: One or more values are not integer')
        elif err_nr_viol_transfertimes == 3:
            print('transfertimes: One or more values are smaller than 0 and not integer')

        print("destinations: (please check numbers)\n\ttotal nr. of empty destinations:\t{0}\t({1:3.2f} %)".format(
            nr_of_empty_dest, (nr_of_empty_dest / nr_dt_df) * 100.0))
        print("\ttotal nr. of not found destination ids: {0}\t({1:3.2f} %)".format(nr_of_not_found_dest_ids, (
                nr_of_not_found_dest_ids / nr_dt_df) * 100.0))

        print("sources: (please check numbers)\n\ttotal nr. of empty sources:\t{0}\t({1:3.2f} %)".format(
            nr_of_empty_src, (
                                     nr_of_empty_src / nr_dt_df) * 100.0))
        print("\ttotal nr. of not found source ids: {0}\t({1:3.2f} %)".format(
            nr_of_not_found_src_ids, (nr_of_not_found_src_ids / nr_dt_df) * 100.0))

    # Validates Workflows, Task fields and their relations
    def validate_workflows_tasks(self, workflow_pdf, task_pdf):
        # Check meta properties
        if workflow_pdf['id'].nunique() != len(workflow_pdf):
            print("Workflows IDs are not unique!")
            exit(-1)

        if workflow_pdf['id'].nunique() != task_pdf['workflow_id'].nunique():
            print("Not all workflows are in task_pdf!")
            exit(-1)

        # check Tasks
        nr_task_df = len(task_pdf)

        def check_children(df):
            nr_of_empty_childs = 0
            nr_of_childs = 0
            nr_of_not_found_child_ids = 0
            nr_of_empty_parents = 0
            nr_of_parents = 0
            nr_of_not_found_parent_ids = 0

            id_set = set(df['id'])
            for index, row in df.iterrows():
                # Check children
                children = row['children']
                if len(children) == 0:
                    nr_of_empty_childs = nr_of_empty_childs + 1
                else:
                    children_set = set(children)
                    nr_of_childs += len(children)
                    if len(children) != len(children_set):
                        print("Duplicate children found")
                    diff = children_set - id_set
                    if len(diff) > 0:
                        print(diff)
                    if diff:
                        print("Keys not found:" ",".join(diff))
                        nr_of_not_found_child_ids += len(diff)

                # Check parents
                parents = row['parents']
                if len(parents) == 0:
                    nr_of_empty_parents = nr_of_empty_parents + 1
                else:
                    parent_set = set(parents)
                    nr_of_parents += len(parents)
                    if len(parents) != len(parent_set):
                        print("Duplicate parents found")
                    diff = parent_set - id_set
                    if len(diff):
                        print("Keys not found:" ",".join(diff))
                        nr_of_not_found_parent_ids += len(diff)

            return pd.DataFrame(
                {
                    "nr_of_empty_childs": nr_of_empty_childs,
                    "nr_of_childs": nr_of_childs,
                    "nr_of_not_found_child_ids": nr_of_not_found_child_ids,
                    "nr_of_empty_parents": nr_of_empty_parents,
                    "nr_of_parents": nr_of_parents,
                    "nr_of_not_found_parent_ids": nr_of_not_found_parent_ids,
                }, index=[0])

        result_df = task_pdf.groupby("workflow_id").apply(check_children)

        nr_of_childs = result_df['nr_of_childs'].sum()
        nr_of_empty_childs = result_df['nr_of_empty_childs'].sum()
        nr_of_not_found_child_ids = result_df['nr_of_not_found_child_ids'].sum()
        nr_of_parents = result_df['nr_of_parents'].sum()
        nr_of_empty_parents = result_df['nr_of_empty_parents'].sum()
        nr_of_not_found_parent_ids = result_df['nr_of_not_found_parent_ids'].sum()

        err_nr_viol_runtime = 0
        err_nr_ts_submit = 0
        if nr_of_childs != nr_of_parents:
            print("The amount of children and parents are not equal! Num children: {} - num parents: {})".format(
                nr_of_childs, nr_of_parents))
            exit(-1)

        # Check ts_submit
        for index, row in task_pdf.iterrows():
            try:
                if int(row['ts_submit']) < -1:
                    if err_nr_ts_submit == 0 or err_nr_ts_submit == 1:
                        err_nr_ts_submit = 1
                    else:
                        err_nr_ts_submit = 3
            except ValueError:
                if err_nr_ts_submit == 0 or err_nr_ts_submit == 2:
                    err_nr_ts_submit = 2
                else:
                    err_nr_ts_submit = 3

            # Check runtime
            try:
                if float(row['runtime']) < 0.0:
                    if err_nr_viol_runtime == 0 or err_nr_viol_runtime == 1:
                        err_nr_viol_runtime = 1
                    else:
                        err_nr_viol_runtime = 3
            except ValueError:
                if err_nr_viol_runtime == 0 or err_nr_viol_runtime == 2:
                    err_nr_viol_runtime = 2
                else:
                    err_nr_viol_runtime = 3

        print("\nValidating Tasks ...")
        self.validate_task_fields(task_pdf)

        invalid_trace = False

        print("children: (please check numbers)\n\ttotal nr. of tasks without children: {0}\t({1:3.2f} %)".format(
            nr_of_empty_childs, (nr_of_empty_childs / nr_task_df) * 100.0))
        print("\ttotal nr. of not found ids: {0}\t({1:3.2f} %)".format(
            nr_of_not_found_child_ids, (nr_of_not_found_child_ids / nr_of_childs) * 100.0))

        if nr_of_not_found_child_ids > 0:
            invalid_trace = True

        print(
            "parents: (please check numbers)\n\ttotal nr. of tasks without parents:\t{0}\t({1:3.2f} %)".format(
                nr_of_empty_parents, (nr_of_empty_parents / nr_task_df) * 100.0))
        print("\ttotal nr. of not found ids: {0}\t({1:3.2f} %)".format(nr_of_not_found_parent_ids, (
                nr_of_not_found_parent_ids / nr_of_parents) * 100.0))

        if nr_of_not_found_parent_ids > 0:
            invalid_trace = True

        if err_nr_ts_submit == 1:
            print('ts_submit: One or more values are not set, unknown or negative')
        elif err_nr_ts_submit == 2:
            print('ts_submit: One or more values are not an integer -> use unix time')
        elif err_nr_ts_submit == 3:
            print('ts_submit: One or more values are not set, unknown or negative, and not an integer -> use unix time')
        if err_nr_viol_runtime == 1:
            print('runtime: One or more time values are negative')
        elif err_nr_viol_runtime == 2:
            print('runtime: One or more values are not float')
        elif err_nr_viol_runtime == 3:
            print('runtime: One or more values are negative and not float')

        if err_nr_ts_submit > 0:
            invalid_trace = True

        if err_nr_viol_runtime > 0:
            invalid_trace = True

        if invalid_trace:
            exit(-1)

        # check Workflows
        err_nr_ts_submit = 0
        nr_viol_task_cnt = 0
        task_cnt = 0
        for index, row in workflow_pdf.iterrows():
            # Check ts_submit
            try:
                if int(row['ts_submit']) < -1:
                    if err_nr_ts_submit == 0 or err_nr_ts_submit == 1:
                        err_nr_ts_submit = 1
                    else:
                        err_nr_ts_submit = 3
            except ValueError:
                if err_nr_ts_submit == 0 or err_nr_ts_submit == 2:
                    err_nr_ts_submit = 2
                else:
                    err_nr_ts_submit = 3

            # Check taskcount
            if row['task_count'] < 0:
                nr_viol_task_cnt = nr_viol_task_cnt + 1
            else:
                task_cnt = task_cnt + row['task_count']
        # print
        print("\nValidating Workflows ...")
        self.validate_workflow_fields(workflow_pdf)
        if err_nr_ts_submit == 1:
            print('ts_submit: At least one time value is smaller than -1')
        elif err_nr_ts_submit == 2:
            print('ts_submit: One or more values are not an integer -> use unix time')
        elif err_nr_ts_submit == 3:
            print('ts_submit: One or more values are not set, unknown or negative, and not an integer -> use unix time')

        if err_nr_ts_submit > 0:
            invalid_trace = True

        if nr_viol_task_cnt > 0:
            print('task_count: {0} negative task counts found.'.format(nr_viol_task_cnt))
            invalid_trace = True

        if task_cnt != nr_task_df:
            print('task_count: Tasks count of Workflows ({0}) is not equal to the sum of Tasks ({1})'.format(
                task_cnt, nr_task_df))
            invalid_trace = True

        if invalid_trace:
            exit(-1)


if __name__ == '__main__':
    pdWorkflowFrame = None
    pdTaskFrame = None
    pdDataTransferFrame = None
    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    task_df_location = os.path.join(sys.argv[1], "tasks", "schema-1.0")
    wf_df_location = os.path.join(sys.argv[1], "workflows", "schema-1.0")
    datatransfer_df_location = os.path.join(sys.argv[1], "datatransfers", "schema-1.0")

    if os.path.exists(datatransfer_df_location):
        pdDataTransferFrame = pd.read_parquet(datatransfer_df_location, engine='pyarrow')

    validator = ParquetValidator()

    pdWorkflowFrame = pd.read_parquet(wf_df_location, engine='pyarrow')
    pdTaskFrame = pd.read_parquet(task_df_location, engine='pyarrow')

    print("\nValidation started (only test fails and warnings are shown) ...")
    validator.validate_workflows_tasks(pdWorkflowFrame, pdTaskFrame)
    if pdDataTransferFrame is not None:
        validator.validate_datatransfers(pdDataTransferFrame, pdTaskFrame)

    generic_information_location = os.path.join(sys.argv[1], "workload", "schema-1.0", "generic_information.json")

    workload_fields = ["total_workflows", "total_tasks", "domain", "date_start", "date_end", "num_sites",
                       "num_resources", "num_users", "num_groups", "total_resource_seconds", "authors",
                       "min_resource_task", "max_resource_task", "std_resource_task", "mean_resource_task",
                       "median_resource_task", "first_quartile_resource_task", "third_quartile_resource_task",
                       "cov_resource_task", "min_memory", "max_memory", "std_memory", "mean_memory", "median_memory",
                       "first_quartile_memory", "third_quartile_memory", "cov_memory", "min_network_usage",
                       "max_network_usage", "std_network_usage", "mean_network_usage", "median_network_usage",
                       "first_quartile_network_usage", "third_quartile_network_usage", "cov_network_usage",
                       "min_disk_space_usage", "max_disk_space_usage", "std_disk_space_usage", "mean_disk_space_usage",
                       "median_disk_space_usage", "first_quartile_disk_space_usage", "third_quartile_disk_space_usage",
                       "cov_disk_space_usage", "min_energy", "max_energy", "std_energy", "mean_energy", "median_energy",
                       "first_quartile_energy", "third_quartile_energy", "cov_energy", "workload_description"]

    with open(generic_information_location, "r") as file:
        wl_data = json.load(file)
        # Check if all field exist
        for field in workload_fields:
            if field not in wl_data:
                print("Workload data is missing the {} field".format(field))
                exit(-1)

    print("DONE.")
