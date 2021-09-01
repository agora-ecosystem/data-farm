import os
from enum import Enum

import numpy as np
import pandas as pd
from pprint import pprint

from PyInquirer import prompt, Separator
from examples import custom_style_2

import BuildAndSubmit
from consolemenu import MultiSelectMenu, SelectionMenu, items
import argparse

class ActiveLearningUtility:

    def get_X_y(df, feature_cols, label_col):
        return df.loc[:, feature_cols].values, df.loc[:, label_col].values

    def get_executed_plans_exec_time(jobs_to_run):
        executed_plans = BuildAndSubmit.get_executed_plans_multiple_data_ids()
        executed_plans_times = [(ep_k[0], ep_k[1], ep_v["netRunTime"]) for ep_k, ep_v in executed_plans.items() if
                                ep_k in jobs_to_run]
        if len(executed_plans_times) != len(jobs_to_run):
            print(
                f"WARNING - The number of executed jobs '{len(executed_plans_times)}' does not match the requested jobs '{len(jobs_to_run)}'.")

        return pd.DataFrame(executed_plans_times, columns=["plan_id", "data_id", "netRunTime"]).set_index(
            ["plan_id", "data_id"])

    def submit_jobs(init_jobs_to_run):
        exec_plans_path_already_computed = BuildAndSubmit.get_exec_plans_path()
        exec_plans_already_computed = {os.path.basename(ep).replace("$.json", "") for ep in
                                       exec_plans_path_already_computed}

        job_projects = BuildAndSubmit.get_job_projects()
        job_projects = sorted(job_projects, key=BuildAndSubmit.job_id_v)
        job_projects = [jp for jp in job_projects if
                        (jp in init_jobs_to_run) and (jp not in exec_plans_already_computed)]

        print(f"Submitting #{job_projects.__len__()} jobs:", job_projects)
        BuildAndSubmit.run_jobs(job_projects)
        return

    def get_dataset(features_df, feature_cols, label_col):
        train_data_df = features_df.loc[~features_df[label_col].isna(), :]
        print("TRAIN DATA DF")
        print(train_data_df)
        # train_data_df = features_df.loc[features_df.index.get_level_values(0).isin(executed_jobs), :]
        val_data_df = features_df.loc[~features_df.index.isin(train_data_df.index), :]
        # test_data_df = test_df.copy()

        X_train, y_train = ActiveLearningUtility.get_X_y(train_data_df, feature_cols, label_col)
        ids_train = train_data_df.reset_index()[["plan_id", "data_id"]]
        print("Train data:", X_train.shape)

        X_test, y_test = ActiveLearningUtility.get_X_y(val_data_df, feature_cols, label_col)
        ids_test = val_data_df.reset_index()[["plan_id", "data_id"]]
        print("Test data:", X_test.shape)
        return X_train, y_train, ids_train, X_test, y_test, ids_test


class UserSampling:

    def __init__(self):
        print("Initiating user specified sampling")

    def user_specified_sampling(self, X_test, iter_res, ids_test):
        a_list = ["red", "blue", "green"]
        # selection = MultiSelectMenu(title="Select Jobs to execute")
        # uncertainty_intervals = iter_res["uncertainty_interval"]
        # for i in range(0,len(k_sample)):
        #     selection.append_item(items.SelectionItem(f"Job {i} Uncertainty Interval: {uncertainty_intervals[i]}",index=i))
        # selection.show()
        # processed = selection.process_user_input()
        # test = selection.selected_item
        # print("Jobs selected:" + str(selection.current_item.index))
        joboptions = []
        uncertainty_intervals = iter_res["uncertainty_interval"]
        job_names = ids_test["plan_id"]
        job_data_ids = ids_test["data_id"]
        for i in range(0,len(X_test)):
            joboptions.append(f"{i}: Job \"{job_names[i]}\", Data ID: {job_data_ids[i]}, Uncertainty Interval: {uncertainty_intervals[i]}")
        sampling_idx = self.options_list("Which jobs would you like to run?", "Type numbers, separated by commas: ", joboptions)

        return ids_test.iloc[sampling_idx].copy()

    def options_list(self,title, query, options):
        print(title)
        for option in options:
            print(option)

        while True:
            try:
                val = input(query)
                print()
                value = list(map(int, val.strip().split(',')))
                print(value)
                return value
            except:
                print("Invalid input, please try again")

class JobExecutionSampler(Enum):
    RANDOM_SAMPLER = 1
    UNCERTAINTY = 2
    USER_SPECIFIED = 3
