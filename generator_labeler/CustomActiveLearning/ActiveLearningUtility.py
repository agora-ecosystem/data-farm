import os
from enum import Enum

import pandas as pd

import BuildAndSubmit


class ActiveLearningUtility:

    def get_X_y(df, feature_cols, label_col):
        return df.loc[:, feature_cols].values, df.loc[:, label_col].values

    def get_executed_plans_exec_time(jobs_to_run):
        executed_plans = BuildAndSubmit.get_executed_plans()
        executed_plans_times = [(ep_k[0], ep_k[1], ep_v["netRunTime"]) for ep_k, ep_v in executed_plans.items() if
                                ep_k[0] in jobs_to_run]
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


class JobExecutionSampler(Enum):
    RANDOM_SAMPLER = 1
    UNCERTAINTY = 2
    USER_SPECIFIED = 3
