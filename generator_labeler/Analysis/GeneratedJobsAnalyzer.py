import sys



import generator_labeler.ExecutionPlanTools as planTools
from generator_labeler.Analysis.Analysis_config_1 import Analysis_1
from generator_labeler.Analysis.Analysis_config_2 import Analysis_2
from generator_labeler.FeatureExtraction.PredictorFeatureExtraction import compute_plan_features_from_graphs, show_feature_corr, \
    preprocess_jobs_data_info, fill_jobs_cardinality, compute_jobs_cardinality_features, \
    compute_cardinality_plan_features, remove_outliers, get_tables_features, compute_original_cardinality_plan_features
from generator_labeler.Analysis.models import compute_LR, test_model_3, build_RFR, validate_RFR
from generator_labeler.FeatureExtraction import FeatureExtraction

import pandas as pd
import numpy as np


import matplotlib.pyplot as plt
import seaborn as sns
sns.set_context("talk")
sns.set_style("whitegrid")


def parse_args(args):
    params = {}

    generated_exec_plans_folders = [
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_exec_plans/1GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_exec_plans/5GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_exec_plans/10GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_exec_plans/50GB"
    ]
    generated_jobs_info_path = "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/generated_jobs/generated_jobs_info.json"
    params["generatedExecPlanSource"] = generated_exec_plans_folders
    params["generatedJobsInfo"] = generated_jobs_info_path

    jobs_task_manager_details = [
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_task_manager_details/1GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_task_manager_details/5GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_task_manager_details/10GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_task_manager_details/50GB"
    ]
    params["jobs_task_manager_details"] = jobs_task_manager_details

    params["original_jobs_exec_plans"] = [
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/original_jobs_exec_plans/1GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/original_jobs_exec_plans/5GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/original_jobs_exec_plans/10GB",
        "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/original_jobs_exec_plans/50GB",
    ]
    params["original_Cardinality_features"] = "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/original_jobs_exec_plans/original_Cardinality_features_2.csv"

    return params


def load_generated_dataset(params, load_original_cards=False, data_sizes=["1GB", "5GB", "10GB", "50GB"]):
    exec_plans_json = []

    for gpf in params["generatedExecPlanSource"]:
        exec_plans_json = exec_plans_json + planTools.load_exec_plans(gpf)

    exec_plans_graph = planTools.compute_graphs_from_plans(exec_plans_json, include_cycles=False)

    # Compute cardinality_plan_features
    if load_original_cards:
        print("Loading original cardinality...")
        cardinality_plan_features = compute_original_cardinality_plan_features(params["jobs_task_manager_details"], params["generatedJobsInfo"])
    else:
        print("Loading estimated cardinality...")
        cardinality_plan_features = compute_cardinality_plan_features(params["generatedJobsInfo"], data_sizes=data_sizes)

    print(cardinality_plan_features.info())
    print()

    # Compute nodes_plan_features
    nodes_plan_features = FeatureExtraction.get_plan_tables_from_plans(exec_plans_graph)  # .set_index("plan_id")

    print(nodes_plan_features.info())
    print()

    # Compute graphs_plan_features
    graphs_plan_features = compute_plan_features_from_graphs(exec_plans_graph)

    print(graphs_plan_features.info())
    #print(graphs_plan_features)

    # Join plan and cardinality features
    plan_data_features = pd.merge(cardinality_plan_features, graphs_plan_features, left_index=True, right_index=True)
    print(plan_data_features.info())

    plan_data_features = plan_data_features.sort_index()
    return plan_data_features, exec_plans_graph


def load_validation_dataset(params):
    exec_plans_json = []

    for gpf in params["original_jobs_exec_plans"]:
        print(gpf)
        exec_plans_json = exec_plans_json + planTools.load_exec_plans(gpf)

    exec_plans_graph = planTools.compute_graphs_from_plans(exec_plans_json, include_cycles=False)

    # Compute cardinality_plan_features
    cardinality_plan_features = pd.read_csv(params["original_Cardinality_features"]).set_index(["plan_id", "data_id"])

    print(cardinality_plan_features)
    print()

    # Compute nodes_plan_features
    nodes_plan_features = FeatureExtraction.get_plan_tables_from_plans(exec_plans_graph)  # .set_index("plan_id")

    print(nodes_plan_features)
    print()

    # Compute graphs_plan_features
    graphs_plan_features = compute_plan_features_from_graphs(exec_plans_graph)

    print(graphs_plan_features.info())
    print(graphs_plan_features)

    # Join plan and cardinality features
    plan_data_features = pd.merge(cardinality_plan_features, graphs_plan_features, left_index=True, right_index=True)
    print(plan_data_features.info())
    print(plan_data_features)

    return plan_data_features, exec_plans_graph





def show_feature_trend(df, independet_col, dempendent_col):
    fig, ax = plt.subplots(figsize=(15, 6))
    ax = sns.scatterplot(x=independet_col, y=dempendent_col, hue="data_id", data=df.reset_index(), ax=ax)
    ax.set_ylabel(f"{dempendent_col} [s]")
    # ax.set_ylim((0,600))
    plt.show()
    plt.close()

    fig, ax = plt.subplots(figsize=(15, 6))
    ax = sns.scatterplot(x=independet_col, y=dempendent_col, hue="data_id", data=df.reset_index(), ax=ax)
    ax.set_xscale('log')
    ax.set_xlabel(f"Log({independet_col})")
    # ax.set_yscale('log')
    ax.set_ylabel(f"{dempendent_col}  [s]")
    # ax.set_ylim((0,600))
    plt.show()
    plt.close()

    fig, ax = plt.subplots(figsize=(15, 6))
    ax = sns.scatterplot(x=independet_col, y=dempendent_col, hue="data_id", data=df.reset_index(), ax=ax)
    # ax.set_xscale('log')
    ax.set_xlabel(f"{independet_col}")
    ax.set_yscale('log')
    ax.set_ylabel(f"Log({dempendent_col})")
    # ax.set_ylim((0,600))
    plt.show()
    plt.close()

    fig, ax = plt.subplots(figsize=(15, 6))
    ax = sns.scatterplot(x=independet_col, y=dempendent_col, hue="data_id", data=df.reset_index(), ax=ax)
    ax.set_xscale('log')
    ax.set_xlabel(f"Log({independet_col})")
    ax.set_yscale('log')
    ax.set_ylabel(f"Log({dempendent_col})")
    # ax.set_ylim((0,600))
    plt.show()
    plt.close()


def run_grid_search(plan_data_features):
    # log of runtime
    plan_data_features["Log_netRunTime"] = np.log(plan_data_features["netRunTime"])

    # bins of runtime
    from sklearn.preprocessing import KBinsDiscretizer
    kbins_model = KBinsDiscretizer(n_bins=3, encode='ordinal', strategy='quantile')
    plan_data_features["Bin3_netRunTime"] = kbins_model.fit_transform(
        plan_data_features["netRunTime"].values.reshape(-1, 1))[:, 0]

    kbins_model = KBinsDiscretizer(n_bins=5, encode='ordinal', strategy='quantile')
    plan_data_features["Bin5_netRunTime"] = kbins_model.fit_transform(
        plan_data_features["netRunTime"].values.reshape(-1, 1))[:, 0]

    a1 = Analysis_1()
    a1.run_analysis_1(plan_data_features, "/Users/researchuser7/IdeaProjects/flink-playground-jobs/etc/abstract-plan-generator/analysis_out/")

    # a2 = Analysis_2()
    # a2.run_analysis_2(plan_data_features, "/Users/researchuser7/IdeaProjects/flink-playground-jobs/etc/abstract-plan-generator/analysis_out/")



def main():

    args_params = parse_args(sys.argv)

    validation_plan_data_features, exec_plans_graph = load_validation_dataset(args_params)

    plan_data_features, exec_plans_graph = load_generated_dataset(args_params)


    # Remove outliers
    plan_data_features_no_out = remove_outliers(plan_data_features.copy(), "netRunTime", b=0.01, verbose=True)

    # Feature trend sourceCardinalitySum
    df = plan_data_features_no_out.copy()
    df["netRunTime"] = df["netRunTime"] / 1000
    df = df  # .loc[df["data_size"]==1.0,:]
    show_feature_trend(df, independet_col="sourceCardinalitySum", dempendent_col="netRunTime")

    # Feature trend outCardinality_mean
    df = plan_data_features_no_out.copy()
    df["netRunTime"] = df["netRunTime"] / 1000
    show_feature_trend(df, independet_col="outCardinality_mean", dempendent_col="netRunTime")

    features_1 = [
        'outCardinality_mean', 'outCardinality_std',
        'outCardinality_min', 'outCardinality_10%', 'outCardinality_50%',
        'outCardinality_90%', 'outCardinality_max', 'outCardinality_kurtosis',
        'outCardinality_skew', 'sourceCardinalitySum',
        'complexity_mean', 'complexity_max',
        'data_size', 'Data Source',
        'Join', 'Map', 'n_nodes', 'n_edges', 'longest_path_len', 'avg_parallelism',
        'Reduce', 'Filter', 'Group by', 'Partition', 'Sort-Partition']

    # Target label
    plan_data_features_no_out["Log_netRunTime"] = np.log(plan_data_features_no_out["netRunTime"])

    # run_grid_search(plan_data_features)

    build_RFR(plan_data_features_no_out, label_col="Log_netRunTime", feature_cols=features_1)
    validate_RFR(plan_data_features_no_out, label_col="Log_netRunTime", feature_cols=features_1, validation_data=validation_plan_data_features)


#if __name__ == '__main__':
#    main()

if __name__ == '__main__':
    args_params = parse_args(sys.argv)

    pd.set_option("display.max_columns", 30)
    pd.set_option('display.width', 10000)

    original_cardinality_plan_features = load_generated_dataset(args_params, load_original_cards=True)

    print(original_cardinality_plan_features)