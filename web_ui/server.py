import sys
import time
import traceback
import shutil
from flask import Flask, send_from_directory
from flask import render_template, request, redirect, url_for, jsonify, session
from datetime import datetime
import os
import subprocess
import json
import pandas as pd
import networkx as nx
import re

import tools
from CONFIG import CONFIG
import matplotlib.pyplot as plt
import seaborn as sns

from RunLabelForecaster import set_up_active_learning, load_data_and_preprocess
from generator_labeler import ExecutionPlanTools
from generator_labeler.OriginalWorkloadModel import OriginalWorkloadModel
from generator_labeler.FeatureExtraction import FeatureExtraction
from networkx.drawing.nx_agraph import graphviz_layout
import pickle

from generator_labeler.FeatureExtraction.PredictorFeatureExtraction import compute_cardinality_plan_features, \
    get_estimated_out_cardinality, preprocess_jobs_data_info
from generator_labeler.Generator.AbstractPlan import AbstractPlanGenerator, AbstactPlanGeneratorModel

import numpy as np

from RunGenerator import create_project_folders
from generator_labeler.JobExecutionSampler.unsupervised_sampler import UniformAgglomerativeSampler, RandomSampler

sns.set_context("talk")
sns.set_style("whitegrid")

# from DataFarm.Generator import AbstractPlan as GAP
# from DataFarm.Generator.AbstractPlan import AbstactPlanGeneratorModel


app = Flask(__name__, static_url_path='/static')
# app.config.from_object("config.DataFarmConfig")
# app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
app.secret_key = os.urandom(24)

plan_graphs = []
reach_input_plans_table = pd.DataFrame()
cardinality_plan_features = pd.DataFrame()
data_plan_features = pd.DataFrame()
jobs_data_info = pd.DataFrame()

status = {
    "apg_upload": False,
    "exp_folder": "",
    "apg_analyze_plans": False,
    "apg_upload_details": {},
    "apg_parent_tm": "",
    "apg_children_tm": "",

    "apg_config": {},
    "apg_run": False,
    "apg_plans": {},

    "sji_run": False,
    "sji_config": {},
    "sji": {},

    "lf_run": False,
    "lf_current_iteration": -1,
    "lf_iterations": list(),
    "max_iter": 1000,
    "early_stop": False
}

keys = {"test": 0, "object": 1}


################
# Error Handling
################
@app.errorhandler(413)
def error_handler_413(error):
    return render_template("error.html", error_message=str(error)), 413


@app.errorhandler(404)
def error_handler_404(error):
    return render_template("error.html", error_message=str(error)), 404


@app.route("/error", methods=["GET"])
def error():
    error_message = request.args["error_message"]
    return render_template("error.html", error_message=str(error_message))


################
# Webapp logic
################
# TODO Provide Download of jobs and labels
@app.route("/download_generated_jobs_and_labels", methods=["GET"])
def download_generated_jobs_and_labels():
    return "Process complete!"


@app.route("/input_plan/<plan_id>", methods=["GET"])
def input_plan(plan_id):
    global reach_input_plans_table

    print(plan_id)
    plan_info = reach_input_plans_table[reach_input_plans_table["plan_id"] == plan_id].to_json(orient="split")
    similar_plans = []

    return render_template("input_plan.html", status=status, plan_info=plan_info)


# Active learning global variables
# Need to be available during active learning iterations
global custom_active_learning
global iteration_results
global active_learning_settings
global active_learning_data
global active_learning_features
global active_learning_features_stats
global instantiated_plan_stats
global active_learning_duration
# Mapping jobs to ids and reverse
global id_to_job_mapping
global job_to_id_mapping
global active_learning_features_id
# Original workload
global original_workload
global OriginalWorkloadModel
global original_workload_results

active_learning_settings = {
    "target_label": "",
    "initial_job_selection": list(),
    "n_init_jobs": 0,
    "n_generation_jobs": 3,
    "uncertainty_threshold": 0,
    "max_iter": 0,
    "max_time": 0,
    "current_automated_sample_ids": list(),
    "current_executed_jobs": list(),

}

active_learning_data = {
    "current_iteration": 0,
    "lf_iterations": list(),
    "executed jobs iteration": list(),
    "execution time": list()
}
iteration_results = []
active_learning_features = []
active_learning_features_id = []
active_learning_features_stats = {}
instantiated_plan_stats = {}
active_learning_duration = 0
id_to_job_mapping = {}
job_to_id_mapping = {}
active_learning_features_id = []
original_workload = []
original_workload_results = {}

@app.route("/lf_iteration", methods=["GET", "POST"])
def lf_iteration():
    global iteration_results
    global active_learning_settings
    global status
    global active_learning_features
    global active_learning_features_id
    global active_learning_duration
    global id_to_job_mapping
    global job_to_id_mapping
    global OriginalWorkloadModel
    global original_workload_results
    if request.method == 'POST':
        print(request.get_data())
        print(request.get_json())
        selected_jobs = request.get_json()["selected_jobs"]

        # Parse into jobname, data id
        selected_jobs_parsed = []
        for item in selected_jobs:
            splitted = re.sub("[\'()]", "", item).split(", ")
            plan_num = splitted[0]
            data_id = splitted[1]
            selected_jobs_parsed.append((plan_num, data_id))

        # Initialize first generation
        if (len(active_learning_features_id) == 0):
            active_learning_features_id = active_learning_features  # load_data_and_preprocess(CONFIG.GENERATED_METADATA_PATH, DATA_IDS = CONFIG.DATA_IDS)

            # active_learning_features_id = active_learning_features.droplevel(1)
            active_learning_features_id = active_learning_features_id.reset_index()

        if (len(id_to_job_mapping) == 0):
            print("ACTIVE LEARNING FEATURES")
            print(active_learning_features_id)
            id_to_job_mapping = active_learning_features_id["plan_id"].to_dict()
            job_to_id_mapping = {v: k for k, v in id_to_job_mapping.items()}
        print("JOB MAPPING")
        print(id_to_job_mapping)
        print(job_to_id_mapping)
        # make global
        tuple_mapping = list(zip(active_learning_features_id['plan_id'], active_learning_features_id['data_id']))
        print(tuple_mapping)
        selected_plan_ids = active_learning_features_id.loc[
            [i for i, x in enumerate(tuple_mapping) if x in selected_jobs_parsed]]
        # selected_plan_ids = active_learning_features_id.loc[active_learning_features_id['plan_id'].isin(selected_jobs_parsed)]
        print(selected_plan_ids)
        selected_ids = selected_plan_ids.index.values.tolist()
        # selected_ids = #[job_to_id_mapping.get(key) for key in selected_jobs_parsed]
        print(selected_ids)

        # Update list of executed jobs
        active_learning_settings["current_executed_jobs"].extend(selected_ids)
        print("Currently executed jobs")
        print(active_learning_settings["current_executed_jobs"])

        global custom_active_learning
        start_time = time.perf_counter()
        if status["lf_current_iteration"] < 1:
            # First iteration: set up active learning strategy object, run initial jobs and train model
            try:
                global LABEL_FORECASTER_OUT
                LABEL_FORECASTER_OUT = CONFIG.LABEL_FORECASTER_OUT
                os.mkdir(LABEL_FORECASTER_OUT)
            except Exception as ex:
                print(f"Experiment '{CONFIG.LABEL_FORECASTER_OUT}' already exists!")
                sys.exit(1)

            custom_active_learning = set_up_active_learning(generated_metadata_path=CONFIG.GENERATED_METADATA_PATH,
                                                            label_forecaster_out=CONFIG.LABEL_FORECASTER_OUT,
                                                            random_init=False,
                                                            user_init=True,
                                                            init_jobs=active_learning_settings["n_init_jobs"],
                                                            feature_cols=CONFIG.FEATURE_COLS,
                                                            label_col=active_learning_settings["target_label"],
                                                            sample_col=CONFIG.SAMPLE_COL,
                                                            sample_ids=selected_ids,
                                                            features_df=active_learning_features)

        else:
            # Later iteration
            # Save jobs
            current_iteration = status["lf_current_iteration"]
            np.savetxt(os.path.join(CONFIG.LABEL_FORECASTER_OUT, f"job_sample_ids_iteration_{current_iteration}.txt"),
                       selected_ids,
                       fmt="%d")

        # Run selected jobs
        custom_active_learning.active_learning_sampler_preparation(selected_plan_ids)
        # TODO: Add timer function here to communicate to client
        iteration_results = custom_active_learning.active_learning_iteration_helper(status["lf_current_iteration"],
                                                                                    active_learning_settings[
                                                                                        "max_iter"],
                                                                                    early_stop_th=
                                                                                    active_learning_settings[
                                                                                        "uncertainty_threshold"])

        previous_iteration = status["lf_current_iteration"]
        active_learning_duration = time.perf_counter() - start_time
        print("ITERATION RESULTS")
        # print(custom_active_learning.get_iteration_results())
        iteration_results = custom_active_learning.get_iteration_results()
        # results_dict = iteration_results["iterations"]
        # print(iteration_results["iterations_results"][previous_iteration].keys())
        # print(iteration_results["iterations_results"][previous_iteration].values())
        results_details_dict = iteration_results["iterations_results"][previous_iteration]
        results_dict = {
            "train_ids": results_details_dict["train_ids"],
            "test_ids": results_details_dict["test_ids"],
            "train_labels": results_details_dict["train_labels"].tolist(),
            "pred_labels": results_details_dict["pred_labels"].tolist(),
            "uncertainty_high": results_details_dict["uncertainty_high"].tolist(),
            "uncertainty_low": results_details_dict["uncertainty_low"].tolist(),
            "uncertainty_interval": results_details_dict["uncertainty_interval"].tolist(),

        }

        # Original workload performance
        train_ids_df = pd.DataFrame(results_details_dict["train_ids"])

        active_learning_features_id = active_learning_features_id.filter(['plan_id', 'data_id'])
        active_learning_features_id["ids"] = active_learning_features_id.index
        active_learning_features_id["ids"] = active_learning_features_id["ids"].astype("Int64")

        # print(train_ids_df.index)
        # selected_plan_ids = selected_plan_ids.filter(['plan_id', 'data_id'])
        # selected_plan_ids["ids"] = selected_plan_ids.index
        # print(selected_plan_ids["ids"])
        # selected_plan_ids["ids"] = selected_plan_ids["ids"].astype("Int64")
        merged = pd.merge(train_ids_df, active_learning_features_id, how='left', left_on=['plan_id', 'data_id'],
                          right_on=['plan_id', 'data_id'])
        print(merged["ids"].tolist())

        print(results_dict["train_labels"])
        original_workload_results = OriginalWorkloadModel.train_model(merged["ids"].tolist(),
                                                                      #CONFIG.FEATURE_COLS)
                                                                      results_dict["train_labels"])

        # print(iteration_results["cross_validation_scores"])
        results_dict.update(iteration_results["cross_validation_scores"][previous_iteration])
        iteration_results = results_dict

        # Automated sampling
        sampling_idx, uncertainties = custom_active_learning.uncertainty_sampler()
        # sampling_idx, uncertainties = custom_active_learning.top_uncertainty_sampler(200)

        # print(uncertainties)
        print(sampling_idx)

        # Get the global idx

        merged = pd.merge(sampling_idx, active_learning_features_id, how='left', left_on=['plan_id', 'data_id'],
                          right_on=['plan_id', 'data_id'])

        total_idx = merged["ids"].tolist()
        active_learning_settings["current_automated_sample_ids"] = total_idx

        # print(active_learning_settings["current_automated_sample_ids"])
        status["lf_current_iteration"] = status["lf_current_iteration"] + 1

    return render_template("index.html", iteration_results=iteration_results, status=status,
                           active_learning_settings=active_learning_settings,
                           features_df=active_learning_features.to_json(orient='records'))


@app.route("/lf_run", methods=["GET", "POST"])
def lf_run():
    global active_learning_data
    global iteration_results
    global active_learning_settings
    global active_learning_features
    global instantiated_plan_stats
    global active_learning_duration
    global original_workload
    global OriginalWorkloadModel
    global original_workload_results

    if request.method == 'POST':
        # First setup
        status["lf_config"] = request.form.to_dict()

        al_setup_params = request.form.to_dict()

        active_learning_settings["target_label"] = al_setup_params["targetLabel"]
        active_learning_settings["initial_job_selection"] = al_setup_params["initialJobSelection"]
        active_learning_settings["n_init_jobs"] = int(al_setup_params["nInitJobs"])
        active_learning_settings["uncertainty_threshold"] = int(al_setup_params["uncertaintyTh"])
        active_learning_settings["max_iter"] = int(al_setup_params["maxIter"])
        active_learning_settings["max_time"] = int(al_setup_params["maxTime"])

        features_df = active_learning_features
        sample_model = UniformAgglomerativeSampler(active_learning_settings["n_init_jobs"], CONFIG.FEATURE_COLS,
                                                   active_learning_settings["target_label"], CONFIG.SAMPLE_COL)
        # sample_model = RandomSampler(active_learning_settings["n_init_jobs"], CONFIG.FEATURE_COLS, active_learning_settings["target_label"])
        print(features_df)
        sample_ids = sample_model.fit(features_df).sample_ids
        print("INITIALLY SAMPLED IDS")
        print(sample_ids)
        print(sample_model.transform(features_df))
        sample_model.transform(features_df)
        active_learning_settings["current_automated_sample_ids"] = sample_ids.tolist()

        status["lf_current_iteration"] = 0

        return render_template("index.html",
                               status=status,
                               features_df=active_learning_features.to_dict(),
                               active_learning_settings=active_learning_settings,
                               instantiated_plan_stats=instantiated_plan_stats,
                               active_learning_duration=active_learning_duration)

    if (len(active_learning_features) == 0):
        if (CONFIG.LOAD_FROM_DISK):
            active_learning_features = pd.read_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "active_learning_features.csv"),
                                                   index_col=[0, 1])
            print(active_learning_features)
        else:
            active_learning_features = load_data_and_preprocess(CONFIG.GENERATED_METADATA_PATH,
                                                                DATA_IDS=CONFIG.DATA_IDS)
            # Persist data
            active_learning_features.to_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "active_learning_features.csv"),
                                            index_label=["plan_id", "data_id"])
        # active_learning_features = active_learning_features.droplevel(1)

    active_learning_features_json = active_learning_features.to_json()

    # Original workload
    if (len(original_workload) == 0):
        print("LOADING ORIGINAL WORKLOAD")
        original_workload = pd.read_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "original_workload.csv"))
        # Getting the right data ids, setting index
        # original_workload["data_id"] = "3GB"
        original_workload.to_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "original_workload.csv"))
        original_workload = original_workload[original_workload["data_id"].isin(CONFIG.DATA_IDS)].set_index(
            ["plan_id", "data_id"])
        print(original_workload)
        OriginalWorkloadModel = OriginalWorkloadModel(original_workload, active_learning_features)
    original_workload_json = original_workload.to_json()
    original_workload_reset = original_workload.reset_index()
    original_workload_table_json = original_workload_reset.to_json(orient='records')

    features_original_workload = pd.merge(original_workload.describe(), active_learning_features.describe(),
                                          left_index=True, right_index=True, suffixes=(' Original', ' Generated'))
    print(features_original_workload)
    original_workload_description_json = features_original_workload.reset_index().to_json(orient='records')
    # Table features
    active_learning_reset = active_learning_features.reset_index()
    active_learning_features_table_json = active_learning_reset.to_json(orient='records')
    status["lf_run"] = True
    iteration_results_json = json.dumps(iteration_results)
    # print(json.dumps(iteration_results))
    global cardinality_plan_features
    global data_plan_features
    global jobs_data_info

    # Check if already collected
    if len(instantiated_plan_stats) == 0:
        # Instantiated job info initialization
        print()
        print("GETTING INSTANTIATED PLANS INFO")
        current_sji_jobs_folder = CONFIG.GENERATED_JOB_FOLDER
        current_abs_plans_folder = CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER
        generatedJobsInfo = os.path.join(current_sji_jobs_folder, "generated_jobs_info.json")
        if (CONFIG.LOAD_FROM_DISK):
            cardinality_plan_features = pd.read_csv(
                os.path.join(CONFIG.EXPERIMENT_PATH, "cardinality_plan_features.csv"), header=[0, 1], index_col=0)
            data_plan_features = pd.read_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "data_plan_features.csv"),
                                             index_col=[0, 1], skipinitialspace=True)
            print(data_plan_features)
            jobs_data_info = pd.read_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "jobs_data_info.csv"), index_col=[0, 1],
                                         skipinitialspace=True)
        else:
            cardinality_plan_features = compute_cardinality_plan_features(generatedJobsInfo, data_sizes=CONFIG.DATA_IDS)
            data_plan_features = get_estimated_out_cardinality(generatedJobsInfo, data_sizes=CONFIG.DATA_IDS)
            print(data_plan_features)
            jobs_data_info = preprocess_jobs_data_info(generatedJobsInfo)

            # Persist data
            cardinality_plan_features.to_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "cardinality_plan_features.csv"),
                                             index_label=["plan_id", "data_id"])
            data_plan_features.to_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "data_plan_features.csv"),
                                      index_label=["plan_id", "data_id"])
            jobs_data_info.to_csv(os.path.join(CONFIG.EXPERIMENT_PATH, "jobs_data_info.csv"),
                                  index_label=["plan_id", "data_id"])

        # data_plan_features.reset_index(inplace=True)

        instantiated_plan_stats["n_gen_jobs"] = len(cardinality_plan_features)
        instantiated_plan_stats["avg_op"] = data_plan_features.groupby(["plan_id", "data_id"]).size().mean()
        instantiated_plan_stats["std_op"] = int(data_plan_features.groupby(["plan_id", "data_id"]).size().std())
        instantiated_plan_stats["op_list"] = data_plan_features.groupby(["plan_id", "data_id"]).size().values.tolist()
        instantiated_plan_stats["plan_id"] = data_plan_features.reset_index()["plan_id"].values.tolist()
        instantiated_plan_stats["op_id"] = data_plan_features["id"].values.tolist()
        instantiated_plan_stats["op_est_out_card"] = data_plan_features["outCardinality"].values.tolist()
        instantiated_plan_stats["avg_op_est_out_card"] = int(data_plan_features["outCardinality"].mean())
        instantiated_plan_stats["std_op_est_out_card"] = int(data_plan_features["outCardinality"].std())
        instantiated_plan_stats["est_source_card"] = data_plan_features.loc[
            data_plan_features["pact"] == "Data Source", "outCardinality"].values.tolist()
        data_plan_features.groupby(["plan_id", "data_id"]).size()
        instantiated_plan_stats["avg_est_source_card"] = int(
            data_plan_features.loc[data_plan_features["pact"] == "Data Source", "outCardinality"].mean())
        instantiated_plan_stats["std_est_source_card"] = int(
            data_plan_features.loc[data_plan_features["pact"] == "Data Source", "outCardinality"].std())
        instantiated_plan_stats["est_sink_card"] = data_plan_features.loc[
            data_plan_features["pact"] == "Data Sink", "outCardinality"].values.tolist()
        instantiated_plan_stats["avg_est_sink_card"] = int(
            data_plan_features.loc[data_plan_features["pact"] == "Data Sink", "outCardinality"].mean())
        instantiated_plan_stats["std_est_sink_card"] = int(
            data_plan_features.loc[data_plan_features["pact"] == "Data Sink", "outCardinality"].std())
        # status["sji"]["cardinality_plan_features"] = cardinality_plan_features.reset_index(level=1).to_dict(orient="split")
        # status["sji"]["data_plan_features"] = data_plan_features.to_dict(orient="split")
        # status["sji"]["pact_size"] = pact_size.to_frame().to_dict(orient="split")
        # jobs_data_info.reset_index(inplace=True)
        print(cardinality_plan_features)
        print("===============")
        print(data_plan_features)
        print("===============")
        print(jobs_data_info)
    jobs_data_info_json = jobs_data_info.to_json(orient='records')
    data_plan_features_json = data_plan_features.to_json(orient='records')
    # Active learning stats calculation
    global active_learning_features_stats
    if (len(active_learning_features_stats) == 0):
        feature_mean = active_learning_features.mean()
        feature_max = active_learning_features.max()
        feature_min = active_learning_features.min()
        feature_median = active_learning_features.median()
        feature_std = active_learning_features.std()
        feature_mode = active_learning_features.mode().iloc[0]

        active_learning_features_stats = pd.DataFrame(columns=active_learning_features.columns)
        active_learning_features_stats.loc["Mean"] = feature_mean
        active_learning_features_stats.loc["Median"] = feature_median
        active_learning_features_stats.loc["Mode"] = feature_mode
        active_learning_features_stats.loc["Standard Deviation"] = feature_std
        active_learning_features_stats.loc["Max"] = feature_max
        active_learning_features_stats.loc["Min"] = feature_min

    # print(active_learning_features_stats)
    # print("INSTANTIATED PLAN STATS")
    # print(instantiated_plan_stats)
    return render_template("index.html",
                           active_learning_settings=active_learning_settings,
                           status=status,
                           features_df=active_learning_features_json,
                           features_table=active_learning_features_table_json,
                           iteration_results=iteration_results_json,
                           jobs_data_info=jobs_data_info_json,
                           data_plan_features=data_plan_features_json,
                           instantiated_plan_stats=instantiated_plan_stats,
                           original_workload=original_workload_table_json,
                           original_workload_description=original_workload_description_json,
                           active_learning_duration=active_learning_duration,
                           original_workload_results=original_workload_results)  # render_template("index.html", status=status)


# json.dumps(iteration_results)
# Job explorer component: shows stats about the jobs (to be executed) for the user to review
@app.route("/lf_plan_stats/<plan_id>", methods=["GET"])
def lf_plan_stats(plan_id):
    global active_learning_features
    global iteration_results
    global active_learning_settings
    global status
    global data_plan_features
    global jobs_data_info

    if (len(active_learning_features) == 0):
        redirect("Error: run the AL component first")
    print("PLAN ID")
    print(plan_id)
    # Get abstract plan image, Get the abstract plan ID
    ap_path = f"/apg_plan/{plan_id[5]}"
    # plan_index = active_learning_features.loc[plan_id]
    # print(plan_index)

    # Insert SHAP plot

    splitted = re.sub("[\'()]", "", plan_id).split(", ")
    plan_num = splitted[0]
    data_id = splitted[1]
    print(plan_num)
    print(data_id)
    shap_path = f"/shap_plot/{plan_num}_{data_id}"
    # Insert feature row and general feature characteristics
    print(active_learning_features)
    feature_row = active_learning_features.loc[(plan_num, data_id)]


    # plan_id_num = active_learning_features.loc[active_learning_features.plan_id == plan_id].index[0]
    # print("Plan id num")
    # print(plan_id_num)
    feature_df = active_learning_features_stats
    feature_df = pd.concat([feature_row.to_frame().T, feature_df])
    feature_df.reset_index(inplace=True)
    # print(feature_df)

    # Insert instantiated job information
    # print(data_plan_features.loc[plan_id])
    # print(jobs_data_info.loc[plan_id])

    print(data_plan_features)
    print(plan_num)
    print(data_id)
    data_plan_features_job = data_plan_features.loc[(plan_num, data_id)]
    jobs_data_info_job = jobs_data_info.loc[plan_num]

    data_plan_features_job.reset_index(inplace=True)
    jobs_data_info_job.reset_index(inplace=True)

    data_plan_features_json = data_plan_features_job.to_json(orient='records')
    jobs_data_info_json = jobs_data_info_job.to_json(orient='records')

    # Insert uncertainty, predictions, set information
    iteration_results_json = json.dumps(iteration_results)
    # iteration_results_df = pd.DataFrame.from_dict(iteration_results)
    print(iteration_results)
    # print(iteration_results_df)

    # Insert similar executed job information
    # if (status["lf_current_iteration"] > 0):
    #
    # else:
    # todo: Add true label/estimated label to results
    similar_jobs_card = active_learning_features.iloc[(active_learning_features['outCardinality_mean'] - feature_row['outCardinality_mean']).abs().argsort()[:5]]
    similar_jobs_op_count = active_learning_features.iloc[(active_learning_features['outCardinality_count'] - feature_row['outCardinality_count']).abs().argsort()[:5]]

    similar_jobs_card = similar_jobs_card.reset_index()
    similar_jobs_op_count = similar_jobs_op_count.reset_index()

    # similar_jobs = []
    if (status["lf_current_iteration"] > 0):
        # Collect similar jobs
        executed_jobs = active_learning_settings["current_executed_jobs"]
        print(executed_jobs)
        # Get id of current job
        # instantiated_plan_stats["op_list"]

    # Similar tables, similar job length
    return render_template("lf_job_details.html",
                           status=status,
                           ap_image_path=ap_path,
                           shap_image_path=shap_path,
                           plan_id=plan_id,
                           features=feature_df.to_json(orient='records'),
                           iteration_results=iteration_results_json,
                           data_plan_features=data_plan_features_json,
                           jobs_data_info=jobs_data_info_json,
                           similar_jobs_card=similar_jobs_card.to_json(orient='records'),
                           similar_jobs_op_count=similar_jobs_op_count.to_json(orient='records'))


# Serves the abstract plan image
@app.route("/apg_plan/<plan_id>")
def apg_plan_serve(plan_id):
    print(CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER)
    path = os.path.join(CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER, f"apg_plan_{plan_id}.png")
    return tools._serve_image(path)


# Serves individual SHAP images for test set
@app.route("/shap_plot/<plan_id>")
def shap_plan_plot_serve(plan_id):
    # If plan ID is in tested set
    # print(plan_id)
    path = os.path.join(CONFIG.LABEL_FORECASTER_OUT, f"SHAP_{plan_id}.png")

    # if(active_learning_settings["current_executed_jobs"].includes(plan_id))
    if (os.path.exists(path)):
        return tools._serve_image(path)
    else:
        return app.send_static_file("/img/image-not-found.png")


# Serves overview SHAP images for test set
@app.route("/shap_plot/<type_plot>")
def shap_plot_serve(type_plot):
    # If plan ID is in tested set
    print(type_plot)
    path = os.path.join(CONFIG.LABEL_FORECASTER_OUT, f"SHAP_{type_plot}.png")
    if (os.path.exists(path)):
        return tools._serve_image(path)
    else:
        return tools._serve_image("/static/img/image-not-found.png")


@app.route("/apg_plan_plot/<plan_id>")
def apg_plan_plot(plan_id):
    global status
    p = status["apg_plans"][plan_id]["path"]
    # print(exp_id, plan_id, p)
    return tools._serve_image(p)


@app.route('/sji_run', methods=["POST"])
def sji_run():
    global status

    status["sji_config"] = request.form.to_dict()
    current_sji_jobs_folder = CONFIG.GENERATED_JOB_FOLDER
    current_abs_plans_folder = CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER

    try:
        os.makedirs(current_sji_jobs_folder)
    except:
        shutil.rmtree(current_sji_jobs_folder, ignore_errors=True)
        os.makedirs(current_sji_jobs_folder)

    # TODO: make data manager selecter
    current_data_manager = CONFIG.DATA_MANAGER

    n_plans = len(status["apg_plans"])
    n_versions = int(status["sji_config"]["nSjiJobs"])
    job_seed = -1
    print(f"|--> Generating '{n_plans * n_versions}' jobs in '{current_sji_jobs_folder}'")
    print()

    os.system(f'cd {CONFIG.JOB_GENERATOR}; '
              f'sbt "runMain Generator.JobGenerator {n_plans} {n_versions} {current_data_manager} {current_abs_plans_folder} {current_sji_jobs_folder} {job_seed}"')

    generatedJobsInfo = os.path.join(current_sji_jobs_folder, "generated_jobs_info.json")

    print("GENERATED JOBS INFO")
    print(generatedJobsInfo)

    global cardinality_plan_features
    global data_plan_features
    global jobs_data_info

    # TODO: Make datasize customizable
    cardinality_plan_features = compute_cardinality_plan_features(generatedJobsInfo, data_sizes=CONFIG.DATA_IDS)
    data_plan_features = get_estimated_out_cardinality(generatedJobsInfo, data_sizes=CONFIG.DATA_IDS)
    jobs_data_info = preprocess_jobs_data_info(generatedJobsInfo)

    pact_size = data_plan_features.groupby(["pact"]).size()
    pact_size = pact_size.apply(lambda x: 100 * x / pact_size.sum())

    status["sji"]["stats"] = dict()
    status["sji"]["stats"]["n_gen_jobs"] = len(cardinality_plan_features)
    status["sji"]["stats"]["n_abs_plans"] = n_plans
    status["sji"]["stats"]["n_versions"] = n_versions
    status["sji"]["stats"]["avg_op"] = data_plan_features.groupby(["plan_id", "data_id"]).size().mean()
    status["sji"]["stats"]["std_op"] = int(data_plan_features.groupby(["plan_id", "data_id"]).size().std())

    status["sji"]["stats"]["plan_id"] = data_plan_features.reset_index()["plan_id"].values.tolist()
    status["sji"]["stats"]["op_id"] = data_plan_features["id"].values.tolist()
    status["sji"]["stats"]["op_est_out_card"] = data_plan_features["outCardinality"].values.tolist()
    status["sji"]["stats"]["avg_op_est_out_card"] = int(data_plan_features["outCardinality"].mean())
    status["sji"]["stats"]["std_op_est_out_card"] = int(data_plan_features["outCardinality"].std())
    status["sji"]["stats"]["est_source_card"] = data_plan_features.loc[
        data_plan_features["pact"] == "Data Source", "outCardinality"].values.tolist()
    status["sji"]["stats"]["avg_est_source_card"] = int(
        data_plan_features.loc[data_plan_features["pact"] == "Data Source", "outCardinality"].mean())
    status["sji"]["stats"]["std_est_source_card"] = int(
        data_plan_features.loc[data_plan_features["pact"] == "Data Source", "outCardinality"].std())
    status["sji"]["stats"]["est_sink_card"] = data_plan_features.loc[
        data_plan_features["pact"] == "Data Sink", "outCardinality"].values.tolist()
    status["sji"]["stats"]["avg_est_sink_card"] = int(
        data_plan_features.loc[data_plan_features["pact"] == "Data Sink", "outCardinality"].mean())
    status["sji"]["stats"]["std_est_sink_card"] = int(
        data_plan_features.loc[data_plan_features["pact"] == "Data Sink", "outCardinality"].std())

    status["sji"]["cardinality_plan_features"] = cardinality_plan_features.reset_index(level=1).to_dict(orient="split")
    status["sji"]["data_plan_features"] = data_plan_features.to_dict(orient="split")
    status["sji"]["pact_size"] = pact_size.to_frame().to_dict(orient="split")

    status["sji_run"] = True
    return render_template("index.html", status=status)


################
# APG
################
@app.route('/apg_run', methods=["POST"])
def apg_run():
    global status
    status["apg_plans"] = dict()
    status["apg_config"] = request.form.to_dict()
    current_abs_plans_folder = CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER

    try:
        os.makedirs(current_abs_plans_folder)
    except:
        for root, dirs, files in os.walk(current_abs_plans_folder, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))

    # {'nAbsPlans': '18', 'maxOpSeqLen': '21', 'maxJoinOp': '3'}
    for i in range(int(status["apg_config"]['nAbsPlans'])):
        # Get the child/parent matrices back
        children_transition_matrix = pd.read_json(status["apg_children_tm"], orient="split")
        parent_transition_matrix = pd.read_json(status["apg_parent_tm"], orient="split")

        # Create Abstractplan generator with matrices
        abs_generator = AbstractPlanGenerator(children_transition_matrix, parent_transition_matrix, seed=i)
        G = abs_generator.generate(max_depth=int(status["apg_config"]['maxOpSeqLen']),
                                   max_joins=int(status["apg_config"]['maxJoinOp']))

        # nx.write_graphml_lxml(G, f"plan_{i}.graphml")
        # nx.write_gexf(G, f"out/plan_{i}.gexf")

        data1 = nx.json_graph.node_link_data(G)
        s1 = json.dumps(data1)
        with open(os.path.join(current_abs_plans_folder, f"plan_{i}.json"), "w") as fp:
            json.dump(data1, fp)
        print(s1)

        apg_plan_id = f"apg_plan_{i}"
        apg_plan_path = os.path.join(current_abs_plans_folder, apg_plan_id + ".png")
        nx.draw(G, graphviz_layout(G, prog='dot'), with_labels=True,
                node_color=[n[1]["color"] for n in G.nodes.data()]
                )
        plt.title(f"Plan {i}")
        plt.savefig(apg_plan_path)
        plt.close()

        status["apg_plans"][apg_plan_id] = dict({"id": apg_plan_id,
                                                 "path": apg_plan_path,
                                                 "plan": s1,
                                                 "selected": True
                                                 })

    status["apg_run"] = True
    return redirect("/#sji")  # render_template("index.html", status=status)


def compute_input_plan_stats(d):
    r = dict()
    r["n_op"] = len(d)
    r.update(d["g_pact"].value_counts().to_dict())
    return r


@app.route('/get_current_input_workload', methods=["GET"])
def get_current_input_workload():
    return jsonify(status["apg_upload_details"]["uploaded_plans"])


@app.route('/analyze_plans', methods=["GET", "POST"])
def analyze_plans():
    global reach_input_plans_table
    global status
    render_url = "/"
    reach_nodes_table = reach_input_plans_table.copy()
    # Filter out non-selected plans

    # Update plans to filter if requested
    if request.method == 'POST':
        render_url = "/#apg"
        status["plans_to_analyze"] = list(request.form.to_dict().values())

    # filter plans

    reach_nodes_table = reach_nodes_table[reach_nodes_table["plan_id"].isin(status["plans_to_analyze"])].copy()
    reach_nodes_table["g_pact"] = reach_nodes_table["g_pact"].astype('category')
    status["apg_upload_details"]["uploaded_plans"] = [
        {
            "plan_id": p,
            "selected": True if p in status["plans_to_analyze"] else False,
            "stats": compute_input_plan_stats(reach_nodes_table.loc[reach_nodes_table["plan_id"] == p, :])
            # .groupby("")
        }
        for p in status["uploaded_plans"]]

    # compute stats
    op_stats = reach_nodes_table.groupby(["g_pact"]).size().to_frame()  # .describe()#[["mean", "std"]]
    op_stats = op_stats.apply(lambda x: 100 * x / float(x.sum())).to_json(orient="split")
    print(op_stats)

    status["apg_upload_details"]["op_stats"] = op_stats
    status["apg_upload_details"]["n_plans"] = reach_nodes_table["plan_id"].nunique()
    status["apg_upload_details"]["mean_op"] = int(reach_nodes_table.groupby("plan_id").size().mean())
    status["apg_upload_details"]["std_op"] = int(reach_nodes_table.groupby("plan_id").size().std())

    expanded_reach_nodes_table = FeatureExtraction.explode_multi_in_out_nodes(reach_nodes_table)
    APG = AbstactPlanGeneratorModel()
    children_transition_matrix = APG.compute_transition_matrix(expanded_reach_nodes_table,
                                                               relationship="children",
                                                               g_pact_exp=True)

    status["apg_children_tm"] = children_transition_matrix.to_json(orient="split")
    # print(children_transition_matrix)

    parent_transition_matrix = APG.compute_transition_matrix(expanded_reach_nodes_table,
                                                             relationship="parent",
                                                             g_pact_exp=True)
    status["apg_parent_tm"] = parent_transition_matrix.to_json(orient="split")
    # print(parent_transition_matrix)

    status["apg_analyze_plans"] = True
    status["apg_run"] = False
    status["sji_run"] = False
    return redirect(render_url)  # render_template("index.html", status=status)


################
# Upload
################
@app.route('/upload_plans', methods=["POST"])
def upload_plans():
    try:
        print("|Init project")
        create_project_folders()
        input_workload_exec_plan = os.path.join(CONFIG.EXPERIMENT_PATH, "input_workload_exec_plan")
        CONFIG.ORIG_EXEC_PLAN_FOLDER = os.path.join(input_workload_exec_plan, "plans")
        if not os.path.exists(input_workload_exec_plan):

            status["exp_folder"] = input_workload_exec_plan

            os.makedirs(input_workload_exec_plan)

            new_file_name = f"plans.zip"
            output_file = os.path.join(
                input_workload_exec_plan, new_file_name
            )

            output_file_unzip = os.path.join(
                input_workload_exec_plan, 'plans'
            )

            print(list(request.files.keys()))
            file = request.files["file"]

            # we are reading the stream when checking the file, so we need to go back to the start
            file.stream.seek(0)
            file.save(output_file)

            command_seq = [
                'unzip', output_file, '-d', output_file_unzip
            ]
            subprocess.run(command_seq, shell=False, timeout=10800)

            status["apg_upload"] = True

        else:
            print(f"|--> Skip creation of original input plan folder: ")
            status["apg_upload"] = True



    except Exception as ex:
        traceback.print_stack()
        traceback.print_exc()
        return redirect(url_for("error", error_message=ex))

    load_plans()

    return redirect("/")  # render_template("index.html", status=status)


def load_plans():
    global plan_graphs
    global reach_input_plans_table
    global status

    # Load json plans
    exec_plans_json = ExecutionPlanTools.load_exec_plans(CONFIG.ORIG_EXEC_PLAN_FOLDER)

    exec_plans_graph = ExecutionPlanTools.compute_graphs_from_plans(exec_plans_json, include_cycles=False)

    reach_nodes_table = FeatureExtraction.get_plan_tables_from_plans(exec_plans_graph)
    reach_input_plans_table = reach_nodes_table.copy()
    #
    status["uploaded_plans"] = list(reach_input_plans_table["plan_id"].unique())
    status["plans_to_analyze"] = list(status["uploaded_plans"])

    return


@app.route('/')
def hello_world():
    return render_template("index.html", status=status)


if __name__ == '__main__':
    app.run(debug=True, port=5001)
