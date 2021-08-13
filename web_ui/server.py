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
import tools
from CONFIG import CONFIG
import matplotlib.pyplot as plt
import seaborn as sns

from RunLabelForecaster import set_up_active_learning, load_data_and_preprocess
from generator_labeler import ExecutionPlanTools
from generator_labeler.FeatureExtraction import FeatureExtraction
from networkx.drawing.nx_agraph import graphviz_layout
import pickle

from generator_labeler.FeatureExtraction.PredictorFeatureExtraction import compute_cardinality_plan_features, \
    get_estimated_out_cardinality, preprocess_jobs_data_info
from generator_labeler.Generator.AbstractPlan import AbstractPlanGenerator, AbstactPlanGeneratorModel

import numpy as np

from RunGenerator import create_project_folders
from generator_labeler.JobExecutionSampler.unsupervised_sampler import UniformAgglomerativeSampler

sns.set_context("talk")
sns.set_style("whitegrid")

# from DataFarm.Generator import AbstractPlan as GAP
# from DataFarm.Generator.AbstractPlan import AbstactPlanGeneratorModel


app = Flask(__name__)
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
active_learning_features_stats={}


@app.route("/lf_iteration", methods=["GET", "POST"])
def lf_iteration():
    global iteration_results
    global active_learning_settings
    global status
    global active_learning_features
    global active_learning_features_id

    if request.method == 'POST':
        print(request.get_data())
        print(request.get_json())
        selected_jobs = request.get_json()["selected_jobs"]

        active_learning_features = load_data_and_preprocess(CONFIG.GENERATED_METADATA_PATH, CONFIG.DATA_ID)

        active_learning_features_id = active_learning_features.droplevel(1)
        active_learning_features_id = active_learning_features_id.reset_index()

        selected_plan_ids = active_learning_features_id.loc[active_learning_features_id['plan_id'].isin(selected_jobs)]
        selected_ids = selected_plan_ids.index.values.tolist()

        # Update list of executed jobs
        active_learning_settings["current_executed_jobs"].extend(selected_ids)
        print("Currently executed jobs")
        print(active_learning_settings["current_executed_jobs"])

        global custom_active_learning
        if status["lf_current_iteration"] < 1:
            # First iteration: set up active learning strategy object, sample and run initial jobs
            try:
                global LABEL_FORECASTER_OUT
                LABEL_FORECASTER_OUT = CONFIG.LABEL_FORECASTER_OUT
                os.mkdir(LABEL_FORECASTER_OUT)
            except Exception as ex:
                print(f"Experiment '{CONFIG.LABEL_FORECASTER_OUT}' already exists!")
                sys.exit(1)

            custom_active_learning = set_up_active_learning(generated_metadata_path=CONFIG.GENERATED_METADATA_PATH,
                                                            data_id=CONFIG.DATA_ID,
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
            # Run selected jobs
            custom_active_learning.active_learning_sampler_preparation(selected_plan_ids)

        # TODO: Add timer function here to communicate to client
        iteration_results = custom_active_learning.active_learning_iteration_helper(status["lf_current_iteration"],
                                                                                    active_learning_settings[
                                                                                        "max_iter"],
                                                                                    early_stop_th=
                                                                                    active_learning_settings[
                                                                                        "uncertainty_threshold"])

        status["lf_current_iteration"] = status["lf_current_iteration"] + 1
        print("ITERATION RESULTS")
        print(custom_active_learning.get_iteration_results())
        iteration_results = custom_active_learning.get_iteration_results()
        # results_dict = iteration_results["iterations"]
        print(iteration_results["iterations_results"][0].keys())
        print(iteration_results["iterations_results"][0].values())
        results_details_dict = iteration_results["iterations_results"][0]
        results_dict = {
            "iterations": iteration_results["iterations"],
            "train_ids": results_details_dict["train_ids"],
            "test_ids": results_details_dict["test_ids"],
            "train_labels": results_details_dict["train_labels"].tolist(),
            "pred_labels": results_details_dict["pred_labels"].tolist(),
            "uncertainty_high": results_details_dict["uncertainty_high"].tolist(),
            "uncertainty_low": results_details_dict["uncertainty_low"].tolist(),
            "uncertainty_interval": results_details_dict["uncertainty_interval"].tolist(),

        }
        iteration_results = results_dict

        # Automated sampling
        sampling_idx, uncertainties = custom_active_learning.uncertainty_sampler()
        # print(uncertainties)
        # print(sampling_idx)

        # Get the global idx
        sampling_idx = active_learning_features_id.loc[active_learning_features_id['plan_id'].isin(sampling_idx)]
        total_idx = sampling_idx.index.values.tolist()
        active_learning_settings["current_automated_sample_ids"] = total_idx
        print(active_learning_settings["current_automated_sample_ids"])

    return render_template("index.html", iteration_results=json.dumps(iteration_results), status=status,
                           active_learning_settings=active_learning_settings,
                           features_df=active_learning_features.to_json(orient='records'))


@app.route("/lf_run", methods=["GET", "POST"])
def lf_run():
    global active_learning_data
    global iteration_results
    global active_learning_settings
    global active_learning_features

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

        features_df = load_data_and_preprocess(CONFIG.GENERATED_METADATA_PATH, CONFIG.DATA_ID)
        sample_model = UniformAgglomerativeSampler(active_learning_settings["n_init_jobs"], CONFIG.FEATURE_COLS,
                                                   active_learning_settings["target_label"], CONFIG.SAMPLE_COL)
        sample_ids = sample_model.fit(features_df).sample_ids
        active_learning_settings["current_automated_sample_ids"] = sample_ids.tolist()

        status["lf_current_iteration"] = 0

        return render_template("index.html", status=status, features_df=active_learning_features.to_dict(),
                               active_learning_settings=active_learning_settings)

    active_learning_features = load_data_and_preprocess(CONFIG.GENERATED_METADATA_PATH, CONFIG.DATA_ID)
    active_learning_features = active_learning_features.droplevel(1)
    active_learning_features_json = active_learning_features.to_json()

    # Table features
    active_learning_reset = active_learning_features.reset_index()
    active_learning_features_table_json = active_learning_reset.to_json(orient='records')
    status["lf_run"] = True
    iteration_results_json = json.dumps(iteration_results)
    print(json.dumps(iteration_results))

    # Instantiated job info initialization
    global cardinality_plan_features
    global data_plan_features
    global jobs_data_info
    print()
    print("GETTING INSTANTIATED PLANS INFO")
    current_sji_jobs_folder = CONFIG.GENERATED_JOB_FOLDER
    current_abs_plans_folder = CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER
    generatedJobsInfo = os.path.join(current_sji_jobs_folder, "generated_jobs_info.json")
    cardinality_plan_features = compute_cardinality_plan_features(generatedJobsInfo, data_sizes=["1GB"])
    data_plan_features = get_estimated_out_cardinality(generatedJobsInfo, data_sizes=["1GB"])
    jobs_data_info = preprocess_jobs_data_info(generatedJobsInfo)
    # data_plan_features.reset_index(inplace=True)

    # jobs_data_info.reset_index(inplace=True)
    print(cardinality_plan_features)
    print("===============")
    print(data_plan_features)
    print("===============")
    print(jobs_data_info)

    # Active learning stats calculation
    global active_learning_features_stats

    feature_mean = active_learning_features.mean()
    feature_max = active_learning_features.max()
    feature_min = active_learning_features.min()
    feature_median = active_learning_features.median()
    feature_std = active_learning_features.std()
    feature_mode = active_learning_features.mode().iloc[0]

    active_learning_features_stats = pd.DataFrame(columns=active_learning_features.columns)
    active_learning_features_stats.loc["Mean"]=feature_mean
    active_learning_features_stats.loc["Median"] = feature_median
    active_learning_features_stats.loc["Mode"] = feature_mode
    active_learning_features_stats.loc["Standard Deviation"] = feature_std
    active_learning_features_stats.loc["Max"] = feature_max
    active_learning_features_stats.loc["Min"] = feature_min

    print(active_learning_features_stats)

    return render_template("index.html", active_learning_settings=active_learning_settings,
                           status=status,
                           features_df=active_learning_features_json,
                           features_table = active_learning_features_table_json,
                           iteration_results=iteration_results_json)  # render_template("index.html", status=status)


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

    # Get abstract plan image, Get the abstract plan ID
    ap_path = f"/apg_plan/{plan_id[3]}"
    plan_index = active_learning_features.loc[plan_id]
    print(plan_index)

    # Insert SHAP plot
    shap_path = f"/shap_plot/{plan_id}"

    # Insert feature row and general feature characteristics
    feature_row = active_learning_features.loc[plan_id]
    feature_df = active_learning_features_stats
    feature_df = pd.concat([feature_row.to_frame().T,feature_df])
    feature_df.reset_index(inplace=True)
    # print(feature_df)

    #Insert instantiated job information
    # print(data_plan_features.loc[plan_id])
    # print(jobs_data_info.loc[plan_id])

    data_plan_features_job = data_plan_features.loc[plan_id]
    jobs_data_info_job = jobs_data_info.loc[plan_id]

    data_plan_features_job.reset_index(inplace=True)
    jobs_data_info_job.reset_index(inplace=True)

    data_plan_features_json = data_plan_features_job.to_json(orient='records')
    jobs_data_info_json = jobs_data_info_job.to_json(orient='records')


    # Insert uncertainty, predictions, set information
    iteration_results_json = json.dumps(iteration_results)
    # iteration_results_df = pd.DataFrame.from_dict(iteration_results)
    print(iteration_results)
    # print(iteration_results_df)


    # TODO: Insert similar executed job information
    # Similar tables, similar job length
    return render_template("lf_job_details.html",
                           ap_image_path=ap_path,
                           shap_image_path=shap_path,
                           plan_id=plan_id,
                           features=feature_df.to_json(orient='records'),
                           iteration_results=iteration_results_json,
                           data_plan_features=data_plan_features_json,
                           jobs_data_info=jobs_data_info_json)


# Serves the abstract plan image
@app.route("/apg_plan/<plan_id>")
def apg_plan_serve(plan_id):
    print(CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER)
    path = os.path.join(CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER, f"apg_plan_{plan_id}.png")
    return tools._serve_image(path)


# Serves individual SHAP images for test set
@app.route("/shap_plot/<plan_id>")
def shap_plot_serve(plan_id):
    # If plan ID is in tested set
    print(plan_id)
    print("CURRENT EXECUTED JOBS")
    print(active_learning_settings["current_executed_jobs"])
    path = os.path.join(CONFIG.LABEL_FORECASTER_OUT, f"SHAP_Bar_{plan_id}.png")

    # if(active_learning_settings["current_executed_jobs"].includes(plan_id))

    return tools._serve_image(path)


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

    global cardinality_plan_features
    global data_plan_features
    global jobs_data_info

    # TODO: Make datasize customizable
    cardinality_plan_features = compute_cardinality_plan_features(generatedJobsInfo, data_sizes=["1GB"])
    data_plan_features = get_estimated_out_cardinality(generatedJobsInfo, data_sizes=["1GB"])
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
