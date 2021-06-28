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
from generator_labeler import ExecutionPlanTools
from generator_labeler.FeatureExtraction import FeatureExtraction
from networkx.drawing.nx_agraph import graphviz_layout
import pickle

from generator_labeler.Generator.AbstractPlan import AbstactPlanGeneratorModel

# from DataFarm.FeatureExtraction import FeatureExtraction
# import DataFarm.ExecutionPlanTools as planTools
# from DataFarm.FeatureExtraction.PredictorFeatureExtraction import compute_cardinality_plan_features, \
#     preprocess_jobs_data_info, get_estimated_out_cardinality, fill_jobs_cardinality


import numpy as np

from RunGenerator import create_project_folders

sns.set_context("talk")
sns.set_style("whitegrid")

# from DataFarm.Generator import AbstractPlan as GAP
# from DataFarm.Generator.AbstractPlan import AbstactPlanGeneratorModel

app = Flask(__name__)
app.config.from_object("config.DataFarmConfig")
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
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



@app.route("/lf_next_iter", methods=["GET", "POST"])
def lf_next_iter():

    base_folder = "/Users/francescoventura/PycharmProjects/DataFarm-App/misc"
    experiment_id = "20200829163549_good"  # Good
    # experiment_id = "20200829165836_prod_feat" # With product features, BAD!
    data_file = f"{base_folder}/{experiment_id}/learning_process.pkl"
    with open(data_file, "rb") as handle:
        learning_data = pickle.load(handle)

    status["early_stop"] = False
    while (not status["early_stop"]):
        if (status["lf_current_iteration"] >= status["max_iter"]):
            break

        if (status["lf_current_iteration"] < 0) and (request.method == 'POST'):
            status["lf_config"] = request.form.to_dict()
            status["max_iter"] = int(status["lf_config"]["maxIter"])
            status["lf_current_iteration"] = 0
            iteration_to_show = status["lf_current_iteration"]
            n_train = int(status["lf_config"]["nInitJobs"])
            n_test = max(0, int(status["sji"]["stats"]["n_gen_jobs"]) - n_train)
        else:
            status["lf_current_iteration"] = status["lf_current_iteration"] + 1
            iteration_to_show = status["lf_current_iteration"]
            n_train = int(status["lf_iterations"][iteration_to_show-1]["n_exec"]) + iteration_to_show * 2
            n_test = max(0, int(status["sji"]["stats"]["n_gen_jobs"]) - n_train)

        jobs, unc, unc_std = tools.get_iteretation_details(learning_data, iteration_to_show, n_train=n_train, n_test=n_test)
        tot_exec_time = sum([60000 + j["execution_time"] for j in jobs if j["executed"]])

        status["lf_iterations"].append({
            "n_exec": n_train,
            "n_forecast": n_test,
            "tot_exec_time": tot_exec_time,
            "jobs": jobs,
            "total_uncertainty": unc,
            "total_uncertainty_std": unc_std
        })
        if int(status["lf_current_iteration"]) in [2, 10, 12]:
            status["early_stop"] = True

    time.sleep(15)

    return redirect("/")  # render_template("index.html", status=status)



@app.route("/lf_run", methods=["GET", "POST"])
def lf_run():
    # {
    #     "job_id": "",
    #     "executed": False,
    #     "selected": False,
    #     "uncertainty": 0.0,
    #     "tot_uncertainty": 0.0,
    #
    # }
    
    # if request.method is "POST":
    #     status["lf_current_iteration"] = status["lf_current_iteration"] + 1

    # Update plans to filter if requested

    if request.method == 'POST':
        status["lf_config"] = request.form.to_dict()

        base_folder = "/Users/francescoventura/PycharmProjects/DataFarm-App/misc"
        experiment_id = "20200829163549_good"  # Good

        # experiment_id = "20200829165836_prod_feat" # With product features, BAD!

        data_file = f"{base_folder}/{experiment_id}/learning_process.pkl"

        with open(data_file, "rb") as handle:
            learning_data = pickle.load(handle)

        status["lf_current_iteration"] = 0
        iteration_to_show = status["lf_current_iteration"]
        n_train = int(status["lf_config"]["nInitJobs"])
        n_test = max(0, int(status["sji"]["stats"]["n_gen_jobs"]) - n_train)

        jobs, unc, unc_std = tools.get_iteretation_details(learning_data, iteration_to_show, n_train=n_train, n_test=n_test)
        tot_exec_time = sum([60000 + j["execution_time"] for j in jobs if j["executed"]])
        status["lf_iterations"].append({
            "n_exec": n_train,
            "n_forecast": n_test,
            "tot_exec_time": tot_exec_time,
            "jobs": jobs,
            "total_uncertainty": unc,
            "total_uncertainty_std": unc_std
        })
        # return render_template("index.html", status=status)

    status["lf_run"] = True
    return redirect("/")  # render_template("index.html", status=status)


@app.route("/apg_plan_plot/<exp_id>/<plan_id>")
def apg_plan_plot(exp_id, plan_id):
    global status
    p = status["apg_plans"][plan_id]["path"]
    print(exp_id, plan_id, p)
    return tools._serve_image(p)


@app.route('/sji_run', methods=["POST"])
def sji_run():
    global status

    status["sji_config"] = request.form.to_dict()
    current_sji_jobs_folder = os.path.join(app.config["OUT_FOLDER"], status["exp_folder"],
                                           "generated_jobs") + "/"
    current_abs_plans_folder = os.path.join(app.config["OUT_FOLDER"], status["exp_folder"],
                                            "generated_abstract_exec_plans")

    try:
        os.makedirs(current_sji_jobs_folder)
    except:
        shutil.rmtree(current_sji_jobs_folder, ignore_errors=True)
        os.makedirs(current_sji_jobs_folder)

    current_data_manager = "IMDBK"

    n_plans = len(status["apg_plans"])
    n_versions = int(status["sji_config"]["nSjiJobs"])
    job_seed = -1
    print(f"|--> Generating '{n_plans * n_versions}' jobs in '{current_sji_jobs_folder}'")
    print()

    os.system(f'cd {app.config["JOB_GENERATOR"]}; '
              f'sbt "runMain Generator.JobGenerator {n_plans} {n_versions} {current_data_manager} {current_abs_plans_folder} {current_sji_jobs_folder} {job_seed}"')

    generatedJobsInfo = os.path.join(current_sji_jobs_folder, "generated_jobs_info.json")

    global cardinality_plan_features
    global data_plan_features
    global jobs_data_info

    cardinality_plan_features = compute_cardinality_plan_features(generatedJobsInfo, data_sizes=["3GB"])
    data_plan_features = get_estimated_out_cardinality(generatedJobsInfo, data_sizes=["3GB"])
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
    current_abs_plans_folder = os.path.join(app.config["OUT_FOLDER"], status["exp_folder"],
                                            "generated_abstract_exec_plans")
    try:
        os.makedirs(current_abs_plans_folder)
    except:
        for root, dirs, files in os.walk(current_abs_plans_folder, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))

    # {'nAbsPlans': '18', 'maxOpSeqLen': '21', 'maxJoinOp': '3'}
    for i in range(int(status["apg_config"]['nAbsPlans'])):
        children_transition_matrix = pd.read_json(status["apg_children_tm"], orient="split")
        parent_transition_matrix = pd.read_json(status["apg_parent_tm"], orient="split")
        abs_generator = GAP.AbstractPlanGenerator(children_transition_matrix, parent_transition_matrix, seed=i)
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
    return redirect("/#sji") # render_template("index.html", status=status)


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
    return redirect(render_url) #render_template("index.html", status=status)


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

    return redirect("/") # render_template("index.html", status=status)


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

    # current_plans_folder = os.path.join(app.config["OUT_FOLDER"], status["exp_folder"], "plans")
    # print(app.config["OUT_FOLDER"])
    # print(f"|--> Generating '{CONFIG.N_JOBS}' abstract plans in '{CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER}'")
    # print()
    #
    # os.system(f'cd {CONFIG.ABSTRACT_PLAN_GENERATOR}; '
    #           f'python3 NewAbstractExecutionPlanAnalyzer.py {CONFIG.N_JOBS} {CONFIG.ORIG_EXEC_PLAN_FOLDER} {CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER}')


    return


@app.route('/')
def hello_world():
    return render_template("index.html", status=status)


if __name__ == '__main__':
    app.run(debug=True, port=5001)
