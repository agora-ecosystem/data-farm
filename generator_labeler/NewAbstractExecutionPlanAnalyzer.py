import generator_labeler.ExecutionPlanTools as planTools
from generator_labeler.FeatureExtraction import FeatureExtraction
from generator_labeler.Generator.AbstractPlan import AbstactPlanGeneratorModel
import os
import seaborn as sns

import sys

sns.set_context("talk")
sns.set_style("whitegrid")


#experiment_version = "Experiment4/"

#tmp_orig_exec_plan_folder = "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/original_execution_plans_local/0GB"
#dest_folder = "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment4/generated_abstract_exec_plans/"
#n_jobs_to_generate = 10


def parse_args(args):
    params = {}
    if args.__len__() == 4:
        params["nJobs"] = int(args[1])
        params["originalExecPlanSource"] = args[2]
        params["genAbsPlanDest"] = args[3]
    else:
        raise Exception("Expected arguments: <nJobs> <originalExecPlanSource> <genAbsPlanDest>")
    return params

if __name__ == '__main__':

    params = parse_args(sys.argv)

    print(os.getcwd())

    if not os.path.exists(params["genAbsPlanDest"]):
        raise Exception(f"Destination folder for genAbsPlanDest '{params['genAbsPlanDest']}' does not exists.")

    exec_plans_json = planTools.load_exec_plans(params["originalExecPlanSource"])

    exec_plans_json = [ep for ep in exec_plans_json if ("TPC_H" in ep[0])]

    print(exec_plans_json)

    exec_plans_graph = planTools.compute_graphs_from_plans(exec_plans_json, include_cycles=False)

    absModel = AbstactPlanGeneratorModel(max_depth=6, max_joins=3, seed=42)

    absModel.fit(exec_plans_graph)

    generatedAbstactractPlans = absModel.predict(params["nJobs"])

    for pg in generatedAbstactractPlans:
        #planTools.show_generated_exec_plan_graph(pg)
        planTools.save_generated_exec_plan_graph(pg, params["genAbsPlanDest"], save_graph_plot=True)
