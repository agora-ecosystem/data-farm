from datetime import datetime

exec_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

feature_cols = ["t_0", "t_1", "t_2", "t_3", "t_4",
                "joinOutCard_sum", "joinOutCard_mean", "joinOutCard_std", "joinOutCard_min", "joinOutCard_max",
                "filterOutCard_mean", "filterOutCard_std", "filterOutCard_min", "filterOutCard_max",
                "groupbyOutCard_mean", "groupbyOutCard_std", "groupbyOutCard_min", "groupbyOutCard_max",
                "outCardinality_mean", "outCardinality_std", "outCardinality_min", "outCardinality_max",
                "outCardinality_kurtosis", "outCardinality_skew",
                "sourceOutCard_mean", "sourceOutCard_std", "sourceOutCard_min", "sourceOutCard_max", "sourceCardinalitySum",
                "complexity_mean", "complexity_min", "complexity_max"
                ]

label_col = "Log_netRunTime"

sample_col = "Log_sourceCardinalitySum"

data_sizes = ["3GB"]

n_iter = 20

init_jobs = 10

dest_folder = f"./data/{exec_timestamp}"

plan_data_features_path = None

def parse_args(args):
    params = {}

    generated_exec_plans_folders = [
        "./data/W2/cloud-7_generated_jobs_exec_plans/3GB",
    ]

    generated_jobs_info_path = "./data/Experiment1/generated_jobs/generated_jobs_info.json"

    params["generatedExecPlanSource"] = generated_exec_plans_folders
    params["generatedJobsInfo"] = generated_jobs_info_path

    jobs_task_manager_details = [
        "./data/W2/cloud-7_generated_jobs_task_manager_details/3GB",
    ]

    params["jobs_task_manager_details"] = jobs_task_manager_details

    params["original_jobs_exec_plans"] = []

    params["original_Cardinality_features"] = ""

    return params