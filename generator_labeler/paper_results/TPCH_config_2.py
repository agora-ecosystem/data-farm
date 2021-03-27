from datetime import datetime

exec_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

feature_cols = ["t_0", "t_1", "t_2", "t_3", "t_4", "t_5", "t_6",
                "joinOutCard_sum", "joinOutCard_mean", "joinOutCard_std", "joinOutCard_min", "joinOutCard_max",
                "filterOutCard_mean", "filterOutCard_std", "filterOutCard_min", "filterOutCard_max",
                "groupbyOutCard_mean", "groupbyOutCard_std", "groupbyOutCard_min", "groupbyOutCard_max",
                "outCardinality_mean", "outCardinality_std", "outCardinality_min", "outCardinality_max",
                "outCardinality_kurtosis", "outCardinality_skew",
                "sourceOutCard_mean", "sourceOutCard_std", "sourceOutCard_min", "sourceOutCard_max", "sourceCardinalitySum",
                "complexity_mean", "complexity_min", "complexity_max",
                # "sourceCardinalityProd", # "joinOutCard_prod", # "sourceCardinalitySum_Joins"
                ]

label_col = "Log_netRunTime"

sample_col = "Log_sourceCardinalitySum"

data_sizes = ["1GB"]#, "5GB", "10GB", "50GB"]

n_iter = 10

dest_folder = f"./data/W3-labeling-results/{exec_timestamp}"

# plan_data_features_path = "./data/W3-labeling-results/20200922013724_2000jobs/plan_data_features_no_out.csv"
plan_data_features_path = None

td_gen_samples = [51, 142, 227, 309, 387, 461, 532, 599, 663, 723, 780, 835, 887, 935, 982, 1026, 1068, 1108, 1146, 1182]

def parse_args(args):
    params = {}

    generated_exec_plans_folders = [
        "./data/W3/could-7-2500_generated_jobs_exec_plans/1GB",
    ]

    generated_jobs_info_path = "./data/W3/generated_jobs/generated_jobs_info.json"

    params["generatedExecPlanSource"] = generated_exec_plans_folders
    params["generatedJobsInfo"] = generated_jobs_info_path

    jobs_task_manager_details = [
        "./data/W3/could-7-2500_generated_jobs_exec_plans/1GB",
    ]

    params["jobs_task_manager_details"] = jobs_task_manager_details

    params["original_jobs_exec_plans"] = [
        "./data/W1/original_jobs_exec_plans/1GB",
        "./data/W1/original_jobs_exec_plans/5GB",
        "./data/W1/original_jobs_exec_plans/10GB",
        "./data/W1/original_jobs_exec_plans/50GB",
    ]

    params[
        "original_Cardinality_features"] = "./data/W1/original_jobs_exec_plans/original_Cardinality_features_2.csv"

    return params