from datetime import datetime

BUILD_SBT = "build.sbt"

RUN = True

# DATA GENERATORS HOME
ABSTRACT_PLAN_GENERATOR = "./generator_labeler"
JOB_GENERATOR = "./instantiator"

# Path to original exec plans
ORIG_EXEC_PLAN_FOLDER = "/Users/francescoventura/PycharmProjects/data-farm/data/input_workload_exec_plan/"

# PATH TO INPUT DATA
GENERATED_JOB_INPUT_DATA_PATH = "/Users/francescoventura/data/tpc-h/"

# Path of generated data destination
BASE_PATH = "/Users/francescoventura/PycharmProjects/data-farm/data/"
EXPERIMENT_ID = "Experiment1/"
DATA_ID = "1GB"

# Job generator conf
N_JOBS = 10
N_VERSIONS = 3
JOB_SEED = -1  # -1 | 0
DATA_MANAGER = "TPCH" # "TPCH" | "IMDB"


EXPERIMENT_PATH = BASE_PATH + EXPERIMENT_ID

GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER = EXPERIMENT_PATH + "generated_abstract_exec_plans"
GENERATED_JOB_FOLDER = EXPERIMENT_PATH + "generated_jobs"
GENERATED_JOB_EXEC_PLAN_FOLDER = EXPERIMENT_PATH + "generated_jobs_exec_plans/"

GENERATED_JOB_TASK_MANAGER_DETAILS = EXPERIMENT_PATH + "generated_jobs_task_manager_details/"


# SUBMIT CONF
# Path where to store the execution plans of the generated jobs after their execution
GENERATED_JOB_OUTPUT_PLAN_PATH = GENERATED_JOB_EXEC_PLAN_FOLDER + DATA_ID + "/"
GENERATED_JOB_TASK_MANAGER_DETAILS_OUTPUT_PATH = GENERATED_JOB_TASK_MANAGER_DETAILS + DATA_ID + "/"


# Job Submission config
LOCAL = "nolocal"  # [local|nolocal]
LOCAL_HEAP = "8GB"  # [8GB|none]
PARALLELISM = "1"

# FLINK HOME
FLINK_HOME = "/Users/francescoventura/flink-1.10.0"

# FLINK_TASK_MANAGER_URL
FLINK_TASK_MANAGER_URL = "http://127.0.0.1:8081/"



########################################################
## Config Label Forecaster

exec_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

LABEL_COL = "Log_netRunTime"

SAMPLE_COL = "Log_sourceCardinalitySum"

FEATURE_COLS = ["t_0", "t_1", "t_2", "t_3", "t_4", "t_5", "t_6",
                "joinOutCard_sum", "joinOutCard_mean", "joinOutCard_std", "joinOutCard_min", "joinOutCard_max",
                "filterOutCard_mean", "filterOutCard_std", "filterOutCard_min", "filterOutCard_max",
                "groupbyOutCard_mean", "groupbyOutCard_std", "groupbyOutCard_min", "groupbyOutCard_max",
                "outCardinality_mean", "outCardinality_std", "outCardinality_min", "outCardinality_max",
                "outCardinality_kurtosis", "outCardinality_skew",
                "sourceOutCard_mean", "sourceOutCard_std", "sourceOutCard_min", "sourceOutCard_max", "sourceCardinalitySum",
                "complexity_mean", "complexity_min", "complexity_max"
                ]

MAX_EARLY_STOP = 2
MAX_ITER = 5
INIT_JOBS = 10
RANDOM_INIT = False

GENERATED_METADATA_PATH = GENERATED_JOB_FOLDER + "/generated_jobs_info.json"

LABEL_FORECASTER_OUT = EXPERIMENT_PATH + exec_timestamp
