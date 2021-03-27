BUILD_SBT = "build.sbt"

RUN = True

# DATA GENERATORS HOME
ABSTRACT_PLAN_GENERATOR = "./generator_labeler"
JOB_GENERATOR = "./instantiator"

# Path to original exec plans
ORIG_EXEC_PLAN_FOLDER = "../data/input_workload_exec_plan/"

# FLINK_TASK_MANAGER_URL
FLINK_TASK_MANAGER_URL = "http://127.0.0.1:8081/"

# Job generator conf
N_JOBS = 10
N_VERSIONS = 3
JOB_SEED = -1  # -1 | 0
DATA_MANAGER = "TPC_H" # "TPC_H" | "IMBDK"

# Path to generated data
BASE_PATH = "../data/"
EXPERIMENT_ID = "Experiment1/" #

EXPERIMENT_PATH = BASE_PATH + EXPERIMENT_ID

GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER = EXPERIMENT_PATH + "generated_abstract_exec_plans"
GENERATED_JOB_FOLDER = EXPERIMENT_PATH + "generated_jobs"
GENERATED_JOB_EXEC_PLAN_FOLDER = EXPERIMENT_PATH + "generated_jobs_exec_plans/"

GENERATED_JOB_TASK_MANAGER_DETAILS = EXPERIMENT_PATH + "generated_jobs_task_manager_details/"

# SUBMIT CONF
# Path where to store the execution plans of the generated jobs after their execution
# GENERATED_JOB_OUTPUT_PLAN_PATH = GENERATED_JOB_EXEC_PLAN_FOLDER + "3GB/"

# GENERATED_JOB_TASK_MANAGER_DETAILS_OUTPUT_PATH = GENERATED_JOB_TASK_MANAGER_DETAILS + "3GB/"

# PATH TO DATA SOURCE
# GENERATED_JOB_INPUT_DATA_PATH = ""
# GENERATED_JOB_INPUT_DATA_PATH = ""

# LOCAL config
LOCAL = "nolocal"  # [local|nolocal]
LOCAL_HEAP = "8GB"  # [8GB|none]
PARALLELISM = "1"

# FLINK HOME
FLINK_HOME = "./flink-1.10.0"
