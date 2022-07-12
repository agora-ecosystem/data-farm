from datetime import datetime
from os.path import join


class CONFIG:

    # PATH TO DATA-FARM PROJECT
    PROJECT_PATH = "/mnt/c/Users/Robin/Documents/Git/Datafarm/data-farm" # <absolute_path_to_project>

    # PATH TO INPUT DATA
    GENERATED_JOB_INPUT_DATA_PATH = "/mnt/c/Users/Robin/Documents/experimentdata/1GB/" #"/mnt/c/Users/Robin/Documents/Git/Datafarm/Dataplans/misc/20200829163549_good" # <absolute_path_to_input_data>
    #GENERATED_JOB_INPUT_DATA_PATH = "/mnt/c/Users/Robin/Documents/experimentdata/IMDB/" #"/mnt/c/Users/Robin/Documents/Git/Datafarm/Dataplans/misc/20200829163549_good" # <absolute_path_to_input_data>

    # FLINK HOME
    FLINK_HOME = "/mnt/c/Users/Robin/Documents/Flink/flink-1.10.0" # <path_to_flink>

    # EXPERIMENT ID
    EXPERIMENT_ID = "Experiment1"
    # DATA ID
    #DATA_ID = "3GB"
    # ACTIVE LEARNING DATA IDS
    DATA_IDS = ["1GB"]
    #DATA_IDS = ["3GB"]

    # # EXPERIMENT ID
    # EXPERIMENT_ID = "Experiment3"
    # # DATA ID
    DATA_ID = "1GB"
    # # ACTIVE LEARNING DATA IDS
    # DATA_IDS = ["1GB"]

    # Job generator conf
    N_JOBS = 10
    N_VERSIONS = 3
    JOB_SEED = -1  # -1 | 0
    DATA_MANAGER = "TPCH" # "TPCH" | "IMDB"
    #DATA_MANAGER = "IMDB"
    #################################################################

    # DATA GENERATORS HOME
    ABSTRACT_PLAN_GENERATOR = join(PROJECT_PATH, "generator_labeler")
    JOB_GENERATOR = join(PROJECT_PATH, "instantiator")

    # Path to original exec plans
    ORIG_EXEC_PLAN_FOLDER = join(PROJECT_PATH, "data/input_workload_exec_plan/") # <absolute_path_to_input_workload>

    # Path of generated data destination
    BASE_PATH = join(PROJECT_PATH, "data/") # <absolute_path_to_input_data>

    EXPERIMENT_PATH = join(BASE_PATH, EXPERIMENT_ID+"/")

    GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER = join(EXPERIMENT_PATH, "generated_abstract_exec_plans")
    GENERATED_JOB_FOLDER = join(EXPERIMENT_PATH, "generated_jobs")
    GENERATED_JOB_EXEC_PLAN_FOLDER = join(EXPERIMENT_PATH, "generated_jobs_exec_plans")

    GENERATED_JOB_TASK_MANAGER_DETAILS = join(EXPERIMENT_PATH, "generated_jobs_task_manager_details")
    print(GENERATED_JOB_TASK_MANAGER_DETAILS)

    # SUBMIT CONF
    # Path where to store the execution plans of the generated jobs after their execution
    GENERATED_JOB_OUTPUT_PLAN_PATH = join(GENERATED_JOB_EXEC_PLAN_FOLDER, DATA_ID+"/")
    GENERATED_JOB_TASK_MANAGER_DETAILS_OUTPUT_PATH = join(GENERATED_JOB_TASK_MANAGER_DETAILS, DATA_ID+"/")


    # Job Submission config
    LOCAL = "nolocal"  # [local|nolocal]
    LOCAL_HEAP = "8GB"  # [8GB|none]
    PARALLELISM = "1"

    # FLINK_TASK_MANAGER_URL
    FLINK_TASK_MANAGER_URL = "http://127.0.0.1:8081/"

    BUILD_SBT = "build.sbt"

    RUN = True

    ########################################################
    ## Config Label Forecaster #############################
    ########################################################

    exec_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    LABEL_COL = "Log_netRunTime"

    SAMPLE_COL = "Log_sourceCardinalitySum"

    # Feature cols tpch
    FEATURE_COLS = ["t_0", "t_1", "t_2", "t_3", "t_4", #"t_5", "t_6",
                    "joinOutCard_sum", "joinOutCard_mean", "joinOutCard_std", "joinOutCard_min", "joinOutCard_max",
                    "filterOutCard_mean", "filterOutCard_std", "filterOutCard_min", "filterOutCard_max",
                    "groupbyOutCard_mean", "groupbyOutCard_std", "groupbyOutCard_min", "groupbyOutCard_max",
                    "outCardinality_mean", "outCardinality_std", "outCardinality_min", "outCardinality_max",
                    "outCardinality_kurtosis", "outCardinality_skew",
                    "sourceOutCard_mean", "sourceOutCard_std", "sourceOutCard_min", "sourceOutCard_max", "sourceCardinalitySum",
                    "complexity_mean", "complexity_min", "complexity_max"
                    ]

    # Feature cols imdb generated
    # FEATURE_COLS = ["t_0", "t_1", "t_2", "t_3", "t_4", #"t_5", "t_6",
    #                 "joinOutCard_sum", "joinOutCard_mean", "joinOutCard_std", "joinOutCard_min", "joinOutCard_max",
    #                 "filterOutCard_mean", "filterOutCard_std", "filterOutCard_min", "filterOutCard_max",
    #                 #"groupbyOutCard_mean", "groupbyOutCard_std", "groupbyOutCard_min", "groupbyOutCard_max",
    #                 "outCardinality_mean", "outCardinality_std", "outCardinality_min", "outCardinality_max",
    #                 "outCardinality_kurtosis", "outCardinality_skew",
    #                 "sourceOutCard_mean", "sourceOutCard_std", "sourceOutCard_min", "sourceOutCard_max", "sourceCardinalitySum",
    #                 "complexity_mean", "complexity_min", "complexity_max"
    #                 ]
    MAX_EARLY_STOP = 2
    EARLY_STOP_TH = 0.1 # Float between 0 and 1
    MAX_ITER = 5
    INIT_JOBS = 190
    RANDOM_INIT = False
    RANDOM_SAMPLING = False

    USER_INIT = False
    USER_PROMPT = False

    GENERATED_METADATA_PATH = join(GENERATED_JOB_FOLDER, "generated_jobs_info.json")

    LABEL_FORECASTER_OUT = join(EXPERIMENT_PATH, "label_forecaster_" + exec_timestamp)
    ########################################################
    ## Config GUI #############################
    ########################################################
    SHAP_IMAGES = True
    LOAD_FROM_DISK = True