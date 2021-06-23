# DataFarm

DataFarm is an innovative framework for efficiently generating and labeling large query workloads. 
It follows a data-driven & white-box approach to learn from pre-existing small workload patterns, input data, and computational resources. 
Thus, DataFarm allows users to produce a large heterogeneous set of realistic jobs with their labels, which can be used by any ML-based data management component.

```F. Ventura, Z. Kaoudi, J. Quiané-Ruiz, and V. Markl. Expand your Training Limits! Generating and Labeling Jobs for ML-based Data Management. In SIGMOD, 2021.```

## Requirements

- SBT >= 1.3 
- Scala == 2.11
- Flink == 1.10.0
- Python == 3.6

Install all the python requirements specified in `requirements.txt`.  
N.B. DtaFarm has been tested on Linux and MacOS.

## Quick-start

1. Update `CONFIG.py`
2. Update `TableMetaData.py` (if needed)
3. Run `RunGenerator.py` 
4. Run `RunLabelForecaster.py`: To submit the jobs it is necessary that a Flink cluster is running. Please, be sure that the cluster is running and accessible.

## Configuration
To configure DataFarm you have to edit the `CONFIG.py`.

Please, provide the following configurations to start generating jobs:

- Provide the absolute path to the DataFarm
    ```Python
    PROJECT_PATH = "/absolute/path/to/DataFarm/project"
    ```

- Provide the absolute path to the folder containing your input data
    ```Python
    GENERATED_JOB_INPUT_DATA_PATH = "/absolute/path/to/input/data" 
    ```
    This folder will contain your input data.
    
- Provide the absolute path to the flink compiled source.
    ```Python
    FLINK_HOME = "/absolute/path/to/flink"
    ```
    N.B. The current version of DataFarm has been tested on Flink 1.10.0 built with scala 2.11. You can download Flink from [here](https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz).
    
We provide a sample _Input Workload_ in the project folder `data/input_workload_exec_plan`. 
You can include here any execution plan extracted from Flink jobs.

We also provide a sample TPC-H input data (about 1GB).
You can download sample TPC-H input data from [here](https://www.kaggle.com/fven7u/tpch-1gb).

The provided `TableMetaData.py` already contains the information necessary to run DataFarm with TPC-H data with scale factors 1GB, 5GB, 10GB, 50GB.

### Generator Configuration
DataFarm can be configured to generate datasets with different characteristics:

- `N_JOBS` defines the number of diverse Abstract Plans that will be generated.
- `N_VERSIONS` defined the number of versions that will be generated for each Abstract Plan.
- `JOB_SEED` can be specified to make the generation process replicable. If -1 the generation process is random. Otherwise, if >-1 the system will use the specified seed.
- `DATA_MANAGER` specifies the database manager to be used. The current implementation already implements the TPC-H database manager. You can use it specifying "TPCH".
- `DATA_ID` specifies the id of the input data meta-data that has to be used by the system. The input data meta-data can be specified in `TableMetaData.py`.
- `EXPERIMENT_ID` defines the id of the experiment. It will be the name of the folder where the results of the generation process will be stored.


### Label Forecaster Configuration
The Label Forecaster can be configured with:

- `MAX_EARLY_STOP` defines the max number of early stops that will be computed before interrupting the labeling process.
- `EARLY_STOP_TH` defines the threshold for early stop. It has to be included in the range (0, 1.0).
- `MAX_ITER` defines the maximum number of iterations that will be performed before interrupting the active learning iterations.
- `INIT_JOBS` defines the number of jobs to sample and run before starting the Active Learning process.
- `RANDOM_SAMPLING` defines if the instances will be picked with weighted random sampling based on uncertainty.
### Example configuration

```python
# PATH TO DATA-FARM
PROJECT_PATH = "~/data-farm/" # <absolute_path_to_project>

# PATH TO INPUT DATA
GENERATED_JOB_INPUT_DATA_PATH = "~/data/tpc-h/"  # <absolute_path_to_input_data>

# FLINK HOME
FLINK_HOME = "~/flink-1.10.0" # <absolute_path_to_flink>

# EXPERIMENT ID
EXPERIMENT_ID = "Experiment1"
# DATA ID
DATA_ID = "1GB"

# Job generator conf
N_JOBS = 10
N_VERSIONS = 3
JOB_SEED = -1  # -1 | 0
DATA_MANAGER = "TPCH" # "TPCH" | "IMDB"

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
EARLY_STOP_TH = 0.1 # Float between 0 and 1
MAX_ITER = 5
INIT_JOBS = 10
RANDOM_INIT = False
RANDOM_SAMPLING = False

GENERATED_METADATA_PATH = join(GENERATED_JOB_FOLDER, "generated_jobs_info.json")

LABEL_FORECASTER_OUT = join(EXPERIMENT_PATH, "label_forecaster_" + exec_timestamp)

```