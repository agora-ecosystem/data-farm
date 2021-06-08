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
    
- Provide the absolute path to the flink source.
    ```Python
    FLINK_HOME = "/absolute/path/to/flink"
    ```
    N.B. The current version of DataFarm has been tested on Flink 1.10.0 built with scala 2.11. You can download Flink from [here](https://archive.apache.org/dist/flink/flink-1.10.0/flink-1.10.0-bin-scala_2.11.tgz).
    
We provide a sample _Input Workload_ in the project folder `data/input_workload_exec_plan`. 
You can include here any execution plan extracted from Flink jobs.

We also provide a sample TPC-H input data (about 1GB).
You can download sample TPC-H input data from [here](https://www.kaggle.com/fven7u/tpch-1gb).

The provided `TableMetaData.py` already contains the information necessary to run DataFarm with TPC-H data with scale factors 1GB, 5GB, 10GB, 50GB.

TODO: other configuration parameters.