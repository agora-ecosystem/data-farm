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
2. Update `TableMetaData.py (if needed)
3. Run `RunGenerator.py`
4. Run `RunLabelForecaster.py``

## Configuration
TODO