import os

import pandas as pd

from sklearn.ensemble import AdaBoostRegressor, RandomForestRegressor, GradientBoostingRegressor, \
    RandomForestClassifier, GradientBoostingClassifier
from sklearn.tree import DecisionTreeRegressor
from sklearn.linear_model import LinearRegression

from generator_labeler.Analysis.models import test_model


class Analysis_2:

    features_1 = [
            'outCardinality_mean', 'outCardinality_std',
            'outCardinality_min', 'outCardinality_10%', 'outCardinality_50%',
            'outCardinality_90%', 'outCardinality_max', 'outCardinality_kurtosis',
            'outCardinality_skew', 'sourceCardinalitySum',
            'complexity_mean', 'complexity_max',
            'data_size', 'Data Source',
            'Join', 'Map', 'n_nodes', 'n_edges', 'longest_path_len', 'avg_parallelism',
            # 'Network', 'Disk I/O', 'CPU', 'Cumulative Network', 'Cumulative Disk I/O', 'Cumulative CPU',
            'Reduce', 'Filter', 'Group by', 'Partition', 'Sort-Partition']

    features_2 = ['data_size', 'Data Source', 'Join', 'Map', 'n_nodes', 'n_edges', 'longest_path_len',
                    'avg_parallelism', 'Network', 'Disk I/O', 'CPU', 'Cumulative Network', 'Cumulative Disk I/O', 'Cumulative CPU',
                    'Reduce', 'Filter', 'Group by', 'Partition', 'Sort-Partition']

    TRAIN_SIZES = [0.1, 0.2, 0.5, 0.8]

    FEATURE_SETS = {
        "Features_1": features_1,
        #"Features_2": features_2
    }

    CLF_SETS = {
        "RandomForestClassifier": {
            "labels": ["Bin3_netRunTime", "Bin5_netRunTime"],
            "clf":RandomForestClassifier(),
            "params":{
                    "n_estimators": [10, 50, 100, 200, 500],
                    "max_depth": [10, 50, 100, 200, 500]
            },
            "score": "f1_macro"
        },
        # "GradientBoostingClassifier": {
        #     "labels": ["Bin3_netRunTime", "Bin5_netRunTime"],
        #     "clf":GradientBoostingClassifier(),
        #     "params":{
        #             "n_estimators": [10, 50, 100, 200, 500],
        #             "max_depth": [10, 50, 100, 200, 500]
        #     },
        #     "score": "f1_macro"
        # },
        "RandomForestRegressor": {
            "labels": ["netRunTime", "Log_netRunTime"],
            "clf":RandomForestRegressor(),
            "params":{
                "n_estimators": [10, 50, 100, 200, 500],
                "max_depth": [10, 50, 100, 200, 500]
            },
            "score": "r2"
        },
        # "GradientBoostingRegressor": {
        #     "labels": ["netRunTime", "Log_netRunTime"],
        #     "clf":GradientBoostingRegressor(),
        #     "params":{
        #         "n_estimators": [10, 50, 100, 200, 500],
        #         "max_depth": [10, 50, 100, 200, 500]
        #     },
        #     "score": "r2"
        # },

        # "AdaBoostRegressor": {
        #     "labels": ["netRunTime", "Log_netRunTime"],
        #     "clf":AdaBoostRegressor(),
        #     "params":{
        #         "base_estimator": [LinearRegression(), DecisionTreeRegressor(max_depth=3), DecisionTreeRegressor(max_depth=10), DecisionTreeRegressor(max_depth=20)],
        #         "n_estimators": [100, 200, 500, 1000, 1500],
        #         "learning_rate": [0.01, 0.05, 0.1, 0.5, 1.0]
        #     },
        #     "score": "r2"
        # }
    }

    @classmethod
    def run_analysis_2(cls, plan_data_features, save_path = None):
        res = []

        for f_set_name, f_cols in cls.FEATURE_SETS.items():
            for clf_key, clf_values in cls.CLF_SETS.items():
                for label_col in cls.CLF_SETS[clf_key]["labels"]:
                    for train_s in cls.TRAIN_SIZES:
                        if "Regr" in clf_key:
                            is_regression = True
                        else:
                            is_regression = False

                        test_s = 1 - train_s

                        print(
                            f"Running> clf:{clf_key} - label_col:{label_col} - f_set_name:{f_set_name} - train_s:{train_s}")
                        clf, score = test_model(plan_data_features,
                                                f_cols,
                                                label_col,
                                                cls.CLF_SETS[clf_key]["clf"],
                                                cls.CLF_SETS[clf_key]["params"],
                                                cls.CLF_SETS[clf_key]["score"],
                                                test_size=test_s,
                                                is_regression=is_regression,
                                                remove_outliers=False,
                                                outlier_col="netRunTime"
                                                )

                        res.append({
                            "model": clf_key,
                            "is_regression": is_regression,
                            "label_col": label_col,
                            "remove_outliers": False,
                            "feature_set_name": f_set_name,
                            "feature_cols": f_cols,
                            "train_size": train_s,
                            "test_size": test_s,
                            "score_function": cls.CLF_SETS[clf_key]["score"],
                            "score": score
                        })

        res_df = pd.DataFrame(res)

        if save_path is not None:
            res_df.to_json(os.path.join(save_path, "analysis_2_out.json"))


        return res_df