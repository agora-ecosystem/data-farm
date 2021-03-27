import os
import sys
import warnings
import pickle
from IPython.core.display import display
import numpy as np
import pandas as pd

from generator_labeler.ActiveModel import RandomForestQuantileRegressor
from generator_labeler.FeatureExtraction import PredictorFeatureExtraction as PFE
from generator_labeler.Analysis.models import get_X_y
from generator_labeler.Analysis.GeneratedJobsAnalyzer import load_generated_dataset, load_validation_dataset
from generator_labeler.FeatureExtraction.PredictorFeatureExtraction import preprocess_jobs_data_info, fill_jobs_cardinality
from generator_labeler.JobExecutionSampler.unsupervised_sampler import UniformAgglomerativeSampler, RandomSampler
from generator_labeler.ActiveModel.ActiveQuantileForest import QuantileForestModel
from generator_labeler.paper_results import IMDB_config, TPCH_config, TPCH_config_2

import matplotlib.pyplot as plt
import seaborn as sns

# TDGen dependencies
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score

sns.set_context("talk")
sns.set_style("whitegrid")

np.random.seed(42)
# np.random.seed(51)


def compute_data_plan_features(config):
    args_params = config.parse_args(sys.argv)
    jobs_data_info = preprocess_jobs_data_info(args_params["generatedJobsInfo"])
    data_sizes = config.data_sizes
    data_plan_features = []
    for d_id in data_sizes:
        data_f = fill_jobs_cardinality(jobs_data_info, data_size=d_id)
        data_plan_features.append(data_f)

    data_plan_features = pd.concat(data_plan_features)
    with open(os.path.join(config.dest_folder, "data_plan_features.pkl"), "wb") as handle:
        pickle.dump(data_plan_features, handle)


def load_data_and_preprocess(config, load_original_cards=False):
    args_params = config.parse_args(sys.argv)

    # Load dataset
    if config.plan_data_features_path is not None:
        print("#####################################################")
        print("## WARNING! Loading pre-existing features dataset! ##")
        print("#####################################################")
        plan_data_features = pd.read_csv(config.plan_data_features_path).set_index(["plan_id", "data_id"])
    else:

        plan_data_features, exec_plans_graph = load_generated_dataset(args_params, data_sizes=config.data_sizes, load_original_cards=load_original_cards)

        # Log of labels and features
        plan_data_features["Log_netRunTime"] = np.log(plan_data_features["netRunTime"])

        sourceCardinalitySum = plan_data_features["sourceCardinalitySum"].copy()
        sourceCardinalitySum[sourceCardinalitySum == 0] = 1 # Solves a bug in uniform sampler, because log of 0 is minus inf
        plan_data_features["Log_sourceCardinalitySum"] = np.log(sourceCardinalitySum)

    return plan_data_features


def TDGen(config, load_original_cards=False, verbose=False):
    config.dest_folder = config.dest_folder + "_td_gen"
    try:
        os.mkdir(config.dest_folder)
    except Exception as ex:
        print(f"Experiment '{config.dest_folder}' already exists!")
        sys.exit(1)

    feature_cols = ["sourceOutCard_mean"]

    label_col = "netRunTime"

    # Load plan_data_features
    plan_data_features = load_data_and_preprocess(config, load_original_cards=load_original_cards)

    # Persist features
    plan_data_features.to_csv(os.path.join(config.dest_folder, "plan_data_features.csv"))

    # Remove outliers
    plan_data_features_no_out = PFE.remove_outliers(plan_data_features.copy(), "netRunTime", b=0.01)
    plan_data_features_no_out.to_csv(os.path.join(config.dest_folder, "plan_data_features_no_out.csv"))

    # Init learning process
    df = plan_data_features_no_out.copy()
    dev_df = df.copy()
    test_df = df[~df.index.isin(dev_df.index)].copy()

    dev_df = dev_df.copy().sample(frac=1, random_state=42)
    results = {}

    for sample in config.td_gen_samples:
        train_data_df = dev_df.iloc[:sample]
        val_data_df = dev_df.loc[~dev_df.index.isin(train_data_df.index), :].copy()
        # test_data_df = test_df.copy()


        X_train, y_train = get_X_y(train_data_df, feature_cols, label_col)
        ids_train = train_data_df.reset_index()[["plan_id", "data_id"]]
        print("Train data:", X_train.shape)

        X_test, y_test = get_X_y(val_data_df, feature_cols, label_col)
        print("Test data:", X_test.shape)

        degree = 5

        model = make_pipeline(PolynomialFeatures(degree), Ridge())
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        score = r2_score(y_test, y_pred)

        if verbose:
            sp = np.argsort(y_test)

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(X_test.reshape(-1)[sp], y_pred[sp], color="red", linewidth=0, marker=".", label="pred.")
            ax.plot(X_test.reshape(-1)[sp], y_test[sp], color="blue", linewidth=0, marker=".", label="true",
                    alpha=0.5)
            ax.set_yscale("log")
            ax.set_title(f"R2: {score}")
            ax.legend()
            # plt.show()
            plt.close()

            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(np.arange(y_pred[sp].__len__()), y_pred[sp], color="red", linewidth=0, marker=".",
                    label="pred.")
            ax.plot(np.arange(y_pred[sp].__len__()), y_test[sp], color="#1f77b4", linewidth=0, marker=".",
                    label="true", alpha=0.5)

            ax.set_title(f"TDGen - EXP - {type(model).__name__} - Score[r2]: {score:.2f}")
            ax.set_ylabel("Runtime [ms]")
            ax.set_xlabel("Test jobs")
            ax.set_yscale("log")
            ax.legend()
            # plt.show()
            plt.close()

        results[sample] = {"train_labels": y_train, "test_labels": y_test, "pred_labels": y_pred, "score": score,
                   "train_ids": ids_train.to_dict()}

    with open(os.path.join(config.dest_folder, "td_gen.pkl"), "wb") as handle:
        pickle.dump(results, handle)


def load_train_data(base_folder, experiment_id, iteration_to_show=2):

    data_file = f"{base_folder}/{experiment_id}/learning_process.pkl"
    features_file = f"{base_folder}/{experiment_id}/plan_data_features_no_out.csv"

    with open(data_file, "rb") as handle:
        learning_data = pickle.load(handle)

    features_df = pd.read_csv(features_file)

    labels = learning_data["iterations_results"][iteration_to_show]

    train_df = pd.DataFrame(labels["train_ids"])
    train_df["labels"] = labels["train_labels"]

    test_df = pd.DataFrame(labels["test_ids"])
    test_df["labels"] = labels["pred_labels"]
    labels_df = pd.concat([train_df, test_df], ignore_index=True)

    return pd.merge(features_df, labels_df)


def validate_with_original_workload(config):
    args_params = config.parse_args(sys.argv)
    features_1 = [
        'outCardinality_mean', 'outCardinality_std',
        'outCardinality_min', 'outCardinality_10%', 'outCardinality_50%',
        'outCardinality_90%', 'outCardinality_max', 'outCardinality_kurtosis',
        'outCardinality_skew', 'sourceCardinalitySum',
        'complexity_mean', 'complexity_max',
        'data_size', 'Data Source',
        'Join', 'Map', 'n_nodes', 'n_edges', 'longest_path_len', 'avg_parallelism',
        'Reduce', 'Filter', 'Group by', 'Partition', 'Sort-Partition']

    feature_cols = ["t_0", "t_1", "t_2", "t_3", "t_4", "t_5", "t_6",
                    "joinOutCard_sum", "joinOutCard_mean", "joinOutCard_std", "joinOutCard_min", "joinOutCard_max",
                    "filterOutCard_mean", "filterOutCard_std", "filterOutCard_min", "filterOutCard_max",
                    "groupbyOutCard_mean", "groupbyOutCard_std", "groupbyOutCard_min", "groupbyOutCard_max",
                    "outCardinality_mean", "outCardinality_std", "outCardinality_min", "outCardinality_max",
                    "outCardinality_kurtosis", "outCardinality_skew",
                    "sourceOutCard_mean", "sourceOutCard_std", "sourceOutCard_min", "sourceOutCard_max",
                    "sourceCardinalitySum",
                    "complexity_mean", "complexity_min", "complexity_max",
                    # "sourceCardinalityProd", # "joinOutCard_prod", # "sourceCardinalitySum_Joins"
                    ]

    validation_plan_data_features, validation_exec_plans = load_validation_dataset(args_params)

    print(validation_plan_data_features[features_1])

    base_folder = "./TPC-H-results"
    experiment_id = "20200829171516"

    train_data = load_train_data(base_folder, experiment_id, iteration_to_show=2)

    #print(train_data[features_1])
    train_X, train_y = get_X_y(train_data, features_1, "labels") # Estimated labels
    top_train_X, top_train_y = get_X_y(train_data, features_1, "Log_netRunTime")

    # df = validation_plan_data_features.copy() # Dataset + Table APIs
    df = validation_plan_data_features[~validation_plan_data_features.index.get_level_values(0).str.contains("-SQL")].copy() # Dataset APIs
    df["Log_netRunTime"] = np.log(df["netRunTime"])
    test_X = df[features_1].values
    test_y = df["Log_netRunTime"].values

    from sklearn.metrics import r2_score
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import GridSearchCV

    scoring_function = "r2"
    param_grid = {
        "n_estimators": [10, 50, 100, 200, 500],
        "max_depth": [10, 50, 100, 200, 500],
        # "min_samples_split": [2, 10],
        "random_state": [42]
    }
    n_folds = 3

    ###############################
    ## Build with forecasted labels
    clf = RandomForestRegressor()
    clf = GridSearchCV(clf, param_grid, cv=n_folds,
                 scoring=scoring_function, verbose=True, n_jobs=4, iid=False)

    clf.fit(train_X, train_y)
    pred_y = clf.predict(test_X)

    score_res = r2_score(np.exp(test_y), np.exp(pred_y))
    print(score_res)


    ###############################
    ## Build with real labels
    clf = RandomForestRegressor()
    clf = GridSearchCV(clf, param_grid, cv=n_folds,
                       scoring=scoring_function, verbose=True, n_jobs=4, iid=False)

    clf.fit(top_train_X, top_train_y)
    top_pred_y = clf.predict(test_X)

    score_res = r2_score(np.exp(test_y), np.exp(top_pred_y))
    print(score_res)

    df["pred_labels"] = pred_y
    df["top_pred_labels"] = top_pred_y

    df.to_csv(os.path.join(config.dest_folder, "workload_validation_reaults.csv"))


def get_cofing(exp_type):
    if exp_type == "IMDB":
        return IMDB_config
    if exp_type == "TPCH":
        return TPCH_config
        # return TPCH_config_2
    else:
        Exception(f"No experiment type '{exp_type}'")


def validate_the_input_data(config):
    feature_cols = [
                    "joinOutCard_sum", "joinOutCard_mean", "joinOutCard_std", "joinOutCard_min", "joinOutCard_max",
                    "filterOutCard_mean", "filterOutCard_std", "filterOutCard_min", "filterOutCard_max",
                    "groupbyOutCard_mean", "groupbyOutCard_std", "groupbyOutCard_min", "groupbyOutCard_max",
                    "outCardinality_mean", "outCardinality_std", "outCardinality_min", "outCardinality_max",
                    "outCardinality_kurtosis", "outCardinality_skew",
                    "sourceOutCard_mean", "sourceOutCard_std", "sourceOutCard_min", "sourceOutCard_max",
                    "sourceCardinalitySum",
                    "complexity_mean", "complexity_min", "complexity_max",
                    # "sourceCardinalityProd", # "joinOutCard_prod", # "sourceCardinalitySum_Joins"
                    ]


    # Load w1
    base_folder = "./TPC-H-results"
    experiment_id = "20200829171516"
    iteration_to_show = 2
    features_file = f"{base_folder}/{experiment_id}/plan_data_features_no_out.csv"
    w1_data = pd.read_csv(features_file)
    #w1_data = load_train_data(base_folder, experiment_id, iteration_to_show)

    # Load w2
    base_folder = "./IMDB-results"
    experiment_id = "20200829163549_good"  # Good
    features_file = f"{base_folder}/{experiment_id}/plan_data_features_no_out.csv"
    w2_data = pd.read_csv(features_file)

    train_X, train_y = get_X_y(w1_data, feature_cols, "Log_netRunTime")
    test_X, test_y = get_X_y(w2_data, feature_cols, "Log_netRunTime")

    ###############################
    ## Build with forecasted labels
    from sklearn.metrics import r2_score
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import GridSearchCV
    scoring_function = "r2"
    param_grid = {
        "n_estimators": [10, 50, 100, 200, 500],
        "max_depth": [10, 50, 100, 200, 500],
        # "min_samples_split": [2, 10],
        "random_state": [42]
    }
    n_folds = 3

    clf = RandomForestQuantileRegressor(random_state=42)
    clf = GridSearchCV(clf, param_grid, cv=n_folds,
                       scoring=scoring_function, verbose=True, n_jobs=4, iid=False)

    clf.fit(train_X, train_y)
    pred_y = clf.predict(test_X)

    score_res = r2_score(np.exp(test_y), np.exp(pred_y))
    print(score_res)

    w2_data["pred_labels"] = pred_y

    w2_data.to_csv(os.path.join(config.dest_folder, "input_data_validation_reaults.csv"))

def main():
    exp_type = "TPCH"  # "TPCH" # "IMDB"
    config = get_cofing(exp_type)
    load_original_cards = False  # False # True
    random_sampling = False

    if load_original_cards:
        print("################################################")
        print("## INFO! Loading original output cardinality! ##")
        print("################################################")
        config.dest_folder = config.dest_folder + "_original_cards"

    if random_sampling:
        print("################################")
        print("## INFO! Random job sampling! ##")
        print("################################")
        config.dest_folder = config.dest_folder + "_random_sample"


    ### Active Learning Process
    # try:
    #     os.mkdir(config.dest_folder)
    # except Exception as ex:
    #     print(f"Experiment '{config.dest_folder}' already exists!")
    #     sys.exit(1)
    ###

    ### Validation
    # validate_with_original_workload(config)

    # validate_the_input_data(config)

    ### TDGen
    TDGen(config, load_original_cards=False, verbose=True)

if __name__ == '__main__':
    main()
