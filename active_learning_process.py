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


def run(config, load_original_cards=False, random_sampling=False):
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

    if random_sampling:
        print("Random init sampling...")
        sample_model = RandomSampler(51, config.feature_cols, config.label_col, seed=42)
    else:
        sample_model = UniformAgglomerativeSampler(50, config.feature_cols, config.label_col, config.sample_col)

    sample_model.fit(dev_df, verbose=True)
    # save init_job_sample_ids
    np.savetxt(os.path.join(config.dest_folder, "init_job_sample_ids.txt"), sample_model.sample_ids, fmt="%d")

    train_data_df = sample_model.transform(dev_df.copy())
    val_data_df = dev_df.loc[~dev_df.index.isin(train_data_df.index), :]
    test_data_df = test_df.copy()

    X_train, y_train = get_X_y(train_data_df, config.feature_cols, config.label_col)
    ids_train = train_data_df.reset_index()[["plan_id", "data_id"]]
    print("Train data:", X_train.shape)

    X_test, y_test = get_X_y(val_data_df, config.feature_cols, config.label_col)
    ids_test = val_data_df.reset_index()[["plan_id", "data_id"]]
    print("Test data:", X_test.shape)

    results = test_active_learning(X_train.copy(), y_train.copy(), ids_train.copy(),
                                   X_test.copy(), y_test.copy(), ids_test.copy(),
                                   config.feature_cols,
                                   n_iter=config.n_iter,
                                   verbose=True)

    with open(os.path.join(config.dest_folder, "learning_process.pkl"), "wb") as handle:
        pickle.dump(results, handle)


def active_learning_iteration(X_train, y_train, ids_train, X_test, y_test, ids_test, feature_cols, verbose=False):
    
    if y_train.__len__() != ids_train.__len__():
        raise Exception("y_train does not match ids_train")

    if y_test.__len__() != ids_test.__len__():
        raise Exception("y_test does not match ids_test")
    
    results = {}

    qf_model = QuantileForestModel(random_state=42)
    qf_model.fit(X_train, y_train)
    qf_model.cross_validate(X_train, y_train)
    qf_model.validate(X_test, y_test)

    y_pred = qf_model.predict(X_test)
    y_pred_upper = qf_model.predict(X_test, quantile=75)
    y_pred_lower = qf_model.predict(X_test, quantile=25)

    if verbose:
        p = y_test.argsort()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(y_test[p], marker=".", linewidth=1, label="y_true", color="#1f77b4")
        ax.errorbar(np.arange(len(y_pred)), y_pred[p],
                    yerr=np.array([y_pred[p] - y_pred_lower[p], y_pred_upper[p] - y_pred[p]]), linewidth=0.5, fmt='.',
                    color="#ff7f0e", label="Pred. interval")
        ax.set_title(f"{type(qf_model).__name__} - Score[r2]: {qf_model.test_scores['r2']:.2f}")
        ax.set_ylabel("Log(Runtime)")
        ax.set_xlabel("Test jobs")
        ax.legend()
        # plt.show()
        plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(np.exp(y_test[p]), marker=".", linewidth=1, label="y_true", color="#1f77b4")
        ax.errorbar(np.arange(len(y_pred)), np.exp(y_pred[p]), yerr=np.array(
            [np.exp(y_pred[p]) - np.exp(y_pred_lower[p]), np.exp(y_pred_upper[p]) - np.exp(y_pred[p])]), linewidth=0.5,
                    fmt='.', color="#ff7f0e", label="Pred. interval")
        ax.set_title(f"EXP - {type(qf_model).__name__} - Score[r2]: {qf_model.test_scores_exp['r2']:.2f}")
        ax.set_ylabel("Runtime [ms]")
        ax.set_xlabel("Test jobs")
        ax.legend()
        # plt.show()
        plt.close()

        display(pd.DataFrame({"Feature": feature_cols, "F. Importance": qf_model.model.feature_importances_}) \
                .sort_values("F. Importance", ascending=False).head(15).style.background_gradient())

    IQR_interval = qf_model.predict_model_uncertainty(X_test, verbose=True)

    results["model"] = qf_model
    results["train_ids"] = ids_train.to_dict(orient="row")
    results["test_ids"] = ids_test.to_dict(orient="row")
    results["train_labels"] = y_train
    results["test_labels"] = y_test
    results["pred_labels"] = y_pred
    results["uncertainty_high"] = y_pred_upper
    results["uncertainty_low"] = y_pred_lower
    results["uncertainty_interval"] = IQR_interval
    results["feature_importance"] = {"Feature": feature_cols, "F_Importance": qf_model.model.feature_importances_}

    return results


def test_active_learning(X_train, y_train, ids_train, X_test, y_test, ids_test, feature_cols, n_iter=20, verbose=False, random_sampling=False):
    warnings.filterwarnings("ignore")

    data_size = []

    test_scores = []
    cross_validation_scores = []

    test_scores_exp = []
    cross_validation_scores_exp = []

    IQRs_mean = []
    iterations_results = []

    for idx in range(n_iter):
        print("======= Iteration", idx)
        data_size.append(X_train.shape[0])
        print("Train:", X_train.shape)
        print("Test:", X_test.shape)

        iter_res = active_learning_iteration(X_train, y_train, ids_train, X_test, y_test, ids_test, feature_cols, verbose=verbose)

        if random_sampling:
            IRQ_th = np.quantile(iter_res["uncertainty_interval"], 0.95)
            len_new_X_train = len(X_test[iter_res["uncertainty_interval"] > IRQ_th])
            sampling_idx = np.random.randint(0, len(X_test), len_new_X_train)

            new_X_train = X_test[sampling_idx]
            new_y_train = y_test[sampling_idx]
            new_ids_train = ids_test.iloc[sampling_idx].copy()
        else: # Sampling based on uncertainty threshold
            IRQ_th = np.quantile(iter_res["uncertainty_interval"], 0.95)
            new_X_train = X_test[iter_res["uncertainty_interval"] > IRQ_th]
            new_y_train = y_test[iter_res["uncertainty_interval"] > IRQ_th]
            new_ids_train = ids_test.iloc[iter_res["uncertainty_interval"] > IRQ_th].copy()

        # update test
        X_test = np.delete(X_test, np.where(iter_res["uncertainty_interval"] > IRQ_th), axis=0)
        y_test = np.delete(y_test, np.where(iter_res["uncertainty_interval"] > IRQ_th), axis=0)
        ids_test = ids_test[~ids_test.index.isin(new_ids_train.index)].copy()

        # update train
        X_train = np.concatenate([X_train, new_X_train])
        y_train = np.concatenate([y_train, new_y_train])
        ids_train = pd.concat([ids_train, new_ids_train])

        # store info
        test_scores.append(iter_res["model"].test_scores)
        cross_validation_scores.append(iter_res["model"].cross_validation_scores)

        test_scores_exp.append(iter_res["model"].test_scores_exp)
        cross_validation_scores_exp.append(iter_res["model"].cross_validation_scores_exp)

        IQRs_mean.append(np.mean(np.abs(iter_res["uncertainty_interval"])))

        iter_res["model"] = str(iter_res["model"])
        iterations_results.append(iter_res)
        print("=====================================================")

    results = {
        "iterations": list(range(n_iter)),
        "data_size": data_size,
        "model_uncertainty": IQRs_mean,
        "test_scores": test_scores,
        "test_scores_exp": test_scores_exp,
        "cross_validation_scores": cross_validation_scores,
        "cross_validation_scores_exp": cross_validation_scores_exp,
        "iterations_results": iterations_results

    }
    return results


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


def get_cofing(exp_type):
    if exp_type == "IMDB":
        return IMDB_config
    if exp_type == "TPCH":
        return TPCH_config
        # return TPCH_config_2
    else:
        Exception(f"No experiment type '{exp_type}'")

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
    try:
        os.mkdir(config.dest_folder)
    except Exception as ex:
        print(f"Experiment '{config.dest_folder}' already exists!")
        sys.exit(1)
    #
    run(config, load_original_cards=load_original_cards, random_sampling=random_sampling)
    # compute_data_plan_features(config)

if __name__ == '__main__':
    main()
