import os
import sys
import warnings
import pickle
# from IPython.core.display import display
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from generator_labeler.paper_results import IMDB_config, TPCH_config
from generator_labeler.FeatureExtraction.PredictorFeatureExtraction import compute_cardinality_plan_features
from generator_labeler.JobExecutionSampler.unsupervised_sampler import UniformAgglomerativeSampler, RandomSampler
from generator_labeler.JobExecutionSampler.supervised_sampler import UserSampler
from generator_labeler.ActiveModel.ActiveQuantileForest import QuantileForestModel
import BuildAndSubmit
from generator_labeler.CustomActiveLearning import ActiveLearningStrategy
from CONFIG import CONFIG


def get_X_y(df, feature_cols, label_col):
    return df.loc[:, feature_cols].values, df.loc[:, label_col].values


def get_executed_plans_exec_time(jobs_to_run):
    executed_plans = BuildAndSubmit.get_executed_plans_multiple_data_ids()

    # Fixed data id bug here
    print(executed_plans.keys())
    # executed_plans_times = [(ep_k[0], ep_k[1], ep_v["netRunTime"]) for ep_k, ep_v in executed_plans.items() if
    #                         ep_k in jobs_to_run]
    executed_plans_times = [(ep_k[0], ep_k[1], ep_v["netRunTime"]) for ep_k, ep_v in executed_plans.items() if
                            ep_k[0] in jobs_to_run]
    if len(executed_plans_times) != len(jobs_to_run):
        print(
            f"WARNING - The number of executed jobs '{len(executed_plans_times)}' does not match the requested jobs '{len(jobs_to_run)}'.")
    return pd.DataFrame(executed_plans_times, columns=["plan_id", "data_id", "netRunTime"]).set_index(
        ["plan_id", "data_id"])


def submit_jobs(init_jobs_to_run):
    print("SUBMIT JOBS FUNCTION RUNNING")
    exec_plans_path_already_computed = BuildAndSubmit.get_exec_plans_path()
    exec_plans_already_computed = {os.path.basename(ep).replace("$.json", "") for ep in
                                   exec_plans_path_already_computed}

    job_projects = BuildAndSubmit.get_job_projects()
    job_projects = sorted(job_projects, key=BuildAndSubmit.job_id_v)
    job_projects = [jp for jp in job_projects if (jp in init_jobs_to_run) and (jp not in exec_plans_already_computed)]

    print(f"Submitting #{job_projects.__len__()} jobs:", job_projects)
    BuildAndSubmit.run_jobs(job_projects)
    return


def active_learning_iteration(X_train, y_train, ids_train, X_test, ids_test, feature_cols, verbose=False):
    if X_train.__len__() != ids_train.__len__():
        raise Exception("x_train does not match ids_train")

    if X_test.__len__() != ids_test.__len__():
        raise Exception("x_test does not match ids_test")

    results = {}
    qf_model = QuantileForestModel(random_state=42)
    qf_model.fit(X_train, y_train)
    qf_model.cross_validate(X_train, y_train)

    y_pred = qf_model.predict(X_test)
    y_pred_upper = qf_model.predict(X_test, quantile=75)
    y_pred_lower = qf_model.predict(X_test, quantile=25)

    if verbose:
        p = y_pred.argsort()
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(y_pred[p], marker=".", linewidth=1, label="y_true", color="#1f77b4")
        ax.errorbar(np.arange(len(y_pred)), y_pred[p],
                    yerr=np.array([y_pred[p] - y_pred_lower[p], y_pred_upper[p] - y_pred[p]]), linewidth=0.5, fmt='.',
                    color="#ff7f0e", label="Pred. interval")
        # ax.set_title(f"{type(qf_model).__name__} - Score[r2]: {qf_model.test_scores['r2']:.2f}")
        ax.set_ylabel("Log(Runtime)")
        ax.set_xlabel("Test jobs")
        ax.legend()
        #  plt.show()
        plt.close()

        fig, ax = plt.subplots(figsize=(10, 6))
        # ax.plot(np.exp(y_pred[p]), marker=".", linewidth=1, label="y_true", color="#1f77b4")
        ax.errorbar(np.arange(len(y_pred)), np.exp(y_pred[p]), yerr=np.array(
            [np.exp(y_pred[p]) - np.exp(y_pred_lower[p]), np.exp(y_pred_upper[p]) - np.exp(y_pred[p])]), linewidth=0.5,
                    fmt='.', color="#ff7f0e", label="Pred. interval")
        # ax.set_title(f"EXP - {type(qf_model).__name__} - Score[r2]: {qf_model.test_scores_exp['r2']:.2f}")
        ax.set_ylabel("Runtime [ms]")
        ax.set_xlabel("Test jobs")
        ax.legend()
        #  plt.show()
        plt.close()

        # display(pd.DataFrame({"Feature": feature_cols, "F. Importance": qf_model.model.feature_importances_}) \
        #        .sort_values("F. Importance", ascending=False).head(15).style.background_gradient())

    IQR_interval = qf_model.predict_model_uncertainty(X_test, verbose=True)

    results["model"] = qf_model
    results["train_ids"] = ids_train.to_dict(orient="row")
    results["test_ids"] = ids_test.to_dict(orient="row")
    results["train_labels"] = y_train
    # results["test_labels"] = y_test
    results["pred_labels"] = y_pred
    results["uncertainty_high"] = y_pred_upper
    results["uncertainty_low"] = y_pred_lower
    results["uncertainty_interval"] = IQR_interval
    results["feature_importance"] = {"Feature": feature_cols, "F_Importance": qf_model.model.feature_importances_}

    return results


def get_dataset(features_df, feature_cols, label_col):
    train_data_df = features_df.loc[~features_df[label_col].isna(), :]
    # train_data_df = features_df.loc[features_df.index.get_level_values(0).isin(executed_jobs), :]
    val_data_df = features_df.loc[~features_df.index.isin(train_data_df.index), :]
    # test_data_df = test_df.copy()

    X_train, y_train = get_X_y(train_data_df, feature_cols, label_col)
    ids_train = train_data_df.reset_index()[["plan_id", "data_id"]]
    print("Train data:", X_train.shape)

    X_test, y_test = get_X_y(val_data_df, feature_cols, label_col)
    ids_test = val_data_df.reset_index()[["plan_id", "data_id"]]
    print("Test data:", X_test.shape)
    return X_train, y_train, ids_train, X_test, y_test, ids_test


def check_early_stop(iterations_results, th=0.1):
    IQRs_RMSE = np.array(
        [np.mean(np.exp(I["uncertainty_high"]) - np.exp(I["uncertainty_low"])) for I in iterations_results])
    # IQRs_std = np.array([np.std(np.exp(I["uncertainty_high"]) - np.exp(I["uncertainty_low"])) for I in iterations_results])
    print(">>> Model's uncertanties: ", IQRs_RMSE)
    if len(IQRs_RMSE) < 2:
        return False

    min_u = IQRs_RMSE[-2]
    min_local_u = IQRs_RMSE[-2]
    r = IQRs_RMSE[-1] / min_local_u

    if (r > 1) or (IQRs_RMSE[-1] > min_u):
        return False

    if (1 - r) < th:
        return False

    return True


def run_active_learning(features_df, feature_cols, label_col, n_iter=20, max_early_stop=2, early_stop_th=0.1,
                        verbose=False, random_sampling=False):
    warnings.filterwarnings("ignore")

    data_size = []
    test_scores = []
    cross_validation_scores = []
    test_scores_exp = []
    cross_validation_scores_exp = []
    IQRs_mean = []
    iterations_results = []
    early_stop_count = 0

    # Start Active-Learning
    X_train, y_train, ids_train, X_test, _, ids_test = get_dataset(features_df, feature_cols, label_col)

    # -> create model
    # -> predict labels
    # -> next iteration

    for idx in range(n_iter):
        print("======= Iteration", idx)
        data_size.append(X_train.shape[0])
        print("Train:", X_train.shape)
        print("Test:", X_test.shape)

        iter_res = active_learning_iteration(X_train, y_train, ids_train, X_test, ids_test, feature_cols,
                                             verbose=verbose)

        # store info
        cross_validation_scores.append(iter_res["model"].cross_validation_scores)
        test_scores_exp.append(iter_res["model"].test_scores_exp)
        cross_validation_scores_exp.append(iter_res["model"].cross_validation_scores_exp)
        IQRs_mean.append(np.mean(np.abs(iter_res["uncertainty_interval"])))
        iter_res["model"] = str(iter_res["model"])
        iterations_results.append(iter_res)

        if (idx + 1 >= n_iter):
            print("Max iteration reached!")
            break

        if check_early_stop(iterations_results, early_stop_th):
            early_stop_count += 1
            if early_stop_count >= max_early_stop:
                print("Early stop reached!")
                break
            else:
                print(f">>> Skip early stop {early_stop_count}. Max early stop is set to {max_early_stop}.")

        # Prepare next iteration
        if random_sampling:
            IRQ_th = np.quantile(iter_res["uncertainty_interval"], 0.95)
            len_new_X_train = len(X_test[iter_res["uncertainty_interval"] > IRQ_th])
            sampling_idx = np.random.randint(0, len(X_test), len_new_X_train)
            new_ids_train = ids_test.iloc[sampling_idx].copy()

        else:  # Sampling based on uncertainty threshold
            IRQ_th = np.quantile(iter_res["uncertainty_interval"], 0.95)
            new_ids_train = ids_test.iloc[iter_res["uncertainty_interval"] > IRQ_th].copy()

        if len(new_ids_train) == 0:
            print("No more jobs to run, Early Stop!")
            break

        print("Candidates to run:\n", new_ids_train)
        # -> RUN Jobs
        new_jobs_to_run = new_ids_train.iloc[:, 0].values
        submit_jobs(new_jobs_to_run)

        # -> Collect exec time
        executed_jobs_runtime = get_executed_plans_exec_time(new_jobs_to_run)
        for k, v in executed_jobs_runtime.iterrows():
            features_df.loc[k, "netRunTime"] = v.values[0]
        features_df[label_col] = np.log(features_df["netRunTime"])

        X_train, y_train, ids_train, X_test, _, ids_test = get_dataset(features_df, feature_cols, label_col)

        print("=====================================================")

    pred_jobs = pd.DataFrame(iterations_results[-1]["test_ids"])
    pred_jobs[f"pred_{label_col}"] = iterations_results[-1]["pred_labels"]
    pred_jobs[f"unc_low_{label_col}"] = iterations_results[-1]["uncertainty_low"]
    pred_jobs[f"unc_up_{label_col}"] = iterations_results[-1]["uncertainty_high"]
    pred_jobs = pred_jobs.set_index(["plan_id", "data_id"])
    final_dataset = pd.merge(features_df, pred_jobs, left_index=True, right_index=True, how="left")

    results = {
        "iterations": list(range(n_iter)),
        "data_size": data_size,
        "model_uncertainty": IQRs_mean,
        "test_scores": test_scores,
        "test_scores_exp": test_scores_exp,
        "cross_validation_scores": cross_validation_scores,
        "cross_validation_scores_exp": cross_validation_scores_exp,
        "iterations_results": iterations_results,
        "final_dataset": final_dataset

    }
    return results


def load_data_and_preprocess(GENERATED_METADATA_PATH, DATA_ID="1GB", DATA_IDS=[]):
    # Load dataset

    # Check for either one or a list of data ids
    if(len(DATA_IDS)==0):
        print("dataids 0")
        plan_data_features = compute_cardinality_plan_features(GENERATED_METADATA_PATH, data_sizes=[DATA_ID])
    else:
        plan_data_features = compute_cardinality_plan_features(GENERATED_METADATA_PATH,data_sizes=DATA_IDS)
    print(plan_data_features)
    plan_data_features = plan_data_features.sort_index()

    sourceCardinalitySum = plan_data_features["sourceCardinalitySum"].copy()
    sourceCardinalitySum[sourceCardinalitySum == 0] = 1  # Solves a bug in uniform sampler, because log of 0 is minus inf
    plan_data_features["Log_sourceCardinalitySum"] = np.log(sourceCardinalitySum)

    return plan_data_features


def run(config):
    # Load plan_data_features
    features_df = load_data_and_preprocess(config.GENERATED_METADATA_PATH, config.DATA_ID)

    # Persist features
    # features_df.to_csv(os.path.join(config.LABEL_FORECASTER_OUT, "plan_data_features.csv"))

    # if config.RANDOM_INIT:
    #     print("Random init sampling...")
    #     sample_model = RandomSampler(config.INIT_JOBS, config.FEATURE_COLS, config.LABEL_COL, seed=42)
    # elif config.USER_INIT:
    #     sample_model = UserSampler(config.INIT_JOBS, config.FEATURE_COLS, config.LABEL_COL, seed=42)
    # else:
    #     sample_model = UniformAgglomerativeSampler(config.INIT_JOBS, config.FEATURE_COLS, config.LABEL_COL,
    #                                                config.SAMPLE_COL)
    #
    # sample_model.fit(features_df, verbose=True)
    # # save init_job_sample_ids
    # np.savetxt(os.path.join(config.LABEL_FORECASTER_OUT, "init_job_sample_ids.txt"), sample_model.sample_ids, fmt="%d")
    #
    # init_jobs_to_run = features_df.iloc[sample_model.sample_ids].index.get_level_values(0)
    # features_df.iloc[sample_model.sample_ids].to_csv(os.path.join(CONFIG.LABEL_FORECASTER_OUT, f"job_sample_ids_iteration_0.csv"), index=True)
    #
    # # -> RUN Jobs
    # submit_jobs(init_jobs_to_run)

    init_jobs_to_run = features_df.index.get_level_values(0)
    # -> Collect exec time
    executed_jobs_runtime = get_executed_plans_exec_time(init_jobs_to_run)
    print(executed_jobs_runtime)
    features_df = pd.merge(features_df, executed_jobs_runtime, left_index=True, right_index=True, how="left")
    features_df.to_csv(os.path.join(config.LABEL_FORECASTER_OUT, "plan_data_features.csv"))
    features_df[config.LABEL_COL] = np.log(features_df["netRunTime"])

    features_df.to_csv(os.path.join(config.LABEL_FORECASTER_OUT, "plan_data_features.csv"))
    # Initialize object
    custom_active_learning = ActiveLearningStrategy.ActiveLearningStrategy(features_df=features_df,
                                                                           feature_cols=config.FEATURE_COLS,
                                                                           label_col=config.LABEL_COL,
                                                                           label_forecaster_out=config.LABEL_FORECASTER_OUT,
                                                                           verbose=True)
    # Run model training
    results = custom_active_learning.run_active_learning(
        n_iter=1,
        max_early_stop=config.MAX_EARLY_STOP,
        early_stop_th=config.EARLY_STOP_TH,
        user_prompt=config.USER_PROMPT)

    # Save results
    results["final_dataset"].to_csv(os.path.join(config.LABEL_FORECASTER_OUT, "final_dataset.csv"))

    # Save model
    with open(os.path.join(config.LABEL_FORECASTER_OUT, "learning_process.pkl"), "wb") as handle:
        pickle.dump(results, handle)


# def get_cofing(exp_type):
#     if exp_type == "IMDB":
#         return IMDB_config
#     if exp_type == "TPCH":
#         return TPCH_config
#     else:
#         Exception(f"No experiment type '{exp_type}'")

def set_up_active_learning(generated_metadata_path, label_forecaster_out, random_init, user_init, init_jobs,
                           feature_cols, label_col, sample_col, sample_ids, features_df):

    # Persist features
    features_df.to_csv(os.path.join(label_forecaster_out, "plan_data_features.csv"))

    # # Integrate into helper
    # if random_init:
    #     print("Random init sampling...")
    #     sample_model = RandomSampler(init_jobs, feature_cols, label_col, seed=42)
    # elif user_init:
    #     sample_model = UserSampler(init_jobs, feature_cols, label_col, seed=42)
    # else:
    #     sample_model = UniformAgglomerativeSampler(init_jobs, feature_cols, label_col,
    #                                                sample_col)
    #
    # sample_model.fit(features_df, verbose=True)

    # save init_job_sample_ids
    np.savetxt(os.path.join(label_forecaster_out, "init_job_sample_ids.txt"), sample_ids, fmt="%d")

    init_jobs_to_run = features_df.iloc[sample_ids].index

    print("INIT JOBS TO RUN")
    print(features_df.iloc[sample_ids])
    # init_jobs_to_run = features_df.iloc[list(range(0, len(features_df)))].index.get_level_values(0)

    # -> RUN Jobs
    # submit_jobs(init_jobs_to_run)

    # -> Collect exec time
    executed_jobs_runtime = get_executed_plans_exec_time(init_jobs_to_run)
    print("Executed jobs runtime")
    print(executed_jobs_runtime)
    features_df = pd.merge(features_df, executed_jobs_runtime, left_index=True, right_index=True, how="left")
    print("Features df merge")
    features_df[label_col] = np.log(features_df["netRunTime"])
    print(features_df.loc[~features_df[label_col].isna(), :])

    # features_df.rename(columns={'Execution Time': 'Log_netRunTime'}, inplace=True)
    features_df.to_csv(os.path.join(label_forecaster_out, "plan_data_features.csv"))

    custom_active_learning = ActiveLearningStrategy.ActiveLearningStrategy(features_df=features_df,
                                                                           feature_cols=feature_cols,
                                                                           label_col=label_col, label_forecaster_out=label_forecaster_out, verbose=True)

    return custom_active_learning

    # results["final_dataset"].to_csv(os.path.join(label_forecaster_out, "final_dataset.csv"))
    # with open(os.path.join(label_forecaster_out, "learning_process.pkl"), "wb") as handle:
    #     pickle.dump(results, handle)


def main():
    # exp_type = "TPCH"  # "TPCH" # "IMDB"
    config = CONFIG
    random_sampling = False

    if config.RANDOM_INIT:
        print("################################")
        print("## INFO! Random job sampling! ##")
        print("################################")
        config.LABEL_FORECASTER_OUT = config.LABEL_FORECASTER_OUT + "_random_sample"

    ### Active Learning Process
    try:
        os.mkdir(config.LABEL_FORECASTER_OUT)
    except Exception as ex:
        print(f"Experiment '{config.LABEL_FORECASTER_OUT}' already exists!")
        sys.exit(1)
    #
    run(config)


if __name__ == '__main__':
    main()
