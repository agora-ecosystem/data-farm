# Sampler for the jobs to execute
import warnings

import numpy as np
import os
import pandas as pd
import pickle
from matplotlib import pyplot as plt
from simple_term_menu import TerminalMenu

from generator_labeler.ActiveModel.ActiveQuantileForest import QuantileForestModel
from generator_labeler.CustomActiveLearning.ActiveLearningUtility import ActiveLearningUtility, JobExecutionSampler, \
    UserSampling
from generator_labeler.JobExecutionSampler.supervised_sampler import UserSampler
from CONFIG import CONFIG

import shap


class IQRs_mean:
    pass


class ActiveLearningStrategy:
    separator = "====================================================="

    def __init__(self, features_df, feature_cols, label_col, label_forecaster_out, verbose=False, user_prompt=False):
        # Active Learning initialization variables
        self.features_df = features_df
        self.feature_cols = feature_cols
        self.label_col = label_col
        self.verbose = verbose
        self.label_forecaster_out = label_forecaster_out

        # Initialize process variables
        self.data_size = []
        self.cross_validation_scores = []
        self.test_scores = []
        self.test_scores_exp = []
        self.cross_validation_scores_exp = []
        self.IQRs_mean = []
        self.iterations_results = []
        self.early_stop_count = 0
        self.iter_res = []
        self.executed_jobs_runtime = []

        # Get dataset
        # print("FEATURES DF TESTING")
        # print(features_df)
        self.X_train, self.y_train, self.ids_train, self.X_test, _, self.ids_test = ActiveLearningUtility.get_dataset(
            self.features_df, self.feature_cols, self.label_col)

        print(f"Initiating Custom Active Learning procedure, predicting {label_col}, with features: {feature_cols}")

    # Fully automated learning
    def run_active_learning(self, n_iter=1, max_early_stop=2, early_stop_th=0.1,
                            sampler=JobExecutionSampler.USER_SPECIFIED, user_prompt=False):
        warnings.filterwarnings("ignore")

        # -> create model
        # -> predict labels
        # -> next iteration

        for idx in range(n_iter):
            # if user_prompt:
            #     val = input("Enter x to stop AL process, press any other key to continue: ")
            #     if val.strip() == "x":
            #         break
            self.active_learning_iteration_helper(idx, n_iter, max_early_stop, early_stop_th)
            new_train_ids, jobs_ids, sample = self.uncertainty_sampler()
            # new_train_ids, jobs_ids, sample = self.top_uncertainty_sampler(130)

            new_train_ids.to_csv(os.path.join(CONFIG.LABEL_FORECASTER_OUT, f"job_sample_ids_iteration_1.csv"), index=False)
            new_train_ids = list(new_train_ids.index.values)

            # np.savetxt(os.path.join(CONFIG.LABEL_FORECASTER_OUT, f"job_sample_ids_iteration_1.txt"), new_train_ids, fmt="%d")
            # self.active_learning_sampler_preparation(new_train_ids)

        results = self.active_learning_finalize(n_iter)
        return results

    def active_learning_finalize(self, n_iter):
        pred_jobs = pd.DataFrame(self.iterations_results[-1]["test_ids"])
        pred_jobs[f"pred_{self.label_col}"] = self.iterations_results[-1]["pred_labels"]
        pred_jobs[f"unc_low_{self.label_col}"] = self.iterations_results[-1]["uncertainty_low"]
        pred_jobs[f"unc_up_{self.label_col}"] = self.iterations_results[-1]["uncertainty_high"]
        pred_jobs = pred_jobs.set_index(["plan_id", "data_id"])
        final_dataset = pd.merge(self.features_df, pred_jobs, left_index=True, right_index=True, how="left")

        results = {
            "iterations": list(range(n_iter)),
            "data_size": self.data_size,
            "model_uncertainty": IQRs_mean,
            "test_scores": self.test_scores,
            "test_scores_exp": self.test_scores_exp,
            "cross_validation_scores": self.cross_validation_scores,
            "cross_validation_scores_exp": self.cross_validation_scores_exp,
            "iterations_results": self.iterations_results,
            "final_dataset": final_dataset
        }
        return results

    def active_learning_sampler_preparation(self, new_ids_train):
        # new_ids_train = self.ids_test.iloc[ids_train_new].copy()

        if len(new_ids_train) == 0:
            print("No more jobs to run, Early Stop!")
            return self.results

        print("Candidates to run:\n", new_ids_train)

        # -> RUN Jobs
        # new_jobs_to_run = new_ids_train.iloc[:, 0].values
        new_jobs_to_run = list(zip(new_ids_train.plan_id, new_ids_train.data_id))
        ActiveLearningUtility.submit_jobs(new_jobs_to_run)
        print("NEW JOBS TO RUN")
        print(new_jobs_to_run)
        # -> Collect exec time
        self.executed_jobs_runtime = ActiveLearningUtility.get_executed_plans_exec_time(new_jobs_to_run)


        for k, v in self.executed_jobs_runtime.iterrows():
            self.features_df.loc[k, "netRunTime"] = v.values[0]
        self.features_df[self.label_col] = np.log(self.features_df["netRunTime"])
        # self.features_df.to_csv(os.path.join(self.label_forecaster_out, "plan_data_features.csv"))

        self.X_train, self.y_train, self.ids_train, self.X_test, _, self.ids_test = ActiveLearningUtility.get_dataset(
            self.features_df, self.feature_cols,
            self.label_col)

        print(self.separator)

    # Get amount of jobs equal to uncertainty threshold
    def uncertainty_sampler(self):
        IRQ_th = np.quantile(self.iter_res["uncertainty_interval"], 0.95)
        len_new_X_train = len(self.X_test[self.iter_res["uncertainty_interval"] > IRQ_th])
        sampling_idx = np.random.randint(0, len(self.X_test), len_new_X_train)
        new_ids_train = self.ids_test.iloc[sampling_idx].copy()
        # job_ids = new_ids_train.iloc[:, 0].values
        # job_ids = new_ids_train.iloc[:, 0:2].values
        # print(new_ids_train.iloc[:, 0:2].index.tolist())
        # job_ids = new_ids_train.iloc[:, 0:2]
        print(new_ids_train.iloc[:, 0:2])
        job_ids = new_ids_train.iloc[:, 0:2]
        return job_ids, self.iter_res["uncertainty_interval"]

    # Get num amount of top uncertain jobs
    def top_uncertainty_sampler(self, num):
        # Get X top samples
        sampling_idx = np.argsort(self.iter_res["uncertainty_interval"])[-num:]
        new_ids_train = self.ids_test.iloc[sampling_idx].copy()
        job_ids = new_ids_train.iloc[:, 0].values
        return new_ids_train, job_ids, self.iter_res["uncertainty_interval"]

    def active_learning_iteration_helper(self, idx, n_iter, max_early_stop=2, early_stop_th=0.1):
        print(f"======= Active Learning Iteration {idx}=======")

        self.data_size.append(self.X_train.shape[0])
        print("Train:", self.X_train.shape)
        print("Test:", self.X_test.shape)
        # print(self.X_train)

        self.iter_res = self.active_learning_iteration(self.X_train, self.y_train, self.ids_train, self.X_test,
                                                       self.ids_test, self.feature_cols, idx,
                                                       verbose=self.verbose)
        # Save model iteration
        # with open(os.path.join(self.label_forecaster_out, f"learning_process_{idx}.pkl"), "wb") as handle:
        #     pickle.dump(iter_res, handle)

        # store info
        self.cross_validation_scores.append(self.iter_res["model"].cross_validation_scores)
        self.test_scores_exp.append(self.iter_res["model"].test_scores_exp)
        self.cross_validation_scores_exp.append(self.iter_res["model"].cross_validation_scores_exp)
        self.IQRs_mean.append(np.mean(np.abs(self.iter_res["uncertainty_interval"])))
        self.iter_res["model"] = str(self.iter_res["model"])
        self.iterations_results.append(self.iter_res)

        self.results = {
            # "iter_res": iter_res,
            "iterations": list(range(n_iter)),
            "data_size": self.data_size,
            # "model_uncertainty": IQRs_mean,
            "test_scores": self.test_scores,
            "test_scores_exp": self.test_scores_exp,
            "cross_validation_scores": self.cross_validation_scores,
            "cross_validation_scores_exp": self.cross_validation_scores_exp,
            "iterations_results": self.iterations_results,

        }

        if (idx + 1 >= n_iter):
            print("Max iteration reached!")
            return self.results

        if self.check_early_stop(self.iterations_results, early_stop_th):
            self.early_stop_count += 1
            if self.early_stop_count >= max_early_stop:
                print("Early stop reached!")
                return self.results
            else:
                print(f">>> Skip early stop {self.early_stop_count}. Max early stop is set to {max_early_stop}.")
        # Split up into actual running of model learning and preparation of jobs for next generation

        # # Prepare next iteration
        # if sampler is JobExecutionSampler.RANDOM_SAMPLER:
        #     IRQ_th = np.quantile(iter_res["uncertainty_interval"], 0.95)
        #     len_new_X_train = len(self.X_test[iter_res["uncertainty_interval"] > IRQ_th])
        #     sampling_idx = np.random.randint(0, len(self.X_test), len_new_X_train)
        #     new_ids_train = self.ids_test.iloc[sampling_idx].copy()
        #
        # elif sampler is JobExecutionSampler.UNCERTAINTY:  # Sampling based on uncertainty threshold
        #     IRQ_th = np.quantile(iter_res["uncertainty_interval"], 0.95)
        #     new_ids_train = self.ids_test.iloc[iter_res["uncertainty_interval"] > IRQ_th].copy()
        #
        # elif sampler is JobExecutionSampler.USER_SPECIFIED:
        #     # Custom job sampling; sample jobs inputted by user
        #     user_sampling = UserSampling()
        #     new_ids_train = user_sampling.user_specified_sampling(self.X_test, iter_res, self.ids_test)
        # print(self.ids_test)
        # print(ids_train_new)
        # new_ids_train = self.ids_test.iloc[ids_train_new].copy()
        #
        # if len(new_ids_train) == 0:
        #     print("No more jobs to run, Early Stop!")
        #     return self.results
        #
        # print("Candidates to run:\n", new_ids_train)
        #
        # # -> RUN Jobs
        # new_jobs_to_run = new_ids_train.iloc[:, 0].values
        # ActiveLearningUtility.submit_jobs(new_jobs_to_run)
        #
        # # -> Collect exec time
        # executed_jobs_runtime = ActiveLearningUtility.get_executed_plans_exec_time(new_jobs_to_run)
        #
        # for k, v in executed_jobs_runtime.iterrows():
        #     self.features_df.loc[k, "netRunTime"] = v.values[0]
        # self.features_df[self.label_col] = np.log(self.features_df["netRunTime"])
        #
        # self.X_train, self.y_train, self.ids_train, self.X_test, _, self.ids_test = ActiveLearningUtility.get_dataset(
        #     self.features_df, self.feature_cols,
        #     self.label_col)
        #
        # print(self.separator)
        return self.results

    def active_learning_iteration(self, X_train, y_train, ids_train, X_test, ids_test, feature_cols, idx, verbose=False):
        if X_train.__len__() != ids_train.__len__():
            raise Exception("x_train does not match ids_train")
        print("ACTIVE LEARNING ITERATION")
        print(ids_train)
        if X_test.__len__() != ids_test.__len__():
            raise Exception("x_test does not match ids_test")
        # print("IDS TEST TESTING!!!")
        # print(ids_test.index)
        # print()

        results = {}
        qf_model = QuantileForestModel(random_state=42)
        qf_model.fit(X_train, y_train)
        qf_model.cross_validate(X_train, y_train)

        y_pred = qf_model.predict(X_test)

        # Shapley values
        if(idx<1 and CONFIG.SHAP_IMAGES):
            self.shapley_calculation(qf_model, X_test, ids_test, feature_cols)
        y_pred_upper = qf_model.predict(X_test, quantile=75)
        y_pred_lower = qf_model.predict(X_test, quantile=25)

        if verbose:
            p = y_pred.argsort()
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot(y_pred[p], marker=".", linewidth=1, label="y_true", color="#1f77b4")
            ax.errorbar(np.arange(len(y_pred)), y_pred[p],
                        yerr=np.array([y_pred[p] - y_pred_lower[p], y_pred_upper[p] - y_pred[p]]), linewidth=0.5,
                        fmt='.',
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
                [np.exp(y_pred[p]) - np.exp(y_pred_lower[p]), np.exp(y_pred_upper[p]) - np.exp(y_pred[p])]),
                        linewidth=0.5,
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
        results["feature_importance"] = {"Feature": feature_cols,
                                         "F_Importance": qf_model.model.feature_importances_}

        return results

    def check_early_stop(self, iterations_results, th=0.1):
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

    # Makes SHAP figures for model and individual figures for testset
    def shapley_calculation(self, model, X_test, ids_test, feature_names):
        explainer = shap.KernelExplainer(model.predict, X_test)
        shap_values = explainer.shap_values(X=X_test)
        # explain_object = explainer.explain(X=X_test)
        print(shap.__version__)
        print(shap_values)

        # Summary Plot
        shap.summary_plot(shap_values, X_test, feature_names=feature_names, show=False)
        plt.savefig(os.path.join(self.label_forecaster_out, f"SHAP_Summary_Plot.png"), bbox_inches="tight")

        print(type(explainer))
        print(type(shap_values))
        plt.clf()
        plt.close()

        # Decision Plot
        shap.decision_plot(explainer.expected_value, shap_values, show=False, feature_names=feature_names)
        plt.savefig(os.path.join(self.label_forecaster_out, f"SHAP_Decision_Plot.png"), bbox_inches="tight")

        plt.clf()
        plt.close()

        shap.plots.force(explainer.expected_value, shap_values, show=True, feature_names=feature_names)
        plt.savefig(os.path.join(self.label_forecaster_out, f"SHAP_Force_Plot.png"), bbox_inches="tight")

        # shap.plots._waterfall.waterfall_legacy(explainer.expected_value, shap_values, show=False, feature_names=feature_names)
        # shap.plots.beeswarm(shap_values)
        # shap.force_plot(

        for i in range(0, len(shap_values)):
            plt.clf()
            # TODO: fix weird bug of overlapping shap plots
            shap.plots._waterfall.waterfall_legacy(explainer.expected_value, shap_values[i], show=False,
                                                   feature_names=feature_names)
            job_name = ids_test.iloc[i]["plan_id"]
            job_data_id = ids_test.iloc[i]["data_id"]

            plt.savefig(os.path.join(self.label_forecaster_out, f"SHAP_{job_name}_{job_data_id}_Waterfall.png"), bbox_inches="tight")
            plt.clf()
            plt.close()
            shap.plots.force(explainer.expected_value, shap_values[i], matplotlib=True, show=False,
                             feature_names=feature_names)
            plt.savefig(os.path.join(self.label_forecaster_out, f"SHAP_{job_name}_{job_data_id}_Force.png"), bbox_inches="tight")

    def get_iteration_results(self):
        return self.results
