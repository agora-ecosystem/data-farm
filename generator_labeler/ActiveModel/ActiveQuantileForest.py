# from skgarden import RandomForestQuantileRegressor
from generator_labeler.ActiveModel import RandomForestQuantileRegressor
from sklearn.model_selection import GridSearchCV
import numpy as np

from sklearn.metrics import r2_score
from sklearn.metrics import mean_tweedie_deviance
from sklearn.model_selection import KFold

import matplotlib.pyplot as plt


class ActiveQuantileForest:
    def __init__(self):
        pass


class QuantileForestModel:

    def __init__(self, random_state=None):
        self.model = RandomForestQuantileRegressor(random_state=random_state)
        self.gs_model = None
        self.scoring_function = "neg_mean_squared_error"
        self.param_grid = {
            "n_estimators": [10, 50, 100, 200, 500],
            "max_depth": [10, 50, 100, 200, 500],
            #"min_samples_split": [2, 10],
            "random_state": [random_state]
        }
        self.n_folds = 3
        self._is_fitted = False

        self.test_scores = None
        self.test_scores_exp = None

        self.cross_validation_scores = None
        self.cross_validation_scores_exp = None

    def cross_validate(self, X, y, k=None):
        if not self._is_fitted:
            raise Exception("Model is not fitted.")

        if self.gs_model is None:
            raise Exception("Cross validation requires a grid search model.")

        if k == None:
            k = int(X.shape[0] / 2) if X.shape[0] < 3 else 3

        print(f"Cross validating on '{k}' folds")

        scores = {}
        scores["r2"] = []
        scores["mse"] = []
        scores["poisson_deviance"] = []
        scores["gamma_deviance"] = []

        scores_exp = {}
        scores_exp["r2"] = []
        scores_exp["mse"] = []
        scores_exp["poisson_deviance"] = []
        scores_exp["gamma_deviance"] = []

        kf = KFold(k)

        for train_index, test_index in kf.split(X):

            clf_model = RandomForestQuantileRegressor(**self.gs_model.best_params_)
            clf_model.fit(X[train_index, :], y[train_index])

            y_pred = clf_model.predict(X[test_index, :])
            y_test = y[test_index]

            if test_index.__len__() > 1:
                r2 = r2_score(y_test, y_pred)
                scores["r2"].append(r2)

                r2_exp = r2_score(np.exp(y_test), np.exp(y_pred))
                scores_exp["r2"].append(r2_exp)

            mse = mean_tweedie_deviance(y_test, y_pred, power=0)
            poisson_deviance = mean_tweedie_deviance(y_test, y_pred, power=1)
            gamma_deviance = mean_tweedie_deviance(y_test, y_pred, power=2)

            scores["mse"].append(mse)
            scores["poisson_deviance"].append(poisson_deviance)
            scores["gamma_deviance"].append(gamma_deviance)

            mse_exp = mean_tweedie_deviance(np.exp(y_test), np.exp(y_pred), power=0)
            poisson_deviance_exp = mean_tweedie_deviance(np.exp(y_test), np.exp(y_pred), power=1)
            gamma_deviance_exp = mean_tweedie_deviance(np.exp(y_test), np.exp(y_pred), power=2)

            scores_exp["mse"].append(mse_exp)
            scores_exp["poisson_deviance"].append(poisson_deviance_exp)
            scores_exp["gamma_deviance"].append(gamma_deviance_exp)

        final_scores = {}
        final_scores["r2_mean"] = np.mean([0 if s < 0 else s for s in scores["r2"]])
        final_scores["r2_std"] = np.std([0 if s < 0 else s for s in scores["r2"]])
        final_scores["mse_mean"] = np.mean(scores["mse"])
        final_scores["mse_std"] = np.std(scores["mse"])
        final_scores["poisson_deviance_mean"] = np.mean(scores["poisson_deviance"])
        final_scores["poisson_deviance_std"] = np.std(scores["poisson_deviance"])
        final_scores["gamma_deviance_mean"] = np.mean(scores["gamma_deviance"])
        final_scores["gamma_deviance_std"] = np.std(scores["gamma_deviance"])
        self.cross_validation_scores = final_scores

        final_scores_exp = {}
        final_scores_exp["r2_mean"] = np.mean([0 if s < 0 else s for s in scores_exp["r2"]])
        final_scores_exp["r2_std"] = np.std([0 if s < 0 else s for s in scores_exp["r2"]])
        final_scores_exp["mse_mean"] = np.mean(scores_exp["mse"])
        final_scores_exp["mse_std"] = np.std(scores_exp["mse"])
        final_scores_exp["poisson_deviance_mean"] = np.mean(scores_exp["poisson_deviance"])
        final_scores_exp["poisson_deviance_std"] = np.std(scores_exp["poisson_deviance"])
        final_scores_exp["gamma_deviance_mean"] = np.mean(scores_exp["gamma_deviance"])
        final_scores_exp["gamma_deviance_std"] = np.std(scores_exp["gamma_deviance"])
        self.cross_validation_scores_exp = final_scores_exp

        return self.cross_validation_scores, self.cross_validation_scores_exp

    def validate(self, X_test, y_test):
        if not self._is_fitted:
            raise Exception("Model is not fitted.")

        scores = {}
        scores_exp = {}

        y_pred = self.model.predict(X_test)

        scores["r2"] = r2_score(y_test, y_pred)
        scores["mse"] = mean_tweedie_deviance(y_test, y_pred, power=0)
        scores["poisson_deviance"] = mean_tweedie_deviance(y_test, y_pred, power=1)
        scores["gamma_deviance"] = mean_tweedie_deviance(y_test, y_pred, power=2)
        self.test_scores = scores

        scores_exp["r2"] = r2_score(np.exp(y_test), np.exp(y_pred))
        scores_exp["mse"] = mean_tweedie_deviance(np.exp(y_test), np.exp(y_pred), power=0)
        scores_exp["poisson_deviance"] = mean_tweedie_deviance(np.exp(y_test), np.exp(y_pred), power=1)
        scores_exp["gamma_deviance"] = mean_tweedie_deviance(np.exp(y_test), np.exp(y_pred), power=2)
        self.test_scores_exp = scores_exp

        return self.test_scores, self.test_scores_exp

    def _fit_grid_search(self, X_train, y_train, verbose=False):
        self.gs_model = GridSearchCV(self.model, self.param_grid, cv=self.n_folds,
                           scoring=self.scoring_function, verbose=True, n_jobs=4, iid=False)
        self.gs_model.fit(X_train, y_train)
        self.model = self.gs_model.best_estimator_

        # self.model = RandomForestQuantileRegressor(**self.gs_model.best_params_)
        # self.model.fit(X_train, y_train)
        return self.model

    def predict(self, X, quantile=None):
        if not self._is_fitted:
            raise Exception("Model is not fitted.")
        return self.model.predict(X, quantile=quantile)

    def fit(self, X, y, grid_search=True, verbose=False):
        if grid_search:
            self.model = self._fit_grid_search(X, y, verbose)
        else:
            self.model = self.model.fit(X, y)

        self._is_fitted = True

        return self.model

    def predict_model_uncertainty(self, X_test, upper=75, lower=25, verbose=False):
        preds = self.predict(X_test)
        upper = self.predict(X_test, quantile=upper)
        lower = self.predict(X_test, quantile=lower)
        #y_true_all = y_test

        IQR_interval = upper - lower
        sort_ind = np.argsort(IQR_interval)
        #y_true_all = y_true_all[sort_ind]
        preds = preds[sort_ind]
        upper = upper[sort_ind]
        lower = lower[sort_ind]
        mean = (upper + lower) / 2
        std = np.std((upper + lower))
        # Center such that the mean of the prediction interval is at 0.0
        #y_true_all_centered = y_true_all.copy()
        upper_centered = upper.copy()
        lower_centered = lower.copy()
        preds_centered = preds.copy()

        #y_true_all_centered -= mean
        upper_centered = (upper_centered - mean)#/std
        lower_centered = (lower_centered - mean)#/std
        preds_centered = (preds_centered - mean)#/std

        if verbose:
            fig, ax = plt.subplots(1, 1, figsize=(10, 6))

            # ax.plot(y_true_all_centered, "ro", markersize=1)
            ax.plot(preds_centered, marker=".", color="#ff7f0e", linewidth=0)

            ax.fill_between(
                np.arange(len(upper_centered)), lower_centered, upper_centered, alpha=0.2, color="#ff7f0e",
                label="Pred. interval")
            ax.set_xlabel("Non-executed instances ordered by uncertainty.")
            ax.set_ylabel("Predicted values (centered)")
            ax.set_ylim([-1.5, 1.5])
            #plt.show()

        return IQR_interval


