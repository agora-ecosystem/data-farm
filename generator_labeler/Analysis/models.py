from IPython.core.display import display
from statsmodels.api import OLS
from sklearn.metrics import r2_score, mean_absolute_error, classification_report, f1_score
from sklearn.model_selection import GridSearchCV, train_test_split

from generator_labeler.FeatureExtraction.PredictorFeatureExtraction import show_feature_corr

import matplotlib.pyplot as plt
import seaborn as sns

import pandas as pd
import numpy as np

sns.set_context("talk")
sns.set_style("whitegrid")


def compute_LR(feat_df, label_col='netRunTime'):
    model = OLS(
        feat_df[label_col],
        feat_df.loc[:, feat_df.columns != label_col]
    ).fit(cov_type='HC3')
    res = model.summary()
    return model, res


def get_X_y(df, feature_cols, label_col):
    return df.loc[:,feature_cols].values, df.loc[:,label_col].values


def split_dev_test(jobs_indexes):
    dev_jobs_indexes = np.random.choice(jobs_indexes, int(0.7*jobs_indexes.__len__()), replace=False)
    test_jobs_indexes = jobs_indexes[~jobs_indexes.isin(dev_jobs_indexes)]
    return dev_jobs_indexes, test_jobs_indexes


def validate_RFR(df, label_col, feature_cols, validation_data, train_size=0.5):
    from sklearn.ensemble import RandomForestRegressor

    params = {
        "n_estimators": [10, 50, 100, 200, 500],
        "max_depth": [10, 50, 100, 200, 500]
    }
    score = "r2"
    clf = RandomForestRegressor()

    clf, s = test_model_3(df, feature_cols, label_col, clf, params, score,
                          test_size=1 - train_size, is_regression=True, verbose=True)

    from sklearn.metrics import r2_score

    val_df = validation_data.copy()
    val_df["Log_netRunTime"] = np.log(val_df["netRunTime"])
    X_val = val_df[feature_cols].values
    y_val_true = val_df["Log_netRunTime"].values

    y_val_pred = clf.predict(X_val)

    score_res = r2_score(y_val_true, y_val_pred)

    print("Validation r2:", score_res)
    fig, ax = plt.subplots(figsize=(15, 6))
    p = y_val_true.argsort()
    ax.plot(y_val_true[p], marker=".", linewidth=1, label="y_val_true")
    ax.plot(y_val_pred[p], marker=".", linewidth=0, label="y_val_pred")
    ax.set_title(
        f"{type(clf.estimator).__name__} - {label_col} - Train: {(train_size):.2f} - Score[{score}]: {score_res:.2f}")
    ax.set_ylabel("Target value")
    ax.set_xlabel("Test jobs")
    ax.legend()
    plt.show()
    plt.close()

    fig, ax = plt.subplots(figsize=(15, 6))
    p = y_val_true.argsort()
    r2_exp = r2_score(np.exp(y_val_true), np.exp(y_val_pred))
    print("Validation r2_exp:", r2_exp)

    ax.plot(np.exp(y_val_true[p]), marker=".", linewidth=1, label="y_val_true")
    ax.plot(np.exp(y_val_pred[p]), marker=".", linewidth=0, label="y_val_pred")
    ax.set_ylim((0, 100000))
    ax.set_title(f"EXP - {type(clf.estimator).__name__} - {label_col} - Train: {(train_size):.2f} - Score[{score}]: {r2_exp:.2f}")
    ax.set_ylabel("Target value (exp)")
    ax.set_xlabel("Test jobs")
    ax.legend()
    plt.show()
    plt.close()

    return score_res, r2_exp


def build_RFR(df, label_col, feature_cols, validation_data=None):
    from sklearn.ensemble import RandomForestRegressor

    params = {
        "n_estimators": [10, 50, 100, 200, 500],
        "max_depth": [10, 50, 100, 200, 500]
    }
    score = "r2"
    clf = RandomForestRegressor()

    scores = []
    for train_size in np.arange(0.1, 1.0, 0.1):
        _, s = test_model_3(df, feature_cols, label_col, clf, params, score,
                            test_size=1 - train_size, is_regression=True, verbose=True)
        scores.append(s)

    plt.plot(np.arange(0.1, 1.0, 0.1), scores, marker="o")
    plt.xlabel("Relative train size")
    plt.xticks(np.arange(0.1, 1, 0.1))
    plt.ylabel(score, rotation=0)
    plt.ylim(0.5, 1)
    plt.show()
    plt.close()

    if validation_data is not None:
        print("\n\nValidation\n\n")

        validate_RFR(df, label_col, feature_cols, validation_data)


def test_model_3(df, feature_cols, label_col, clf, params, score, test_size=0.2, is_regression=False, train_test_split_func=None, verbose=False):
    X, y = get_X_y(df, feature_cols, label_col)

    if train_test_split_func is None:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
    else:
        X_train, X_test, y_train, y_test = train_test_split_func(X, y, test_size=test_size, random_state=42)

    print("Train size:", X_train.shape)
    print("Test size:", X_test.shape)

    if is_regression:
        clf, score_res = build_regressor(X_train, y_train, X_test, y_test, clf, score, params, n_folds=3)
    else:
        clf, score_res = build_classifier(X_train, y_train, X_test, y_test, clf, score, params, n_folds=3)

    try:
        display(pd.DataFrame({"Feature": feature_cols, "feature_importance": clf.best_estimator_.feature_importances_}) \
                .sort_values("feature_importance", ascending=False).head(15))
    except:
        print("Skip feature importance")

    if verbose:
        y_pred = clf.predict(X_test)

        fig, ax = plt.subplots(figsize=(15, 6))
        p = y_test.argsort()
        ax.plot(y_test[p], marker=".", linewidth=1, label="y_true")
        ax.plot(y_pred[p], marker=".", linewidth=0, label="y_pred")
        ax.set_title(f"{type(clf.estimator).__name__} - {label_col} - Train: {(1 - test_size):.2f} - Score[{score}]: {score_res:.2f}")
        ax.set_ylabel("Target value")
        ax.set_xlabel("Test jobs")
        ax.legend()
        plt.show()
        plt.close()

        if "Log" in label_col:
            fig, ax = plt.subplots(figsize=(15, 6))
            p = y_test.argsort()
            from sklearn.metrics import r2_score
            r2_exp = r2_score(np.exp(y_test), np.exp(y_pred))
            print("r2_exp:", r2_exp)

            ax.plot(np.exp(y_test[p]), marker=".", linewidth=1, label="y_true")
            ax.plot(np.exp(y_pred[p]), marker=".", linewidth=0, label="y_pred")
            ax.set_ylim((0, 100000))
            ax.set_title(f"EXP - {type(clf.estimator).__name__} - {label_col} - Train: {(1 - test_size):.2f} - Score[{score}]: {r2_exp:.2f}")
            ax.set_ylabel("Target value (exp)")
            ax.set_xlabel("Test jobs")
            ax.legend()
            plt.show()
            plt.close()

        if is_regression:
            fig, ax = plt.subplots(figsize=(15, 6))
            sns.distplot(np.abs(y_test[p] - y_pred[p]), ax=ax, bins=100, kde=False, norm_hist=False)
            ax.set_title("Distribution of absolute errors")
            ax.set_ylabel("# instances")
            ax.set_xlabel("Absolute error")
            plt.show()
            plt.close()

    return clf, score_res


def test_model(df, feature_cols, label_col, clf, params, score, test_size=0.2, is_regression=False,
               remove_outliers=False, outlier_col=None, train_test_split_func=None, verbose=False):
    if verbose:
        ax = show_feature_corr(df.loc[:, feature_cols + [label_col]])
        plt.show()
        plt.close()

    if remove_outliers and outlier_col:
        out_quant = np.quantile(df[outlier_col], 0.99)
        print(f"Removing outlires with label value lower then: {out_quant}")
        df = df[df[outlier_col] < out_quant]

    X, y = get_X_y(df, feature_cols, label_col)

    if train_test_split_func is None:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
    else:
        X_train, X_test, y_train, y_test = train_test_split_func(X, y, test_size=test_size, random_state=42)

    if is_regression:
        clf, score_res = build_regressor(X_train, y_train, X_test, y_test, clf, score, params, n_folds=3)
    else:
        clf, score_res = build_classifier(X_train, y_train, X_test, y_test, clf, score, params, n_folds=3)

    if verbose:
        y_pred = clf.predict(X_test)

        fig, ax = plt.subplots(figsize=(15, 6))
        p = y_test.argsort()
        ax.plot(y_test[p], marker=".", linewidth=1)
        ax.plot(y_pred[p], marker=".", linewidth=0)
        ax.set_title()
        ax.set_yabel("Value")
        ax.set_xlabel("Test jobs")
        plt.show()
        plt.close()

        if is_regression:
            fig, ax = plt.subplots(figsize=(15, 6))
            sns.distplot(np.abs(y_test[p] - y_pred[p]), ax=ax, bins=100, kde=False, norm_hist=False)
            plt.show()
            plt.close()

    return clf, score_res


def build_model(X_train, y_train, X_test, y_test, clf_to_evaluate, score, param_grid, n_folds=3 , verbose=False):
    print("# Tuning hyper-parameters for %s" % score) if verbose else ""
    print() if verbose else ""

    clf = GridSearchCV(clf_to_evaluate, param_grid, cv=n_folds,
                       scoring=score, verbose=True, n_jobs=4, iid=False)
    clf.fit(X_train, y_train)

    print("Best parameters set found on development set:")
    # print()
    print(clf.best_params_)

    if verbose:
        print()
        print("Grid scores on development set:")
        print()
        means = clf.cv_results_['mean_test_score']
        stds = clf.cv_results_['std_test_score']
        for mean, std, params in zip(means, stds, clf.cv_results_['params']):
            print("%0.3f (+/-%0.03f) for %r"
                  % (mean, std * 2, params))
        print()

    return clf


def build_classifier(X_train, y_train, X_test, y_test, clf_to_evaluate, score, param_grid, n_folds=3 ):
    clf = build_model(X_train, y_train, X_test, y_test, clf_to_evaluate, score, param_grid, n_folds=n_folds)

    print("Detailed classification report:")
    print()
    y_true, y_pred = y_test, clf.predict(X_test)
    print(classification_report(y_true, y_pred))
    print()

    f1 = f1_score(y_true, y_pred, average="macro")
    return clf, f1


def build_regressor(X_train, y_train, X_test, y_test, clf_to_evaluate, score, param_grid, n_folds=3 ):

    clf = build_model(X_train, y_train, X_test, y_test, clf_to_evaluate, score, param_grid, n_folds=n_folds)

    print("Detailed classification report:")
    print()
    y_true, y_pred = y_test, clf.predict(X_test)
    r2 = r2_score(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    print("r2_score:", r2)
    print("mae_score:", mae)
    print()
    return clf, r2