import numpy as np
from sklearn.metrics import r2_score

np.random.seed(42)

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

figsize = (8, 4)


def show_r2(results):
    data_size = results["data_size"]
    test_scores = results["test_scores"]
    test_scores_exp = results["test_scores_exp"]
    
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(list(map(lambda x: x["r2"], test_scores)), marker="o", label="Log(Exec. time)", color="#777777")
    ax.plot(list(map(lambda x: x["r2"], test_scores_exp)), marker="o", label="Exec. time", color="#111111")
    ax.set_xticks(list(range(data_size.__len__())))
    ax.set_xticklabels(data_size, rotation=60)
    ax.set_ylim((0, 1))
    ax.set_yticks(np.arange(0, 1, 0.1))
    ax.set_xlabel("# Executed Jobs")
    ax.set_ylabel("$R^2$ Score")
    ax.legend()
    return ax


def compare_r2(results, results_real_card, results_random_sampling=None, exp=True):
    data_size = results["data_size"]
    if exp:
        test_scores_real = results_real_card["test_scores_exp"]
        test_scores = results["test_scores_exp"]
    else:
        test_scores_real = results_real_card["test_scores"]
        test_scores = results["test_scores"]

    fig, ax = plt.subplots(figsize=(8, 2))


    if results_random_sampling:
        if exp:
            test_scores_random = results_random_sampling["test_scores_exp"]
        else:
            test_scores_random = results_random_sampling["test_scores"]

        ax.plot(list(map(lambda x: x["r2"], test_scores_random)), marker="^", linestyle="dotted",
                label="Rand. samples - Estimated out card. (Baseline)",
                color=sns.color_palette()[-4])

    ax.plot(list(map(lambda x: x["r2"], test_scores)), marker="o", label="Active labeling - Estimated out card.",
            color="#111111")


    ax.plot(list(map(lambda x: x["r2"], test_scores_real)), linestyle="--", marker="s",
            label="Active labeling - Real out card. (Top-line)",
            color=sns.color_palette()[-3], alpha=0.85)

    ax.set_xticks(list(range(data_size.__len__())))
    ax.set_xticklabels(data_size, rotation=60)
    ax.set_ylim((0, 1))
    ax.set_yticks(np.arange(0, 1, 0.2))
    ax.set_xlabel("# Cumulated Executed Jobs")
    ax.set_ylabel("$R^2$ of pred.\nExec. Time")
    ax.legend()
    return ax


def show_uncertainty(results, show_errors=False):
    data_size = results["data_size"]
    IQRs_RMSE = results["model_uncertainty"]
    
    IQRs_RMSE = np.array([np.mean(np.exp(I["uncertainty_high"]) - np.exp(I["uncertainty_low"])) for I in results["iterations_results"]])
    IQRs_std = np.array([np.std(np.exp(I["uncertainty_high"]) - np.exp(I["uncertainty_low"])) for I in
                 results["iterations_results"]])
    
    fig, ax = plt.subplots(figsize=(8, 2))

    if show_errors:
        ax.errorbar(np.arange(len(IQRs_RMSE)),
                    IQRs_RMSE,
                    yerr=IQRs_std, fmt='o', label="Uncertainty")
    else:
        ax.plot(IQRs_RMSE, marker="o", label="Uncertainty")
    ax.set_xticks(list(range(data_size.__len__())))
    ax.set_xticklabels(data_size, rotation=60)
    ax.set_xlabel("# Cumulated Executed Jobs")
    ax.set_ylabel("Model\nUncertainty [ms]")
    
    final_th = 0.1
    count = 0

    min_u = IQRs_RMSE[0]
    min_local_u = IQRs_RMSE[0]
    stops = []

    for i in range(1, len(data_size)):
        #print(i, " -> min_local_u", min_local_u)
        r = IQRs_RMSE[i] / min_local_u
        #print(r)
        if (r > 1) or (IQRs_RMSE[i]>min_u):
            pass
        elif (1-r) < final_th:
            pass
        else:
            print(i, data_size[i], "-> STOP!")
            count += 1
            stops.append({"iteration": i, "data_size": data_size[i],
                          "uncertainty": IQRs_RMSE[i],
                          "uncertainty_std": IQRs_std[i],
                          "cost": np.sum(np.exp(results["iterations_results"][i]["train_labels"]))
                          })

            print("--------------------------------")
        min_u = min(IQRs_RMSE[:i+1])
        min_local_u = min(IQRs_RMSE[i-1:i+1])
        #min_cost_id = np.argwhere(IQRs_RMSE == min_cost)
    
    if len(stops) == 0:
        stops.append({"iteration": len(data_size)-1, "data_size": data_size[len(data_size)-1], "cost": np.sum(np.exp(results["iterations_results"][len(data_size)-1]["train_labels"])) })

    ax.errorbar([s["iteration"] for s in stops], [s["uncertainty"] for s in stops], color="red", label="Early stop", linewidth=0, marker="o" )
    
    ax.legend()
    print(pd.DataFrame(stops))
    return ax

def show_iteration(results, iteration_to_show, exp=False, drop_outliers=False):
    y_test = results["iterations_results"][iteration_to_show]["test_labels"]
    y_pred = results["iterations_results"][iteration_to_show]["pred_labels"]
    y_pred_lower = results["iterations_results"][iteration_to_show]["uncertainty_low"]
    y_pred_upper = results["iterations_results"][iteration_to_show]["uncertainty_high"]
    p = y_test.argsort()

    if drop_outliers:
        q = np.quantile(y_test, 0.97)
        print(q)
        out_mask = y_test < q
        print(out_mask.shape)
        y_test = y_test[out_mask]
        y_pred = y_pred[out_mask]
        y_pred_lower = y_pred_lower[out_mask]
        y_pred_upper = y_pred_upper[out_mask]
        p = y_test.argsort()

    
    fig, ax = plt.subplots(figsize=(6, 3))


    if exp:
        y_test = np.exp(y_test)
        y_pred = np.exp(y_pred)
        y_pred_lower = np.exp(y_pred_lower)
        y_pred_upper = np.exp(y_pred_upper)
        if drop_outliers:
            new_r2 = r2_score(y_test, y_pred)
            print("NEW R2 without outliers:", new_r2)
        
        ax.plot(y_test[p], marker=".", linewidth=1, label="Real", color="#777777", alpha=0.5)
        ax.errorbar(np.arange(len(y_pred)),y_pred[p], yerr=np.array([y_pred[p] - y_pred_lower[p], y_pred_upper[p] -  y_pred[p]]), linewidth=0.5, fmt='.', color="#ff7f0e", label="Pred. + Interval", alpha=0.5)

        #ax.plot(np.arange(len(y_pred)), (y_pred_lower[p]+y_pred_upper[p])/2, marker=".", linewidth=0, label="smooth", color="green")

        ax.set_ylabel("Exec. Time [ms]")
        # ax.ticklabel_format(axis='y', style='sci', scilimits=(0, 3))
        #ax.set_yscale("log")
        ax.set_xlabel("Non-executed Jobs")
        ax.legend()

        print(results["test_scores_exp"][iteration_to_show])

    else:
        ax.plot(y_test[p], marker=".", linewidth=1, label="Real", color="#777777", alpha=0.5)
        ax.errorbar(np.arange(len(y_pred)), y_pred[p], yerr=np.array([y_pred[p] - y_pred_lower[p], y_pred_upper[p] -  y_pred[p]]), linewidth=0.5, fmt='.', color="#ff7f0e", label="Pred. + Interval", alpha=0.5)
        ax.set_ylabel("Log(Exec. Time)")
        ax.set_xlabel("Non-executed Jobs")
        ax.legend()

        print(results["test_scores"][iteration_to_show])

    return ax

def show_iteration_2(results, iteration_to_show, drop_outliers=False):
    y_test = results["iterations_results"][iteration_to_show]["test_labels"]
    y_pred = results["iterations_results"][iteration_to_show]["pred_labels"]
    y_pred_lower = results["iterations_results"][iteration_to_show]["uncertainty_low"]
    y_pred_upper = results["iterations_results"][iteration_to_show]["uncertainty_high"]
    p = y_test.argsort()

    new_r2 = r2_score(y_test, y_pred)
    print("NEW R2 log with outliers:", new_r2)

    if drop_outliers:
        q = np.quantile(y_test, 0.97)
        print(q)
        out_mask = y_test < q
        print(out_mask.shape)
        y_test = y_test[out_mask]
        y_pred = y_pred[out_mask]
        y_pred_lower = y_pred_lower[out_mask]
        y_pred_upper = y_pred_upper[out_mask]
        p = y_test.argsort()

    fig, ax = plt.subplots(figsize=(6, 6))

    y_test = np.exp(y_test)
    y_pred = np.exp(y_pred)
    y_pred_lower = np.exp(y_pred_lower)
    y_pred_upper = np.exp(y_pred_upper)

    if drop_outliers:
        new_r2 = r2_score(y_test, y_pred)
        print("NEW R2 without outliers:", new_r2)

    ax.plot(y_test[p], y_test[p], marker=".", linewidth=1, label="Real", color="#777777", alpha=0.5)
    ax.errorbar(y_test[p],y_pred[p], yerr=np.array([y_pred[p] - y_pred_lower[p], y_pred_upper[p] -  y_pred[p]]), linewidth=0.5, fmt='.', color="#ff7f0e", label="Pred. + Interval", alpha=0.5)
    ax.set_ylabel("Forecasted Exec. Time [ms] (Log scale)")
    ax.set_yscale("log")
    ax.set_xlabel("Real Exec. Time [ms] (Log scale)")
    ax.set_xscale("log")
    ax.legend()
    
    return ax


def show_td_gen(results, iteration_to_show):

    y_test = results[list(results.keys())[iteration_to_show]]["test_labels"]
    y_pred = results[list(results.keys())[iteration_to_show]]["pred_labels"]

    from sklearn.metrics import r2_score
    score = r2_score(y_test, y_pred)
    print("R2 score:", score)
    p = y_test.argsort()

    fig, ax = plt.subplots(figsize=(6, 3))

    ax.plot(y_test[p], y_test[p], marker=".", linewidth=1, label="Real", color="#777777", alpha=0.5)
    ax.plot(y_test[p], y_pred[p], marker=".", linewidth=0, label="TDGen Pred.", color=sns.color_palette()[4], alpha=0.5)

    ax.set_ylabel("Forecasted Exec. Time [ms] (Log scale)")
    ax.set_yscale("log")
    ax.set_xlabel("Real Exec. Time [ms] (Log scale)")
    ax.set_xscale("log")
    ax.legend()

    return ax

def show_our_and_td_gen(our_results, td_gen_results, iteration_to_show):

    our_y_test = np.exp(our_results["iterations_results"][iteration_to_show]["test_labels"])
    our_y_pred = np.exp(our_results["iterations_results"][iteration_to_show]["pred_labels"])

    y_test = td_gen_results[list(td_gen_results.keys())[iteration_to_show]]["test_labels"]
    y_pred = td_gen_results[list(td_gen_results.keys())[iteration_to_show]]["pred_labels"]

    from sklearn.metrics import r2_score
    score = r2_score(y_test, y_pred)
    print("R2 score:", score)
    p = y_test.argsort()
    our_p = our_y_test.argsort()

    fig, ax = plt.subplots(figsize=(6, 6))

    ax.plot(y_test[p], y_test[p], marker="", linewidth=1, label="Real", color="#777777", alpha=0.5)

    ax.plot(our_y_test[our_p], our_y_pred[our_p], marker=".", linewidth=0, label="Our solution", color=sns.color_palette()[1], alpha=0.2)
    ax.plot(y_test[p], y_pred[p], marker=".", linewidth=0, label="TDGen Pred.", color=sns.color_palette()[4], alpha=0.2)

    ax.set_ylabel("Forecasted Exec. Time [ms] (Log scale)")
    ax.set_yscale("log")
    ax.set_xlabel("Real Exec. Time [ms] (Log scale)")
    ax.set_xscale("log")
    ax.legend()

    return ax


def compare_td_gen_r2(results, results_td_gen):
    data_size = results["data_size"]

    test_scores = results["test_scores_exp"]

    from sklearn.metrics import r2_score
    td_gen_scores = []
    x = []

    for k, v in results_td_gen.items():
        y_test = v["test_labels"]
        y_pred = v["pred_labels"]
        score = r2_score(y_test, y_pred)
        print(k ,"R2 score:", score)
        td_gen_scores.append(score)
        x.append(k)

    fig, ax = plt.subplots(figsize=(8, 2))
    ax.plot(td_gen_scores, linestyle="--", marker="o", label="TDGen",
            color=sns.color_palette()[4])
    ax.plot(list(map(lambda x: x["r2"], test_scores)), marker="o", label="Our solution",
            color="#111111")
    ax.set_xticks(list(range(data_size.__len__())))
    ax.set_xticklabels(data_size, rotation=60)

    print(np.array(list(map(lambda x: x["r2"], test_scores)))/np.array(td_gen_scores))
    #ax.set_ylim((0, 1))
    #ax.set_yticks(np.arange(0, 1, 0.1))
    ax.set_xlabel("# Cumulated Executed Jobs")
    ax.set_ylabel("$R^2$ of pred. Exec. Time")
    ax.legend()
    return ax


def show_centerd_uncertainty(data, iteration, exp=False):
    print(data["iterations_results"][iteration].keys())
    if exp:
        preds = np.exp(np.array(data["iterations_results"][iteration]["pred_labels"]))
        upper = np.exp(np.array(data["iterations_results"][iteration]["uncertainty_high"]))
        lower = np.exp(np.array(data["iterations_results"][iteration]["uncertainty_low"]))
    else:
        preds = np.array(data["iterations_results"][iteration]["pred_labels"])
        upper = np.array(data["iterations_results"][iteration]["uncertainty_high"])
        lower = np.array(data["iterations_results"][iteration]["uncertainty_low"])

    IQR_interval = upper - lower
    sort_ind = np.argsort(IQR_interval)
    # y_true_all = y_true_all[sort_ind]
    preds = preds[sort_ind]
    upper = upper[sort_ind]
    lower = lower[sort_ind]
    mean = (upper + lower) / 2
    std = np.std((upper + lower))
    # Center such that the mean of the prediction interval is at 0.0
    # y_true_all_centered = y_true_all.copy()
    upper_centered = upper.copy()
    lower_centered = lower.copy()
    preds_centered = preds.copy()

    # y_true_all_centered -= mean
    upper_centered = (upper_centered - mean)  # /std
    lower_centered = (lower_centered - mean)  # /std
    preds_centered = (preds_centered - mean)  # /std

    IRQ_th = np.quantile(IQR_interval, 0.95)
    print(IRQ_th)
    x_idx = np.arange(len(upper_centered))
    cut = x_idx[IQR_interval[sort_ind] > IRQ_th]
    print(cut)

    fig, ax = plt.subplots(1, 1, figsize=(8, 4))

    # ax.plot(y_true_all_centered, "ro", markersize=1)
    ax.plot(preds_centered, marker=".", color="#ff7f0e", linewidth=0)

    ax.fill_between(
        np.arange(len(upper_centered)), lower_centered, upper_centered, alpha=0.2, color="#ff7f0e",
        label="Pred. interval (centerd)")
    ax.axvline(cut[0], color="red", linestyle="--", label="Threshold $\eta$")
    ax.set_xlabel("Non-executed jobs sorted by uncertainty.")
    ax.set_ylabel("Predicted values (centered)")
    ax.legend()
    #  ax.set_yscale("symlog")
    #  ax.set_ylim([-1.5, 1.5])


def compute_stats_on_pred_errors(results, iteration_to_show):
    y_train = results["iterations_results"][iteration_to_show]["train_labels"]
    y_test = results["iterations_results"][iteration_to_show]["test_labels"]
    y_pred = results["iterations_results"][iteration_to_show]["pred_labels"]
    y_pred_lower = results["iterations_results"][iteration_to_show]["uncertainty_low"]
    y_pred_upper = results["iterations_results"][iteration_to_show]["uncertainty_high"]

    y_train = np.exp(y_train)
    y_test = np.exp(y_test)
    y_pred = np.exp(y_pred)
    y_pred_lower = np.exp(y_pred_lower)
    y_pred_upper = np.exp(y_pred_upper)

    print("Real values")
    print(pd.Series(np.hstack((y_train, y_test)) / 1000).describe())
    print("highest 5:", np.sort(np.hstack((y_train, y_test)))[-5:]/1000)
    print()
    print("\nAverage Prediction Error")
    print(pd.Series(np.abs(y_test - y_pred) / 1000).describe())

    # count_true = (y_test <= y_pred_upper) & (y_test >= y_pred_lower)
    # print(len(count_true),len(count_true[count_true==True]))