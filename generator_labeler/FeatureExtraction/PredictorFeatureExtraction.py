import pandas as pd
import generator_labeler.ExecutionPlanAnalyzer as EPAnalyzer
from IPython.display import display
import numpy as np
from scipy.stats import kurtosis, skew
import matplotlib.pyplot as plt
import seaborn as sns


import networkx as nx
import re


####################
## Table features ##
####################
from generator_labeler.TaskMonitor.TaskManagerClient import TaskDetailsParser


def get_tables_features(df_tables, data_cardinality=False, factors=-1, verbose=True):

    one_hot = sorted(list(set([t for tables in df_tables.values for t in tables])))
    one_hot_enc = {one_hot[c]: c for c in range(one_hot.__len__())}
    one_hot_dec = {c: one_hot[c] for c in range(one_hot.__len__())}
    print("one_hot_enc:", one_hot_enc, "\none_hot_dec:", one_hot_dec)

    def encode(x, one_hot_enc):
        enc_vec = np.zeros(one_hot_enc.__len__())
        for v in x:
            idx = one_hot_enc[v]
            enc_vec[idx] = 1.0

        return enc_vec

    df_tables_enc = df_tables.apply(lambda x: encode(x, one_hot_enc))

    if data_cardinality:
        cards = []
        ks = []
        for k, values in df_tables_enc.iteritems():
            new_values = [v * data_cardinality[k[1]][one_hot_dec[idx]] for idx, v in enumerate(values)]
            cards.append(new_values)
            ks.append(k)

        cards_df = pd.DataFrame(cards)
        cards_df["plan_id"] = [k[0] for k in ks]
        cards_df["data_id"] = [k[1] for k in ks]
        cards_df = cards_df.set_index(["plan_id", "data_id"])
        df_tables_enc = cards_df

    df_tables_enc = df_tables_enc.apply(pd.Series)
    df_tables_enc.columns = ["t_" + str(c) for c in df_tables_enc.columns]

    if factors > 0:
        from sklearn.decomposition import PCA
        pca_model = PCA(n_components=factors, svd_solver="auto")
        df_tables_enc_factors = pca_model.fit_transform(df_tables_enc)

        for f in range(factors):
            df_tables_enc[f"t_F{f + 1}"] = df_tables_enc_factors[:, f]

        if verbose:
            print("n_components:", pca_model.n_components,
                  "\nexplained_variance_ :", pca_model.explained_variance_,
                  "\nexplained_variance_ratio_ :", np.cumsum(pca_model.explained_variance_ratio_))

    return df_tables_enc


###################
## DEPRECATED! - Data sampling ##
###################
def sample_by_value_dist(df, sample_size, sample_col, verbose=True):
    values = df[sample_col].values
    arg_s_values = values.argsort()

    if verbose:
        plt.plot(values, marker='.', linewidth=0)
        plt.ylabel(sample_col)
        plt.xlabel("Samples")
        plt.show()

    norm_values = values / max(values)

    if verbose:
        plt.plot(norm_values[arg_s_values], marker=".")
        plt.ylabel("Norm - " + sample_col)
        plt.xlabel("Samples")
        plt.show()

    n_samples = int(norm_values.__len__() * sample_size)

    th_step = (norm_values.max() - norm_values.min()) / n_samples

    cut_points_indexes = []
    th = norm_values.min()
    sampled_sorted_idx = []
    i = 0
    for idx in arg_s_values:
        if norm_values[idx] >= th:
            cut_points_indexes.append(idx)
            th = th + th_step

            sampled_sorted_idx.append(i)
        i = i + 1

    cut_points_indexes.append(arg_s_values[-1])  # Add last point
    sampled_sorted_idx.append(arg_s_values.__len__() - 1)

    if verbose:
        plt.plot(norm_values[arg_s_values], marker=".")
        plt.plot(sampled_sorted_idx, norm_values[cut_points_indexes], 'r.')
        plt.ylabel("Norm - " + sample_col)
        plt.xlabel("Samples")
        plt.show()

    if verbose:
        plt.plot(values, marker='.', linewidth=0)
        plt.plot(cut_points_indexes, values[cut_points_indexes], 'r.')
        plt.ylabel(sample_col)
        plt.xlabel("Samples")
        plt.show()

    sampled_data = df.iloc[cut_points_indexes, :]

    return sampled_data


def sample_by_data_2(x, samples_key, sample_col, verbose=False):
    k = x["data_id"].values[0]
    print(k) if verbose else None
    return sample_by_value_dist(x, samples_key[k], sample_col, verbose)


def custom_train_test_split_2(df, samples_key, sample_col, job_filter=".*", verbose=False):
    df = df.reset_index().loc[df.reset_index()["plan_id"].str.contains(job_filter), :]
    print("Len before:", df.__len__())
    df_train = df.groupby("data_id", group_keys=False).apply(
        lambda x: sample_by_data_2(x, samples_key, sample_col, verbose))
    print("Len train:", df_train.__len__())
    df_test = df[~df.index.isin(df_train.index)]
    print("Len test:", df_test.__len__())

    return df_train, df_test


######################
## Outliers removal ##
######################

def remove_outliers(df, outlier_col, b=0.01, verbose=False):
    asd = []
    for ds in sorted(df["data_size"].unique()):
        ds_df = df.loc[df["data_size"] == ds, :]
        len_before = ds_df.__len__()
        q = b
        ds_df = ds_df[ds_df[outlier_col] > np.quantile(ds_df[outlier_col], q)]

        q = 1.0 - b
        ds_df = ds_df[ds_df[outlier_col] < np.quantile(ds_df[outlier_col], q)]

        print(f"Removing {len_before - ds_df.__len__()} outliers from '{ds}'")

        asd.append(ds_df)

    df_no_out = pd.concat(asd)

    display(df[~df.index.isin(df_no_out.index)]) if verbose else None

    return df_no_out



###############################
## Data cardinality features ##
###############################
from generator_labeler.DatasetMetadata.TableMetaData import data_cardinality


def preprocess_jobs_data_info(path):
    df = pd.read_json(path)

    tmp_df = df.set_index("JobId")

    tmp_df = tmp_df["operatorsInfo"] \
        .apply(pd.Series) \
        .unstack() \
        .reset_index(level=0, drop=True) \
        .apply(pd.Series)

    tmp_df = tmp_df.loc[tmp_df["id"].notnull(), :].drop(0, axis=1)

    return tmp_df


def fill_cardinality(df, jid, data_size="1GB"):
    oc_queue = []

    tmp_df = df.loc[jid, :].copy()
    outCards = []
    for idx in range(tmp_df.__len__()):  # .sort_values("id")
        row = tmp_df.iloc[idx, :]
        # print(row)
        # print(idx, "Queue:", oc_queue)

        if row["pact"] == "Data Source":
            # new branch
            # TODO load data cardinality from the correct metadata
            oc = data_cardinality[data_size][row["tables"][0]] * row["selectivity"]
            oc_queue.append(oc)

        elif row["pact"] == "Data Sink":
            oc = oc_queue[-1]  # 0

        elif row["pact"] != "Join":
            if np.isnan(row["outCardinality"]):
                oc = oc_queue[-1] * row["selectivity"] if not np.isnan(row["selectivity"]) else 1.0
                oc_queue[-1] = oc
            else:
                oc = row["outCardinality"]
                oc_queue[-1] = oc

        elif row["pact"] == "Join":
            oc = max(oc_queue.pop(0), oc_queue.pop(0))
            oc_queue.append(oc)


        else:
            raise Exception("Error")

        outCards.append(oc)

    tmp_df["outCardinality"] = outCards
    return tmp_df


def fill_jobs_cardinality(df, data_size="1GB"):
    filled_jobs = []
    for jid in df.index.unique():
        j = fill_cardinality(df, jid, data_size=data_size)
        filled_jobs.append(j)

    filled_jobs_df = pd.concat(filled_jobs)
    filled_jobs_df["data_id"] = data_size
    filled_jobs_df = filled_jobs_df.reset_index().rename(columns={"JobId": "plan_id"})
    filled_jobs_df = filled_jobs_df.set_index(["plan_id", "data_id"])

    return filled_jobs_df


def compute_jobs_cardinality_features(jobs_card_df, percentiles=[0.1, 0.5, 0.9]):

    # outCardinality features
    jobs_card_features_df = jobs_card_df.groupby(level=[0, 1])["outCardinality"].describe(percentiles=percentiles)
    jobs_card_features_df["kurtosis"] = jobs_card_df.groupby(level=[0, 1])["outCardinality"].apply(lambda x: kurtosis(x))
    jobs_card_features_df["skew"] = jobs_card_df.groupby(level=[0, 1])["outCardinality"].apply(lambda x: skew(x))

    jobs_card_features_df.columns = ["outCardinality_" + c for c in jobs_card_features_df.columns]

    # Complexity features
    complexity_features_df = jobs_card_df[jobs_card_df["pact"] == "Map"]["outCardinality"] * (10 ** jobs_card_df[jobs_card_df["pact"] == "Map"]["complexity"]) # TODO 10 is a fixed parameter to be sostituted with real number of features
    complexity_features_df = complexity_features_df.groupby(level=[0, 1]).describe()[["mean", "std", "min", "max"]]
    complexity_features_df = complexity_features_df.fillna(0.0)
    complexity_features_df.columns = ["complexity_" + c for c in complexity_features_df.columns]

    j_features = pd.merge(jobs_card_features_df, complexity_features_df, left_index=True, right_index=True)


    # Selectivity features
    selectivity_features_df = jobs_card_df.groupby(level=[0, 1])["selectivity"].describe()[["mean", "min", "max"]]
    selectivity_features_df = selectivity_features_df.fillna(-1.0)
    selectivity_features_df.columns = ["selectivity_" + c for c in selectivity_features_df.columns]

    j_features = pd.merge(j_features, selectivity_features_df, left_index=True, right_index=True)

    # Source features
    source_card_sum_df = jobs_card_df[jobs_card_df["pact"] == "Data Source"].groupby(level=[0, 1])["outCardinality"].sum()
    source_card_sum_df = source_card_sum_df.to_frame().rename(columns={"outCardinality": "sourceCardinalitySum"})

    source_card_prod_df = jobs_card_df[jobs_card_df["pact"] == "Data Source"].groupby(level=[0, 1])["outCardinality"].prod()
    source_card_prod_df = source_card_prod_df.to_frame().rename(columns={"outCardinality": "sourceCardinalityProd"})

    source_out_card_df = jobs_card_df[jobs_card_df["pact"] == "Data Source"].groupby(level=[0, 1])["outCardinality"].describe()[["mean", "std", "min", "max"]]
    source_out_card_df = source_out_card_df.fillna(0.0)
    source_out_card_df.columns = ["sourceOutCard_" + c for c in source_out_card_df.columns]

    j_features = pd.merge(j_features, source_card_sum_df, left_index=True, right_index=True)
    j_features = pd.merge(j_features, source_card_prod_df, left_index=True, right_index=True)
    j_features = pd.merge(j_features, source_out_card_df, left_index=True, right_index=True)

    # Join features
    if jobs_card_df[jobs_card_df["pact"] == "Join"].__len__() > 0:
        join_card_mean_df = jobs_card_df[jobs_card_df["pact"] == "Join"].groupby(level=[0, 1])["outCardinality"].describe()[["mean", "std", "min", "max"]]
        join_card_mean_df["sum"] = jobs_card_df[jobs_card_df["pact"] == "Join"].groupby(level=[0, 1])["outCardinality"].sum()
        join_card_mean_df["prod"] = jobs_card_df[jobs_card_df["pact"] == "Join"].groupby(level=[0, 1])[
            "outCardinality"].prod()
        # display(join_card_mean_df)
        join_card_mean_df = join_card_mean_df.fillna(0.0)
        join_card_mean_df.columns = ["joinOutCard_" + c for c in join_card_mean_df.columns]

        j_features = pd.merge(j_features, join_card_mean_df, left_index=True, right_index=True, how="left")

    # Group by features
    if jobs_card_df[jobs_card_df["pact"] == "Group by"].__len__() > 0:
        groupby_card_mean_df = jobs_card_df[jobs_card_df["pact"] == "Group by"].groupby(level=[0, 1])["outCardinality"].describe()[["mean", "std", "min", "max"]]
        # display(groupby_card_mean_df)
        groupby_card_mean_df = groupby_card_mean_df.fillna(0.0)
        groupby_card_mean_df.columns = ["groupbyOutCard_" + c for c in groupby_card_mean_df.columns]

        j_features = pd.merge(j_features, groupby_card_mean_df, left_index=True, right_index=True, how="left")

    # Filter features
    filter_out_card_features_df = jobs_card_df[jobs_card_df["pact"] == "Filter"].groupby(level=[0, 1])["outCardinality"].describe()[["mean", "std", "min", "max"]]
    filter_out_card_features_df = filter_out_card_features_df.fillna(0.0)
    filter_out_card_features_df.columns = ["filterOutCard_" + c for c in filter_out_card_features_df.columns]

    j_features = pd.merge(j_features, filter_out_card_features_df, left_index=True, right_index=True, how="left")

    # Table features
    table_df = jobs_card_df.groupby(level=[0,1])["tables"] \
        .apply(list) \
        .apply(lambda values: list(dict.fromkeys([v for vv in values for v in vv]))).to_frame()

    j_features = pd.merge(j_features, table_df, left_index=True, right_index=True, how="left")

    j_features = j_features.fillna(0.0)
    return j_features


def get_original_out_cardinality(jobs_task_manager_details_path):
    task_manager_parser = TaskDetailsParser(jobs_task_manager_details_path)
    tasks_details = task_manager_parser.get_job_details()
    original_data_plan_features = tasks_details.copy()
    original_data_plan_features = original_data_plan_features[
        ["plan_id", "data_id", "operator", "write-records", "code_line"]] \
        .rename(mapper={"operator": "pact", "write-records": "outCardinality", "code_line": "id"}, axis=1) \
        .sort_index()

    #original_data_plan_features["selectivity"] = -1
    #original_data_plan_features["complexity"] = -1

    return original_data_plan_features.set_index(["plan_id", "data_id"])


def compute_original_cardinality_plan_features(jobs_task_manager_details_path, generatedJobsInfo_path):
    original_data_plan_features = get_original_out_cardinality(jobs_task_manager_details_path).reset_index()

    # Add table info
    jobs_data_info = preprocess_jobs_data_info(generatedJobsInfo_path)

    table_df = jobs_data_info.groupby(level=0)["tables"] \
        .apply(list) \
        .apply(lambda values: list(dict.fromkeys([v for vv in values for v in vv]))).to_frame()

    original_data_plan_features = pd.merge(original_data_plan_features,
                                           table_df.reset_index().rename({"JobId": "plan_id"}, axis=1), on="plan_id",
                                           how="left")

    complexity_df = jobs_data_info.groupby(level=0)["complexity"].mean()
    selectivity_df = jobs_data_info.groupby(level=0)["selectivity"].mean()

    original_data_plan_features = pd.merge(original_data_plan_features,
                                           complexity_df.reset_index().rename({"JobId": "plan_id"}, axis=1), on="plan_id",
                                           how="left")

    original_data_plan_features = pd.merge(original_data_plan_features,
                                           selectivity_df.reset_index().rename({"JobId": "plan_id"}, axis=1), on="plan_id",
                                           how="left")
    # Set map complexity
    # for did in original_data_plan_features["data_id"].unique():
    #     o_idx = original_data_plan_features.\
    #         sort_values(["plan_id", "data_id", "id"]).\
    #         loc[(original_data_plan_features["pact"] == "Map") &
    #         (original_data_plan_features["data_id"] == did),
    #             "complexity"].index
    #     f_idx = jobs_data_info[jobs_data_info["pact"] == "Map"].\
    #                             reset_index().\
    #                             sort_values(["JobId", "id"]).index

    #    original_data_plan_features.loc[o_idx, "complexity"] = jobs_data_info.reset_index().loc[f_idx, "complexity"]

    original_data_plan_features = original_data_plan_features.set_index(["plan_id", "data_id"])

    # Compute features
    cardinality_plan_features = compute_jobs_cardinality_features(original_data_plan_features)

    # Compute table features
    table_features = get_tables_features(cardinality_plan_features.tables, data_cardinality=data_cardinality,
                                         factors=-1, verbose=False)

    cardinality_plan_features = pd.merge(cardinality_plan_features, table_features, left_index=True, right_index=True)

    return cardinality_plan_features


def get_estimated_out_cardinality(generated_job_info_path, data_sizes=["1GB", "5GB", "10GB", "50GB"]):
    jobs_data_info = preprocess_jobs_data_info(generated_job_info_path)

    data_plan_features = []
    for d_id in data_sizes:
        data_f = fill_jobs_cardinality(jobs_data_info, data_size=d_id)
        data_plan_features.append(data_f)

    data_plan_features = pd.concat(data_plan_features)
    return data_plan_features


def compute_cardinality_plan_features(generated_job_info_path, data_sizes=["1GB", "5GB", "10GB", "50GB"]):

    data_plan_features = get_estimated_out_cardinality(generated_job_info_path, data_sizes)

    # print(data_plan_features)

    cardinality_plan_features = compute_jobs_cardinality_features(data_plan_features)

    # Compute table features
    table_features = get_tables_features(cardinality_plan_features.tables, data_cardinality=data_cardinality, factors=-1, verbose=False)

    cardinality_plan_features = pd.merge(cardinality_plan_features, table_features, left_index=True, right_index=True)

    return cardinality_plan_features


#############################
## Execution plan features ##
#############################

def parse_value(v):
    try:
        pv = float(v)
    except:
        try:
            if v[-1 ]=="M":
                pv = float(v[:-1])
            elif v[-1 ]=="G":
                pv = float(v[:-1]) * 1000.0
            else:
                pv = None
        except:
            raise Exception()

    return pv

def get_costs(g):
    g_costs = []
    for n in g.nodes:
        costs = g.nodes[n]["costs"]
        costs_dict = {c["name"] :parse_value(c["value"]) for c in costs}
        g_costs.append(costs_dict)


    g_costs_df = pd.DataFrame(g_costs)
    # print(g_costs_df.mean().to_dict())
    # print(g_costs_df.median())
    # print(g_costs_df.min())
    # print(g_costs_df.max())
    return g_costs_df.mean().to_dict()


def average_parallelism(g):
    ps = [float(g.nodes[n]["parallelism"]) for n in g.nodes]
    return sum(ps) / len(ps)


def compute_plan_features(plan_graph, include_costs=False, verbose=False):
    plan_data_ids = plan_graph.graph["plan_id"].split("_")
    feature_dict = {
        "plan_id": plan_data_ids[0],
        "data_id": plan_data_ids[-1],
        "data_size": float(re.sub(r'GB.*', '', plan_data_ids[-1]))
    }

    try:
        # feature_dict["dataPath"] = plan_graph.graph["dataPath"]
        feature_dict["netRunTime"] = plan_graph.graph["netRunTime"]
    except Exception as ex:
        print(ex)

    reach_node_plan = EPAnalyzer.get_plan_tables_from_plans([plan_graph]).set_index("plan_id")
    display(reach_node_plan) if verbose else ""

    pacts_count = reach_node_plan.groupby("g_pact").size().to_dict()
    display(pacts_count) if verbose else ""
    feature_dict.update(pacts_count)

    feature_dict["n_nodes"] = plan_graph.number_of_nodes()
    feature_dict["n_edges"] = plan_graph.number_of_edges()
    feature_dict["longest_path_len"] = nx.algorithms.dag.dag_longest_path_length(plan_graph)
    feature_dict["avg_parallelism"] = average_parallelism(plan_graph)

    if include_costs:
        feature_dict.update(get_costs(plan_graph))

    print(feature_dict) if verbose else ""

    return feature_dict


def show_feature_corr(graphs_plan_features):
    features_corr = graphs_plan_features.corr().fillna(0)
    # display(features_corr)

    fig, ax = plt.subplots(figsize=(12, 12))
    sns.heatmap(features_corr, ax=ax)
    return ax


def compute_plan_features_from_graphs(plan_graphs):

    plans_feature_df = pd.DataFrame([compute_plan_features(pg, include_costs=True) for pg in plan_graphs]).fillna(
        0)  # .set_index("plan_id")
    plans_feature_df["plan_id"] = plans_feature_df["plan_id"].apply(lambda x: x.replace("$", ""))
    plans_feature_df = plans_feature_df.set_index(["plan_id", "data_id"])

    return plans_feature_df
