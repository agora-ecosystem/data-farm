import os
import sys
import warnings
import pickle
from IPython.core.display import display
import numpy as np
import pandas as pd


from generator_labeler.paper_results import IMDB_config, TPCH_config
from generator_labeler.FeatureExtraction.PredictorFeatureExtraction import compute_cardinality_plan_features
from generator_labeler.JobExecutionSampler.unsupervised_sampler import UniformAgglomerativeSampler, RandomSampler
import BuildAndSubmit


def submit_jobs(init_jobs_to_run):
    exec_plans_path_already_computed = BuildAndSubmit.get_exec_plans_path()
    exec_plans_already_computed = {os.path.basename(ep).replace("$.json", "") for ep in
                                   exec_plans_path_already_computed}

    job_projects = BuildAndSubmit.get_job_projects()
    job_projects = sorted(job_projects, key=BuildAndSubmit.job_id_v)
    print(f"Found #{job_projects.__len__()} jobs:", job_projects)
    job_projects = [jp for jp in job_projects if (jp in init_jobs_to_run) and (jp not in exec_plans_already_computed)]

    print(job_projects)
    BuildAndSubmit.run_jobs(job_projects)
    return


def load_data_and_preprocess(config, load_original_cards=False):
    args_params = config.parse_args(sys.argv)

    # Load dataset
    if config.plan_data_features_path is not None:
        print("#####################################################")
        print("## WARNING! Loading pre-existing features dataset! ##")
        print("#####################################################")
        plan_data_features = pd.read_csv(config.plan_data_features_path).set_index(["plan_id", "data_id"])
    else:
        plan_data_features = compute_cardinality_plan_features(args_params["generatedJobsInfo"], data_sizes=config.data_sizes)
        plan_data_features = plan_data_features.sort_index()

        sourceCardinalitySum = plan_data_features["sourceCardinalitySum"].copy()
        sourceCardinalitySum[sourceCardinalitySum == 0] = 1 # Solves a bug in uniform sampler, because log of 0 is minus inf
        plan_data_features["Log_sourceCardinalitySum"] = np.log(sourceCardinalitySum)

    return plan_data_features


def run(config, random_sampling=False):
    # Load plan_data_features
    features_df = load_data_and_preprocess(config)

    # Persist features
    features_df.to_csv(os.path.join(config.dest_folder, "plan_data_features.csv"))

    if random_sampling:
        print("Random init sampling...")
        sample_model = RandomSampler(config.init_jobs, config.feature_cols, config.label_col, seed=42)
    else:
        sample_model = UniformAgglomerativeSampler(config.init_jobs, config.feature_cols, config.label_col, config.sample_col)

    sample_model.fit(features_df, verbose=True)
    # save init_job_sample_ids
    np.savetxt(os.path.join(config.dest_folder, "init_job_sample_ids.txt"), sample_model.sample_ids, fmt="%d")

    init_jobs_to_run = features_df.iloc[sample_model.sample_ids].index.get_level_values(0)

    # -> RUN Jobs
    submit_jobs(init_jobs_to_run)


    #Start Active-Learning here

    # -> Collect results
    # -> create model
    # -> predict labels
    # -> next iteration

    sys.exit(-1)

    train_data_df = sample_model.transform(features_df.copy())
    val_data_df = features_df.loc[~features_df.index.isin(train_data_df.index), :]
    #test_data_df = test_df.copy()

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

def get_cofing(exp_type):
    if exp_type == "IMDB":
        return IMDB_config
    if exp_type == "TPCH":
        return TPCH_config
    else:
        Exception(f"No experiment type '{exp_type}'")


def main():
    exp_type = "TPCH"  # "TPCH" # "IMDB"
    config = get_cofing(exp_type)
    random_sampling = False

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
    run(config, random_sampling=random_sampling)


if __name__ == '__main__':
    main()