import pandas as pd
import numpy as np
from generator_labeler.Analysis.models import get_X_y
import matplotlib.pyplot as plt
from IPython.display import display

import seaborn as sns

sns.set_context("talk")
sns.set_style("whitegrid")


def options_list(title, query, options):

    print(title)
    for option in options:
        print(option)

    while True:
        try:
            val = input(query)
            print()
            value = list(map(int, val.strip().split(',')))
            return value
        except:
            print("Invalid input, please try again")


class UserSampler:

    def __init__(self, k_sample, feature_cols, label_col, seed=42):
        self.k_sample = k_sample
        self.feature_cols = feature_cols
        self.label_col = label_col

        print("Initiating user specified sampling")

    def fit(self, features_df, verbose):

        job_options = []
        job_names = features_df.index.values.tolist()
        print(f"Config file has indicated to start with {self.k_sample} jobs")
        for i in range(0, len(job_names)):
            job_options.append(f"{i}: Job \"{job_names[i]}\"")
        self.sample_ids = options_list("Which jobs would you like to run?", "Type numbers, separated by commas: ",
                                            job_options)

        return self
