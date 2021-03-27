import pandas as pd
import numpy as np
from generator_labeler.Analysis.models import get_X_y
import matplotlib.pyplot as plt
from IPython.display import display

import seaborn as sns

sns.set_context("talk")
sns.set_style("whitegrid")


class RandomSampler:
    def __init__(self, k_sample, feature_cols, label_col, seed=42):
        self.k_sample = k_sample
        self.feature_cols = feature_cols
        self.label_col = label_col

        self.seed = seed

        self.samples = None
        self.sample_ids = None

    def fit(self, df, verbose=False):

        self.samples = df.reset_index().sample(self.k_sample, random_state=self.seed)
        self.sample_ids = self.samples.index.values

        return self

    def transform(self, df):
        return df.iloc[self.sample_ids, :]

    def fit_transform(self, df, verbose=False):
        return self.fit(df, verbose=verbose).transform(df)


class UniformSampler:

    def __init__(self, k_sample, sample_col, step=0.01, max_iter=1000):
        self.k_sample = k_sample
        self.sample_col = sample_col
        self.step = step
        self.max_iter = max_iter

        self.samples = None
        self.sample_ids = None

    def sample_by_value_dist(self, df, verbose=True):

        n_samples = self.k_sample #- 2

        if n_samples >= df.__len__():
            print("WARNING - asking to sample more points than in data!")
            return df

        values = df[self.sample_col].values
        values[values == np.NINF] = np.nan
        arg_s_values = values.argsort()

        if verbose:
            plt.plot(values, marker='.', linewidth=0, alpha=0.2)
            plt.ylabel(self.sample_col)
            plt.xlabel("Samples")
            # plt.show()

        norm_values = values  # / max(values)
        # norm_values = np.gradient(norm_values[arg_s_values])

        th_step = (np.nanmax(norm_values) - np.nanmin(norm_values)) / n_samples
        len_samples = 0
        it = 0
        cut_points_indexes = []
        while (len_samples < n_samples) or (it >= self.max_iter):
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
            th_step -= self.step * th_step
            len_samples = cut_points_indexes.__len__()

        th_step -= self.step * th_step

        if verbose:
            plt.plot(norm_values[arg_s_values], marker=".", alpha=0.2)
            for idx in range(len_samples-1):
                plt.hlines(norm_values.min() + idx*th_step, xmin=-50, xmax=norm_values.__len__()+50, linestyles="dashed", linewidth=0.5, color="red")
            #plt.plot(sampled_sorted_idx, norm_values[cut_points_indexes], 'r+')
            plt.ylabel(self.sample_col)
            plt.xlabel("Samples")
            # plt.show()

        if verbose:
            plt.plot(norm_values[arg_s_values], marker=".", alpha=0.2)
            plt.plot(sampled_sorted_idx, norm_values[cut_points_indexes], 'r+')
            plt.ylabel(self.sample_col)
            plt.xlabel("Samples")
            # plt.show()

        if verbose:
            plt.plot(values, marker='.', linewidth=0, alpha=0.2)
            plt.plot(cut_points_indexes, values[cut_points_indexes], 'r+')
            plt.ylabel(self.sample_col)
            plt.xlabel("Samples")
            # plt.show()

        if cut_points_indexes.__len__() == 0:
            raise Exception("Can not find any cut point.")
        self.sample_ids = cut_points_indexes
        it += 1
        sampled_data = df.iloc[cut_points_indexes, :]

        return sampled_data

    def show_sampled_jobs(self, df):
        n_samples = self.k_sample  # - 2

        if n_samples >= df.__len__():
            print("WARNING - asking to sample more points than in data!")
            return df

        values = df[self.sample_col].values
        values[values == np.NINF] = np.nan
        arg_s_values = values.argsort()

        norm_values = values  # / max(values)
        # norm_values = np.gradient(norm_values[arg_s_values])

        th_step = (np.nanmax(norm_values) - np.nanmin(norm_values)) / n_samples
        len_samples = 0
        it = 0
        cut_points_indexes = []
        ths = []
        while (len_samples < n_samples) or (it >= self.max_iter):
            cut_points_indexes = []
            th = norm_values.min()
            sampled_sorted_idx = []
            i = 0
            for idx in arg_s_values:
                if norm_values[idx] >= th:
                    ths.append(th)
                    cut_points_indexes.append(idx)
                    th = th + th_step

                    sampled_sorted_idx.append(i)
                i = i + 1

            cut_points_indexes.append(arg_s_values[-1])  # Add last point
            sampled_sorted_idx.append(arg_s_values.__len__() - 1)
            th_step -= self.step * th_step
            len_samples = cut_points_indexes.__len__()

        th_step -= self.step * th_step

        fig, ax = plt.subplots(figsize=(5, 5))

        sns.scatterplot(x=np.arange(len(arg_s_values)), y=norm_values[arg_s_values], ax=ax, alpha=0.2, marker="o", legend=False, palette="tab10",
                        label="Non-executed jobs", linewidth=0)
        sns.scatterplot(x=sampled_sorted_idx, y=norm_values[cut_points_indexes], ax=ax, marker="P", s=100, legend=False,
                        color="black", label="Selected jobs")
        for idx in range(len(ths)):
            ax.hlines(ths[idx], xmin=-50, xmax=norm_values.__len__() + 50,
                       linestyles="dashed", linewidth=1, color="gray", label="Uniform thresholds" if idx == 0 else None)

        ax.set_ylabel(self.sample_col)
        ax.set_xlabel("Non-executed jobs")
        return ax

    def fit(self, df, verbose=False):
        self.samples = self.sample_by_value_dist(df, verbose=verbose)
        return self

    def transform(self, df):
        return self.samples

    def fit_transform(self, df, verbose=False):
        return self.fit(df, verbose=verbose).transform(df)


class UnsupervisedSampler:

    def __init__(self, k_sample, feature_cols, label_col, k_comp=6):
        self.k_sample = k_sample
        self.k_comp = k_comp
        self.feature_cols = feature_cols
        # self.label_col = label_col

        self.pca_model = None
        self.cluster_model = None
        self.y_pred = None
        self.median_centroids = None
        self.median_centroid_ids = None
        self.score = None

    def get_components(self, df, normalize_columns=False, verbose=False):
        from sklearn.decomposition import PCA

        X = df.loc[:, self.feature_cols].values

        if normalize_columns:
            X = (X - X.mean(axis=0)) / X.std(axis=0)

        pca_model = PCA(self.k_comp)
        pca_model.fit(X)
        X_reduced = pca_model.transform(X)

        if verbose:
            print("Explained variance %", np.cumsum(pca_model.explained_variance_ratio_))

            display(
                pd.DataFrame(pca_model.components_.T, index=self.feature_cols).abs().style.background_gradient()
            )

            fig, ax = plt.subplots(figsize=(10, 6))
            sns.scatterplot(X_reduced[:, 0], X_reduced[:, 1], ax=ax)
            ax.set_xlabel("Component 1")
            ax.set_ylabel("Component 2")
            # plt.show()
            # plt.close()

        return pca_model, X_reduced

    def clustering(self, X, verbose=False):

        from sklearn.cluster import KMeans
        from sklearn.cluster import AgglomerativeClustering

        #cluster_model = KMeans(self.k_sample)
        cluster_model = AgglomerativeClustering(self.k_sample)

        y_pred = cluster_model.fit_predict(X)

        if verbose:
            # plot_dendrogram(model, truncate_mode='level', p=3)

            fig, ax = plt.subplots(figsize=(10, 6))
            sns.scatterplot(x=X[:, 0], y=X[:, 1], hue=y_pred, ax=ax, legend=False) # , palette="tab20"
            ax.set_xlabel("Component 1")
            ax.set_ylabel("Component 2")
            # plt.show()
            # plt.close()

        return cluster_model, y_pred

    def get_score(self, X, y):
        from sklearn.metrics import silhouette_samples
        return silhouette_samples(X, y)

    def get_median_centroids(self, X, y, verbose=False):
        median_centroids = []
        for c in np.unique(y):
            X_c = X[y == c]
            X_c_mean = X_c.mean(axis=0)

            X_c_dist = np.linalg.norm(X_c - X_c_mean, axis=1)
            # print(X_c_dist.shape)

            x_median_centroid = X_c[np.argmin(X_c_dist)]
            median_centroids.append(x_median_centroid)

        median_centroids = np.array(median_centroids)

        if verbose:
            fig, ax = plt.subplots(figsize=(10, 6))
            sns.scatterplot(x=X[:, 0], y=X[:, 1], hue=y, ax=ax, legend=False, alpha=0.2)
            sns.scatterplot(x=median_centroids[:, 0], y=median_centroids[:, 1], ax=ax, marker="+", s=800, legend=False, color="red") # , hue=np.unique(y) ,palette="tab20"
            ax.set_xlabel("Component 1")
            ax.set_ylabel("Component 2")
            # plt.show()
            # plt.close()

        return median_centroids, np.unique(y)

    def show_sampled_jobs(self, X, y):
        median_centroids = []
        for c in np.unique(y):
            X_c = X[y == c]
            X_c_mean = X_c.mean(axis=0)

            X_c_dist = np.linalg.norm(X_c - X_c_mean, axis=1)
            # print(X_c_dist.shape)

            x_median_centroid = X_c[np.argmin(X_c_dist)]
            median_centroids.append(x_median_centroid)

        median_centroids = np.array(median_centroids)

        fig, ax = plt.subplots(figsize=(5, 5))
        sns.scatterplot(x=X[:, 0], y=X[:, 1], hue=y, ax=ax, alpha=0.8, legend=False, palette="tab10", label="Non-executed jobs (colors are clusters)")
        sns.scatterplot(x=median_centroids[:, 0], y=median_centroids[:, 1], ax=ax, marker="P", s=100, legend=False,
                        color="black", label="Selected jobs")  # , hue=np.unique(y) ,palette="tab20"
        ax.set_xlabel("Component 1")
        ax.set_ylabel("Component 2")
        return ax

    def get_centroids_ids(self, X, centroids):
        centroids_ids = []
        for idx in range(centroids.shape[0]):
            c_id = (np.where((X == centroids[idx, :]).all(axis=1))[0][0])
            centroids_ids.append(c_id)
        return centroids_ids

    def fit(self, df, verbose=False):
        self.pca_model, X_reduced = self.get_components(df, verbose=verbose)
        self.cluster_model, self.y_pred = self.clustering(X_reduced, verbose=verbose)

        self.median_centroids, _ = self.get_median_centroids(X_reduced, self.y_pred, verbose=verbose)

        self.median_centroid_ids = self.get_centroids_ids(X_reduced, self.median_centroids)

        self.score = self.get_score(X_reduced, self.y_pred)

        return self

    def transform(self, df):
        return df.copy().iloc[self.median_centroid_ids, :]

    def fit_transform(self, df, verbose=False):
        return self.fit(df, verbose=verbose).transform(df)


class UniformAgglomerativeSampler:

    def __init__(self, k_sample, feature_cols, label_col, sample_col, step=0.01, max_iter=1000, k_comp=6):
        self.k_sample = int(k_sample/2)
        self.k_comp = k_comp
        self.feature_cols = feature_cols
        self.label_col = label_col

        self.sample_col = sample_col
        self.step = step
        self.max_iter = max_iter

        self.unsupervised_model = None
        self.uniform_model = None
        self.sample_ids = None

    def fit(self, df, verbose=False):
        self.unsupervised_model = UnsupervisedSampler(self.k_sample, self.feature_cols, self.label_col)
        self.unsupervised_model.fit(df, verbose)
        self.uniform_model = UniformSampler(self.k_sample, self.sample_col)  # TODO fix bug of k_sample in uniform sampler with -> UniformSampler(self.k_sample-1, self.sample_col)
        self.uniform_model.fit(df, verbose)

        self.sample_ids = np.union1d(self.unsupervised_model.median_centroid_ids, self.uniform_model.sample_ids)

        return self

    def transform(self, df):
        return df.copy().iloc[self.sample_ids, :]

    def fit_transform(self, df, verbose=False):
        return self.fit(df, verbose=verbose).transform(df)