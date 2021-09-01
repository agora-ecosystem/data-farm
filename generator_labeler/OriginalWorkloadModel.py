import sklearn
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.metrics import r2_score, mean_absolute_error

from CONFIG import CONFIG


class OriginalWorkloadModel:

    def __init__(self, original_dataset, generated_dataset):
        # self.original_dataset = original_dataset
        # self.generated_dataset = generated_dataset

        self.labels_original_dataset = np.array(original_dataset['Log_netRunTime'])

        self.original_dataset = original_dataset.drop(
            columns=[col for col in original_dataset if col not in CONFIG.FEATURE_COLS])
        self.generated_dataset = generated_dataset.drop(
            columns=[col for col in generated_dataset if col not in CONFIG.FEATURE_COLS])

        col_diff = self.generated_dataset.columns.difference(self.original_dataset.columns)
        self.generated_dataset = self.generated_dataset.drop(col_diff, axis=1)

    def train_model(self, input_ids, labels):
        labels_generated_dataset = labels
        training_features = self.generated_dataset.iloc[input_ids]

        clf = xgb.XGBRFRegressor(objective="reg:squarederror", random_state=42)

        small_model = clf.fit(training_features, labels_generated_dataset)

        complete_labels = small_model.predict(self.generated_dataset)

        clf = xgb.XGBRFRegressor(objective="reg:squarederror", random_state=42)

        complete_classifier = clf.fit(self.generated_dataset, complete_labels)

        return self.original_jobs_prediction(complete_classifier, "Sampled Ids model")

    def original_jobs_prediction(self, clf, name):
        print(self.original_dataset)
        test_X = self.original_dataset
        test_y = self.labels_original_dataset
        pred_y = clf.predict(test_X)
        log_r2 = r2_score(test_y, pred_y)
        mae = mean_absolute_error(test_y, pred_y)
        print(name)
        print("Log r2", log_r2)
        print("MAE", mae)
        return [log_r2, mae]
