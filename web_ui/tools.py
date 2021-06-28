from io import BytesIO

from flask import send_file
from PIL import Image
import numpy as np

def _serve_buffer(buf):
    buf.seek(0)
    return send_file(buf, mimetype='image/png')

def _serve_image(path):
    pil_image = Image.open(path)
    return _serve_pil_image(pil_image)

def _serve_pil_image(pil_img):
    img_io = BytesIO()
    pil_img.save(img_io, 'png', quality=100)
    img_io.seek(0)
    return send_file(img_io, mimetype='image/png')


def get_iteration_details(results, iteration_to_show, n_train, n_test):
    it_results = results["iterations_results"][iteration_to_show]
    train_mask = np.arange(0, n_train)
    test_mask = np.arange(0, n_test)

    train_jobs = list(it_results["train_ids"]["plan_id"].values())[:n_train]
    print(train_jobs.__len__())
    train_jobs_uncertainty = np.zeros(len(train_jobs))[:n_train]
    train_real_jobs_labels = np.exp(it_results["train_labels"][:n_train])

    list_of_iteration_jobs = []
    for i in range(min(n_train, len(train_jobs))):
        list_of_iteration_jobs.append(
            {
                "job_id": train_jobs[i],
                "executed": True,
                "selected": True,
                "uncertainty": train_jobs_uncertainty[i],
                "uncertainty_low": 0.0,
                "uncertainty_high": 0.0,
                "execution_time": train_real_jobs_labels[i]
            }
        )
    test_jobs = np.array(list(it_results["test_ids"]["plan_id"].values()))[:n_test]
    # print(test_jobs.__len__())
    test_jobs_uncertainty_low = np.exp(it_results["uncertainty_low"])[:n_test]
    test_jobs_uncertainty_high = np.exp(it_results["uncertainty_high"])[:n_test]

    test_jobs_uncertainty = np.exp(it_results["uncertainty_interval"])[:n_test]
    test_real_jobs_labels = np.exp(it_results["test_labels"][:n_test])
    test_pred_jobs_labels = np.exp(it_results["pred_labels"][:n_test])

    for i in range(min(n_test, len(test_jobs))):
        list_of_iteration_jobs.append(
            {
                "job_id": test_jobs[i],
                "executed": False,
                "selected": False,
                "uncertainty": test_jobs_uncertainty[i],
                "uncertainty_low": test_jobs_uncertainty_low[i],
                "uncertainty_high": test_jobs_uncertainty_high[i],
                "execution_time": test_pred_jobs_labels[i]
            }
        )

    IQRs_RMSE = np.mean((np.exp(it_results["uncertainty_high"]) - np.exp(it_results["uncertainty_low"]))[:n_test])
    IQRs_std = np.std((np.exp(it_results["uncertainty_high"]) - np.exp(it_results["uncertainty_low"]))[:n_test])

    return list_of_iteration_jobs, IQRs_RMSE, IQRs_std