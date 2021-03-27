import CONFIG
import os


def generate_abstract_plans():
    print(f"|--> Generating '{CONFIG.N_JOBS}' abstract plans in '{CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER}'")
    print()

    os.system(f'cd {CONFIG.ABSTRACT_PLAN_GENERATOR}; '
              f'python3 NewAbstractExecutionPlanAnalyzer.py {CONFIG.N_JOBS} {CONFIG.ORIG_EXEC_PLAN_FOLDER} {CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER}')


def generate_jobs():
    print(f"|--> Generating '{CONFIG.N_JOBS}' jobs in '{CONFIG.GENERATED_JOB_FOLDER}'")
    print()

    os.system(f'cd {CONFIG.JOB_GENERATOR}; '
              f'sbt "runMain Generator.JobGenerator {CONFIG.N_JOBS} {CONFIG.N_VERSIONS} {CONFIG.DATA_MANAGER} {CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER} {CONFIG.GENERATED_JOB_FOLDER} {CONFIG.JOB_SEED}"')


def create_project_folders():
    project_folders = [
        CONFIG.GENERATED_ABSTRACT_EXECUTION_PLAN_FOLDER,
        CONFIG.GENERATED_JOB_FOLDER,
        CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER,
        CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER+"1GB",
        CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER+"5GB",
        CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER+"10GB",
        CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER+"50GB",
        CONFIG.GENERATED_JOB_TASK_MANAGER_DETAILS,
        #CONFIG.GENERATED_JOB_EXEC_PLAN_FOLDER+"3GB",
        #CONFIG.GENERATED_JOB_TASK_MANAGER_DETAILS+"3GB"

    ]

    for pj_f in project_folders:
        if not os.path.exists(pj_f):
            print(f"|--> Creating project folder: {pj_f}")
            os.makedirs(pj_f)
        else:
            print(f"|--> Skip project folder: {pj_f}")


if __name__ == '__main__':

    print("|Init project")
    create_project_folders()

    print()
    print("|Abstract Plan Generation")
    generate_abstract_plans()

    print()
    print("|Job Generation")
    generate_jobs()