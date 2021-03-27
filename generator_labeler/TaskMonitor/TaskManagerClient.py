import requests
import re
import json
import os
import pandas as pd

###
# 127.0.0.1:8081/jobs/
# 127.0.0.1:8081/jobs/<job_id>
# 127.0.0.1:8081/jobs/<job_id>/vertices/<vertex_id>
###


class TaskDetailsParser:

    def __init__(self, TASK_DETAILS_PATHS):
        self.TASK_DETAILS_PATHS = TASK_DETAILS_PATHS

    @staticmethod
    def parse_job_vertices(job_vertices, job_plans, fill_all_op=True):
        """
        {
                "id": "2c2ab9c040d0821ef25bcd17c62a0b10",
                "name": "CHAIN DataSource (at TPC_H_Jobs.Q3_Job$.run(Q3_Job.scala:66) (org.apache.flink.api.java.io.TupleCsvInputFormat)) -> Filter (Filter at TPC_H_Jobs.Q3_Job$.run(Q3_Job.scala:69))",
                "parallelism": 1,
                "status": "FINISHED",
                "start-time": 1591015833278,
                "end-time": 1591015836246,
                "duration": 2968,
                "tasks": {
                    "CREATED": 0,
                    "SCHEDULED": 0,
                    "RUNNING": 0,
                    "FINISHED": 1,
                    "CANCELED": 0,
                    "RECONCILING": 0,
                    "CANCELING": 0,
                    "FAILED": 0,
                    "DEPLOYING": 0
                },
                "metrics": {
                    "read-bytes": 0,
                    "read-bytes-complete": true,
                    "write-bytes": 4784128,
                    "write-bytes-complete": true,
                    "read-records": 0,
                    "read-records-complete": true,
                    "write-records": 30142,
                    "write-records-complete": true
                }
            }
            """
        vertex_details = []

        for idx in range(len(job_vertices)):
            v_id = job_vertices[idx]["id"]

            # Look for plan node
            node = [j_n for j_n in job_plans["nodes"] if j_n["id"] == v_id]
            if len(node) != 1:
                raise Exception()
            operators = node[0]["operator"].split(" -> ")

            # print(operators)

            # init vertex_operators_details
            vertex_operators_details = [{
                "operator": op,
                "read-bytes": -1,
                "read-records": -1,
                "write-bytes": -1,
                "write-records": -1
            } for op in operators]

            operators_desc = job_vertices[idx]["name"].split("->")
            # print(operators_desc)
            for jdx in range(len(vertex_operators_details)):
                m = re.search(r'(?<=at )(.*?\))', operators_desc[jdx])
                vertex_operators_details[jdx]["code_det"] = m.group(0) if m else ""

                m = re.search(r'(?<=\:)(.*?)(?=\))', vertex_operators_details[jdx]["code_det"])
                vertex_operators_details[jdx]["code_line"] = int(m.group(0)) if m else 1000000

                if (fill_all_op is True):
                    vertex_operators_details[jdx]["read-bytes"] = job_vertices[idx]["metrics"]["read-bytes"]
                    vertex_operators_details[jdx]["read-records"] = job_vertices[idx]["metrics"]["read-records"]
                    vertex_operators_details[jdx]["write-bytes"] = job_vertices[idx]["metrics"]["write-bytes"]
                    vertex_operators_details[jdx]["write-records"] = job_vertices[idx]["metrics"]["write-records"]

                if (jdx + 1 == len(vertex_operators_details)):
                    vertex_operators_details[jdx]["read-bytes"] = job_vertices[idx]["metrics"]["read-bytes"]
                    vertex_operators_details[jdx]["read-records"] = job_vertices[idx]["metrics"]["read-records"]
                    vertex_operators_details[jdx]["write-bytes"] = job_vertices[idx]["metrics"]["write-bytes"]
                    vertex_operators_details[jdx]["write-records"] = job_vertices[idx]["metrics"]["write-records"]

                    if vertex_operators_details[jdx]["operator"] == "Data Sink":
                        vertex_operators_details[jdx]["write-bytes"] = vertex_operators_details[jdx]["read-bytes"]
                        vertex_operators_details[jdx]["write-records"] = vertex_operators_details[jdx]["read-records"]

                if "Group" in vertex_operators_details[jdx]["operator"]:
                    vertex_operators_details[jdx]["operator"] = "Group by"

                # print(vertex_operators_details[jdx])

            min_code_line = min([vod["code_line"] for vod in vertex_operators_details])

            for vod in vertex_operators_details:
                vod["chain_id"] = min_code_line

            vertex_details = vertex_details + vertex_operators_details

        return sorted(vertex_details, key=lambda x: x["chain_id"])

    @staticmethod
    def save_vertex_details(vertex_details, dest_file):
        with open(dest_file, "w") as fp:
            json.dump(vertex_details, fp)

    @staticmethod
    def parse_job_details(job_details, persist_vertex_details=False, vertex_details_dest=None):
        vertex_details = TaskDetailsParser.parse_job_vertices(job_details["vertices"], job_details["plan"])

        if persist_vertex_details and vertex_details_dest is not None:
            TaskDetailsParser.save_vertex_details(vertex_details, vertex_details_dest)

        return vertex_details

    def load_job_details(self):
        tasks_dets = []
        for TS_DETAILS in self.TASK_DETAILS_PATHS:
            data_size_id = os.path.basename(TS_DETAILS)

            task_files = os.listdir(TS_DETAILS)
            task_files = [f for f in task_files if not f.startswith(".")]

            for f in task_files:
                td_id = f.split("-")[0]
                with open(os.path.join(TS_DETAILS, f)) as fs:
                    t_d = json.load(fs)
                    tasks_dets.append((td_id, data_size_id, t_d))
        return tasks_dets

    def get_job_details(self, to_pandas=True):
        job_details = self.load_job_details()

        tasks_vertexes_dets = []
        for t_id, data_id, v in job_details:
            vertexes_details = TaskDetailsParser.parse_job_details(v)
            tasks_vertexes_dets.append((t_id, data_id, vertexes_details))

        if to_pandas:
            dfs = []
            for t_id, data_id, v in tasks_vertexes_dets:
                df = pd.DataFrame(v)
                df["plan_id"] = t_id
                df["data_id"] = data_id
                dfs.append(df)

            return pd.concat(dfs, ignore_index=True)

        else:
            return tasks_vertexes_dets


def main():
    pd.set_option("display.max_columns", 10)
    pd.set_option('display.width', 1000)

    task_manager_parser = TaskDetailsParser(["/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/Experiment5/cloud-7_generated_jobs_task_manager_details/1GB"])
    vartexes_details = task_manager_parser.get_job_details()


    print(vartexes_details)


if __name__ == '__main__':
    main()
