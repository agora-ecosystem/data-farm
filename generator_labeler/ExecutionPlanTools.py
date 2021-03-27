import os
import sys
import json
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout

import matplotlib.pyplot as plt

from generator_labeler.Generator.Node import NodeOp


def load_exec_plans(plan_folder):
    gen_plan_files = os.listdir(plan_folder)

    gen_plan_files = [f for f in gen_plan_files if not f.startswith(".")]

    exec_plans = []
    for ep_file in gen_plan_files:
        data_size_id = os.path.basename(plan_folder)
        ep_id = ep_file.replace(".json", "")

        with open(os.path.join(plan_folder, ep_file)) as f:
            ep_str = " ".join(f.readlines()).replace('"UTF-16LE"', r'\"UTF-16LE\"') # To fix issues with the json produced by flink

        ep_json = json.loads(ep_str)
        exec_plans.append((ep_id, data_size_id, ep_json))
    return exec_plans


def compute_flink_graph_from_exec_plan(exec_plan, G, include_cycles=True):

    for n in exec_plan["nodes"]:
        # check if there is an iteration
        if n["type"] == "bulk_iteration":
            G.add_node(n["id"], **n, color="purple")

            if include_cycles:
                for step_function in n["step_function"]:
                    # print(step_function["id"])
                    G.add_node(step_function["id"], **step_function, color="red")
                    if "predecessors" in step_function.keys():
                        pres = step_function["predecessors"]
                        for p in pres:
                            G.add_edge(p["id"], step_function["id"], **p, color="black")

                # add iteration edge
                G.add_edge(n["next_partial_solution"], n["partial_solution"], color="gray")

                # add return from iteration edge
                G.add_edge(n["next_partial_solution"], n["id"], color="purple")

        elif (n["pact"] == "Data Source") or (n["pact"] == "Data Sink"):
            G.add_node(n["id"], **n, color="green")
        else:
            G.add_node(n["id"], **n, color="lightblue")

        if "predecessors" in n.keys():
            pres = n["predecessors"]
            for p in pres:
                color = "orange" if p["ship_strategy"] == "Broadcast" else "black"
                G.add_edge(p["id"], n["id"], **p, color=color)

    return G


def compute_graph_from_plan(exec_plan, include_cycles=True):
    plan_id, data_size_id, ex_plan = exec_plan

    G = nx.DiGraph()

    G.graph["plan_id"] = f"{plan_id}_{data_size_id}"
    #print(G.graph["plan_id"])

    try:
        G.graph["dataPath"] = ex_plan["dataPath"]
        G.graph["execPlanOutPath"] = ex_plan["execPlanOutPath"]
        G.graph["execute"] = ex_plan["execute"]
        G.graph["netRunTime"] = ex_plan["netRunTime"]
    except Exception as ex:
        G.graph["dataPath"] = None
        G.graph["execPlanOutPath"] = None
        G.graph["execute"] = None
        G.graph["netRunTime"] = None

    G = compute_flink_graph_from_exec_plan(ex_plan["executionPlan"], G, include_cycles=include_cycles)

    return G


def compute_graphs_from_plans(exec_plans, include_cycles=True):
    exec_plans_graph = []
    for exec_plan in exec_plans:
        G = compute_graph_from_plan(exec_plan, include_cycles=include_cycles)
        exec_plans_graph.append(G)

    return exec_plans_graph


def show_exec_plan_graph(pg):
    fig, ax = plt.subplots(1,1, figsize=(8,5), dpi=110)
    nx.draw(pg, graphviz_layout(pg, prog='dot'), with_labels=True,
            labels = {n[0]:f"{n[0]}_{n[1]['pact']}" for n in pg.nodes.data()},
            node_color=[n[1]["color"] for n in pg.nodes.data()],
            edge_color=[e[2]["color"] for e in pg.edges.data()],
            ax=ax
            )
    ax.set_title(pg.graph["plan_id"])
    plt.show()
    plt.close()

def _show_generated_exec_plan_graph(pg):
    fig, ax = plt.subplots(1, 1, figsize=(8, 5), dpi=110)
    nx.draw(pg, graphviz_layout(pg, prog='dot'), with_labels=True,
            labels={n[0]: f"{n[0]}" for n in pg.nodes.data()},
            node_color=[NodeOp.get_operator_color(n[1]) for n in pg.nodes.data()],
            ax=ax
            )
    ax.set_title(pg.graph["plan_id"])
    return ax

def show_generated_exec_plan_graph(pg):
    ax = _show_generated_exec_plan_graph(pg)
    plt.show()
    plt.close()

def save_generated_exec_plan_graph(pg, dest_folder, save_graph_plot=True):
    data1 = nx.json_graph.node_link_data(pg)

    with open(os.path.join(dest_folder, f'{pg.graph["plan_id"]}.json'), "w") as fp:
        json.dump(data1, fp)
    print(data1)

    if save_graph_plot:
        ax = _show_generated_exec_plan_graph(pg)
        #ax.set_title(f'{pg.graph["plan_id"]}')
        plt.savefig(os.path.join(dest_folder, f'{pg.graph["plan_id"]}.pdf'))
        plt.close()