import json
import numpy as np
import pandas as pd
import os
import networkx as nx

import matplotlib.pyplot as plt
import seaborn as sns

from networkx.drawing.nx_agraph import graphviz_layout
from IPython.display import display
import itertools

from generator_labeler.Generator.AbstractPlan import AbstractPlanGenerator
from generator_labeler.Generator.Node import NodeOp

sns.set_context("talk")
sns.set_style("whitegrid")

exec_plans_folder = "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/original_execution_plans_local/0GB"
generated_exec_plans_folder = "/Users/researchuser7/IdeaProjects/flink-playground-jobs/out/generated_abstract_exec_plans"


def get_g_pact(pact):
    g_pact = ""
    for k, v in NodeOp.OPERATORS.items():
        if pact in v:
            g_pact = k
            break
    if g_pact == "":
        g_pact = pact
    return g_pact


def enrich_plan_table(o_nodes_df, p_G, verbose=False):
    nodes_df = o_nodes_df.copy()

    nodes_df[NodeOp.FIELDS["Plan Id"]] = p_G.graph[NodeOp.FIELDS["Plan Id"]]

    # Init new fileds
    for type_f in ["parents", "children"]:
        for f in NodeOp.FIELDS[type_f].values():
            nodes_df[f] = 0
    # print(nodes_df.columns)

    # Iterate over plan operators
    for idx in range(nodes_df.__len__()):
        p_id = nodes_df.loc[idx, "id"]
        pact = nodes_df.loc[idx, "pact"]

        # Get grouped pact

        nodes_df.loc[idx, NodeOp.FIELDS["Grouped Pact"]] = get_g_pact(pact)

        # Get parents and children
        parents = list(p_G.predecessors(p_id))
        children = list(p_G.successors(p_id))
        print(p_id, "-> p:", parents, "- c:", children) if verbose else None

        # For each parent
        p_g_pacts = []
        print("|Parents") if verbose else None

        for p in parents:
            p_pact = p_G.nodes[p]["pact"]
            p_g_pacts.append(get_g_pact(p_pact))
            print("|-", p, ":", p_pact) if verbose else None

            if p_pact not in NodeOp.get_operator_pacts():
                raise Exception(f"Unknown pact: {p_pact}")

            for o in NodeOp.OPERATORS.keys():
                if p_pact in NodeOp.OPERATORS[o]:
                    nodes_df.loc[idx, NodeOp.FIELDS["parents"][o]] += 1

        nodes_df.loc[idx, NodeOp.FIELDS["Parent Grouped Pact"]] = ", ".join(sorted(p_g_pacts))
        print("|") if verbose else None

        # For each children
        c_g_pacts = []
        print("|Children") if verbose else None
        for c in children:
            c_pact = p_G.nodes[c]["pact"]
            c_g_pacts.append(get_g_pact(c_pact))
            print("|-", c, ":", c_pact) if verbose else None

            if c_pact not in NodeOp.get_operator_pacts():
                raise Exception(f"Unknown pact: {c_pact}")

            for o in NodeOp.OPERATORS.keys():
                if c_pact in NodeOp.OPERATORS[o]:
                    nodes_df.loc[idx, NodeOp.FIELDS["children"][o]] += 1

        nodes_df.loc[idx, NodeOp.FIELDS["Children Grouped Pact"]] = ", ".join(sorted(c_g_pacts))
        print() if verbose else None

    return nodes_df


def get_plan_table(p_G, verbose=False):
    nodes_df = pd.DataFrame([n[1] for n in list(p_G.nodes.data())])
    # display(nodes_df)
    try:
        nodes_df = nodes_df.loc[:, ["id", "type", "pact", "color", "predecessors", "driver_strategy"]]
    except:
        nodes_df = nodes_df.loc[:, ["id", "type", "pact", "color", "predecessors"]]
        nodes_df["driver_strategy"] = None
    return enrich_plan_table(nodes_df, p_G, verbose=verbose)


def get_plan_tables_from_plans(plan_graphs, verbose=False):
    p_tabs = []
    for plan_idx in range(plan_graphs.__len__()):
        # print(plan_graphs[plan_idx].graph["plan_id"])
        p_tab = get_plan_table(plan_graphs[plan_idx], verbose=verbose)
        p_tabs.append(p_tab)

    return pd.concat(p_tabs, ignore_index=True)


def parseOperators(exec_plans, flatten=False):
    if flatten:
        return [o  for ep in exec_plans for o in ep[1]["executionPlan"]["nodes"]]
    return [ep[1]["executionPlan"]["nodes"] for ep in exec_plans]


def compute_graph_from_plan(exec_plan, include_cycles=True):
    ex_plan_file, ex_plan = exec_plan

    G = nx.DiGraph()

    plan_id = ex_plan_file.replace(".json", "")
    # print(plan_id)
    # print()
    plan_metadata = plan_id.split("-")
    timestamp = plan_metadata[0]
    workload = plan_metadata[1]
    sql = 1 if (plan_metadata.__len__() == 3) and (plan_metadata[2] == "SQL") else 0

    G.graph["plan_id"] = plan_id
    G.graph["timestamp"] = timestamp
    G.graph["workload"] = workload
    G.graph["sql"] = sql

    for n in ex_plan["executionPlan"]["nodes"]:
        # print(n["id"])

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

        else:
            G.add_node(n["id"], **n, color="lightblue")

        if "predecessors" in n.keys():
            pres = n["predecessors"]
            for p in pres:
                color = "orange" if p["ship_strategy"] == "Broadcast" else "black"
                G.add_edge(p["id"], n["id"], **p, color=color)

    return G


def expand_multi_in_out_nodes(reach_nodes_table):
    exp_reach_nodes_list = []
    for k,v in reach_nodes_table.iterrows():
        p_g_pacts = v["p_g_pact"].split(",")
        c_g_pacts = v["c_g_pact"].split(",")
        for p_g_p in p_g_pacts:
            for c_g_p in c_g_pacts:
                #print("->", p_g_p,",", c_g_p)
                v2 = v.copy()
                v2["p_g_pact_exp"] = p_g_p.strip()
                v2["c_g_pact_exp"] = c_g_p.strip()
                exp_reach_nodes_list.append(v2)
        #print("-----")
    return pd.concat(exp_reach_nodes_list, axis=1, ignore_index=True).T


def compute_transition_matrix(nodes_table, relationship="children", g_pact_exp=True):
    rel_field = "c_g_pact" if relationship == "children" else "p_g_pact"
    g_pact_exp_field = f"{rel_field}_exp" if g_pact_exp else rel_field

    dists = []
    for g_p in nodes_table["g_pact"].unique():
        v_c = nodes_table.loc[(nodes_table["g_pact"] == g_p), g_pact_exp_field].value_counts()
        norm_v_c = v_c / v_c.sum()
        norm_v_c.name = g_p
        dists.append(norm_v_c)

    transotion_matrix = pd.DataFrame(dists)
    transotion_matrix = transotion_matrix.sort_index(axis=0).sort_index(axis=1)
    transotion_matrix = transotion_matrix.fillna(0.0)

    assert (transotion_matrix.shape[0] == transotion_matrix.shape[1])
    assert (transotion_matrix.sum(axis=1).sum() == transotion_matrix.shape[0])

    return transotion_matrix


if __name__ == '__main__':
    exec_plan_files = os.listdir(exec_plans_folder)

    exec_plans_str = []

    for ep_file in exec_plan_files:
        with open(os.path.join(exec_plans_folder, ep_file)) as f:
            exec_plans_str.append((ep_file, " ".join(f.readlines()).replace('"UTF-16LE"', r'\"UTF-16LE\"')))

    # Load json plans
    exec_plans = [(exec_plans_str[i][0], json.loads(exec_plans_str[i][1])) for i in range(exec_plans_str.__len__())]

    # operators = parseOperators(exec_plans, flatten=True)

    plan_graphs = []
    for exec_plan in exec_plans:
        G = compute_graph_from_plan(exec_plan, False) # Load without iteration
        plan_graphs.append(G)

    # Show plans
    # for pg in plan_graphs:
    #     nx.draw(pg, graphviz_layout(pg, prog='dot'), with_labels=True,
    #             node_color=[n[1]["color"] for n in pg.nodes.data()],
    #             edge_color=[e[2]["color"] for e in pg.edges.data()]
    #             )
    #     plt.title(pg.graph["plan_id"])
    #     plt.show()
    #     plt.close()

    reach_nodes_table = get_plan_tables_from_plans(plan_graphs)
    #print(reach_nodes_table)

    expanded_reach_nodes_table = expand_multi_in_out_nodes(reach_nodes_table)
    print(expanded_reach_nodes_table)

    children_transition_matrix = compute_transition_matrix(expanded_reach_nodes_table, relationship="children",
                                                           g_pact_exp=True)
    print(children_transition_matrix)

    parent_transition_matrix = compute_transition_matrix(expanded_reach_nodes_table, relationship="parent",
                                                         g_pact_exp=True)
    print(parent_transition_matrix)

    for i in range(10):
        abs_generator = AbstractPlanGenerator(children_transition_matrix, parent_transition_matrix, seed=i)

        #TODO the max_depth of the plan is also determistic since AbstractPlanGenerator sets the seed globally
        G = abs_generator.generate(np.random.randint(4, 7)) # abs_generator.generate(4)
        # nx.write_graphml_lxml(G, f"plan_{i}.graphml")
        # nx.write_gexf(G, f"out/plan_{i}.gexf")

        data1 = nx.json_graph.node_link_data(G)
        s1 = json.dumps(data1)
        with open(os.path.join(generated_exec_plans_folder, f"plan_{i}.json"), "w") as fp:
            json.dump(data1, fp)
        print(s1)

        nx.draw(G, graphviz_layout(G, prog='dot'), with_labels=True,
                node_color=[n[1]["color"] for n in G.nodes.data()]
                )
        plt.title(f"Plan {i}")
        plt.savefig(os.path.join(generated_exec_plans_folder,f"plan_{i}.pdf"))
        plt.close()

