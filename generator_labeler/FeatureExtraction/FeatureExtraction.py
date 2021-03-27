from generator_labeler.Generator.Node import NodeOp

import pandas as pd


def explode_multi_in_out_nodes(reach_nodes_table):
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
    # Create dataframe from nodes body
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
        p_tab = get_plan_table(plan_graphs[plan_idx], verbose=verbose)
        p_tabs.append(p_tab)

    return pd.concat(p_tabs, ignore_index=True)


