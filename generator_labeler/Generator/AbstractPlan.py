import time

import numpy as np
from numpy import random
import networkx as nx
from networkx.algorithms.dag import dag_longest_path_length
import pandas as pd

from generator_labeler.FeatureExtraction import FeatureExtraction
from generator_labeler.Generator.Node import NodeOp

class AbstactPlanGeneratorModel:

    def __init__(self, max_depth = -1, max_joins = -1, seed="rand"):

        self.max_depth = max_depth
        self.max_joins = max_joins
        self.seed = seed

        self.children_transition_matrix = None
        self.parent_transition_matrix = None

        self.n_children_mapping = None
        self.n_parents_mapping = None

        self.longest_path_length_dist = None

        if self.seed == "rand":
            random.seed(int(time.time()))
        elif isinstance(self.seed, int):
            random.seed(self.seed)
        else:
            raise Exception(f"Uknown seed: {self.seed}")

    def fit(self, exec_plans_graph):
        reach_nodes_table = FeatureExtraction.get_plan_tables_from_plans(exec_plans_graph)

        self.n_children_mapping = self.get_n_relatives_dist(reach_nodes_table, relationship="children")
        #print(self.n_children_mapping)
        self.n_parents_mapping = self.get_n_relatives_dist(reach_nodes_table, relationship="parents")
        #print(self.n_parents_mapping)

        expanded_reach_nodes_table = FeatureExtraction.explode_multi_in_out_nodes(reach_nodes_table)

        self.children_transition_matrix = self.compute_transition_matrix(expanded_reach_nodes_table, relationship="children", g_pact_exp=True)
        self.parent_transition_matrix = self.compute_transition_matrix(expanded_reach_nodes_table, relationship="parents", g_pact_exp=True)

        path_lengths = np.array([dag_longest_path_length(pg) for pg in exec_plans_graph])

        self.longest_path_length_dist = np.quantile(path_lengths, q=np.arange(0.01, 1.0, 0.01), interpolation="higher")
        #[0.1, 0.25, 0.5, 0.75, 0.9, 1.0]
        #print(self.longest_path_length_dist)

        return self

    def predict(self, n, rand_seed=False):
        generated_plans = []

        for i in range(n):
            abs_generator = AbstractPlanGenerator(self.children_transition_matrix,
                                                  self.parent_transition_matrix,
                                                  seed=i if not rand_seed else "rand")

            m_depth = self.max_depth if self.max_depth != -1 else random.choice(self.longest_path_length_dist)
            m_joins = self.max_joins

            G = abs_generator.generate(m_depth, m_joins)
            G.graph["plan_id"] = f"plan_{i}"
            generated_plans.append(G)

        return generated_plans

    def compute_transition_matrix(self, nodes_table, relationship="children", g_pact_exp=True):
        """
        :param nodes_table:
        :param relationship:
        :param g_pact_exp: True if you want to use the node exploded field, False otherwise.
        :return:
        """
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

    def get_n_relatives_dist(self, nodes_table, relationship="children"):
        n_children_dist = nodes_table.loc[:, ["g_pact"] + list(NodeOp.FIELDS[relationship].values())] \
            .set_index("g_pact") \
            .sum(axis=1) \
            .groupby(level=0).apply(
            lambda x: np.quantile(np.array(x), q=np.arange(0.01, 1.0, 0.01), interpolation="lower")).to_dict()

        return n_children_dist


class AbstractPlanGenerator:
    ALPHA_CODE = "abcdefghijklmnopqrstuvwxyz"
    branch_count = 0

    def __init__(self, children_transition_matrix, parent_transition_matrix, seed="rand"):
        self.node_count = 0
        self.join_count = 0
        self.abstract_G = nx.DiGraph()

        self.children_transition_matrix = children_transition_matrix
        self.parent_transition_matrix = parent_transition_matrix

        if seed == "rand":
            random.seed(int(time.time()))
        elif isinstance(seed, int):
            random.seed(seed)
        else:
            raise Exception(f"Uknown seed: {seed}")

    def get_next_node_pact(self, list_of_candidates, number_of_items_to_pick, probability_distribution):
        next_node_pact = random.choice(list_of_candidates, number_of_items_to_pick, p=probability_distribution)[0]
        return next_node_pact

    def get_node_children_candidates(self, node):
        candidates = self.children_transition_matrix.columns.values
        proba_dist = self.children_transition_matrix.loc[node.pact, :].values
        return candidates, proba_dist

    def get_node_parent_candidates(self, node):
        candidates = self.parent_transition_matrix.columns.values
        proba_dist = self.parent_transition_matrix.loc[node.pact, :].values
        return candidates, proba_dist

    def create_node(self, pact):
        n = NodeOp(pact, self.node_count)
        self.node_count += 1
        return n

    def add_node(self, node):
        self.abstract_G.add_node(node.node_id, pact=node.pact, color=node.color)

    def add_edge(self, node_1, node_2, reverse=False):
        if reverse:
            self.abstract_G.add_edge(node_2.node_id, node_1.node_id)
        else:
            self.abstract_G.add_edge(node_1.node_id, node_2.node_id)

    def update_plan(self, current_node, next_node, reverse=False):
        self.add_node(next_node)
        self.add_edge(current_node, next_node, reverse=reverse)

    def backward_join_branch_generation(self, join_node, max_depth):

        current_node = join_node
        next_node = None

        candidates, proba_dist = self.get_node_parent_candidates(current_node)
        number_of_items_to_pick = current_node.n_children

        for i in range(max_depth):
            # Avoid Joins in backward branches starting from a Join
            next_node_pact = "Join"
            while "Join" in next_node_pact:
                next_node_pact = self.get_next_node_pact(candidates, number_of_items_to_pick, proba_dist)

            next_node = self.create_node(next_node_pact)
            self.update_plan(current_node, next_node, reverse=True)

            # update
            current_node = next_node
            next_node = None

            candidates, proba_dist = self.get_node_parent_candidates(current_node)
            number_of_items_to_pick = current_node.n_children

            # check early exit status
            if "Data Source" in current_node.pact:
                break

        # check correct end of the plan
        if "Data Source" not in current_node.pact:
            next_node = self.create_node("Data Source")
            self.update_plan(current_node, next_node, reverse=True)

    def generate(self, max_depth, max_joins=-1):
        current_node = self.create_node("Data Source")
        # Add first source node
        self.add_node(current_node)

        # Init variables
        next_node = None
        previous_node = None

        candidates, proba_dist = self.get_node_children_candidates(current_node)
        number_of_items_to_pick = current_node.n_children

        # Foreward main branch generation
        for i in range(max_depth):
            next_node_pact = self.get_next_node_pact(candidates, number_of_items_to_pick, proba_dist)

            if max_joins > -1:
                # If join count is over th, change the node
                if ("Join" in next_node_pact) and (self.join_count >= max_joins):
                    while "Join" in next_node_pact:
                        next_node_pact = self.get_next_node_pact(candidates, number_of_items_to_pick, proba_dist)
                # Else increase the counter
                elif "Join" in next_node_pact:
                    self.join_count += 1

            # Create next node
            next_node = self.create_node(next_node_pact)

            self.update_plan(current_node, next_node, reverse=False)

            # update
            previous_node = current_node
            current_node = next_node
            next_node = None

            candidates, proba_dist = self.get_node_children_candidates(current_node)
            number_of_items_to_pick = current_node.n_children

            if "Join" in current_node.pact:
                self.backward_join_branch_generation(current_node, int(max_depth/2))

            # check early exit status
            if "Data Sink" in current_node.pact:
                break

        # check correct end of the plan
        if "Data Sink" not in current_node.pact:
            next_node = self.create_node("Data Sink")
            self.update_plan(current_node, next_node, reverse=False)

        return self.abstract_G