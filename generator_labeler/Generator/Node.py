class NodeOp:
    OPERATORS = {
        "Data Source": ["Data Source"],
        "Filter": ["Filter"],
        "Map": ["Map", "FlatMap", "CoMap", "CoFlatMap", "MapPartition"],
        "Reduce": ["Reduce"],
        "Group by": ["GroupReduce", "CoGroup", "GroupCombine"],
        "Join": ["Join", "Outer Join"],
        "Bulk Iteration": ["Bulk Iteration", "Workset Iteration"],
        "Partition": ["Partition"],
        "Sort-Partition": ["Sort-Partition"],
        "Data Sink": ["Data Sink"]
    }

    FIELDS = {
        "Plan Id": "plan_id",
        "Grouped Pact": "g_pact",
        "Parent Grouped Pact": "p_g_pact",
        "Children Grouped Pact": "c_g_pact",
        "parents": {
            "Data Source": "#_p_source",
            "Map": "#_p_map",
            "Filter": "#_p_filter",
            "Reduce": "#_p_reduce",
            "Join": "#_p_join",
            "Group by": "#_p_groupby",
            "Partition": "#_p_partition",
            "Sort-Partition": "#_p_sort",
            "Bulk Iteration": "#_p_iteration",
        },
        "children": {
            "Data Sink": "#_c_source",
            "Map": "#_c_map",
            "Filter": "#_c_filter",
            "Reduce": "#_c_reduce",
            "Join": "#_c_join",
            "Group by": "#_c_groupby",
            "Partition": "#_c_partition",
            "Sort-Partition": "#_c_sort",
            "Bulk Iteration": "#_c_iteration",
        }
    }

    @classmethod
    def get_operator_color(c, node):
        if node["pact"] == "Bulk Iteration":
            return "purple"
        elif (node["pact"] == "Data Source") or (node["pact"] == "Data Sink"):
            return "green"
        elif node["pact"] == "Partition":
            return "orange"
        else:
            return "lightblue"

    @classmethod
    def get_operator_pacts(c):
        return [v for g in c.OPERATORS.values() for v in g]

    def __init__(self, pact, n_id):
        self.pact = pact
        self.n_id = n_id
        self.node_id = f"{pact}_{n_id}"
        self.n_children = 1

        # meta-data

        if self.pact == "Bulk Iteration":
            self.color = "purple"
        elif (self.pact == "Data Source") or (self.pact == "Data Sink"):
            self.color = "green"
        else:
            self.color = "lightblue"

    def __str__(self):
        return self.node_id
