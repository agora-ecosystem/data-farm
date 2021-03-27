import pandas as pd
import numpy as np
import sys
from DatasetMetadata import TableMetaData

#data_source = "/Users/researchuser7/Desktop/tpc-test/sample_data/"
data_source = "/Users/researchuser7/Desktop/tpc-test/db_out/"
#lineitem_table = "sample_lineitem.tbl"
lineitem_table = "lineitem.tbl"


qs = [0.25, 0.5, 0.75]


def analyze_filter_field(ff, data_df):
    f_dict = {}

    if data_df[ff].dtype == "datetime64[ns]":
        f_df = data_df[ff].astype(np.int64).quantile(qs)
        f_dict["selectivity"] = [str(x) for x in f_df.index.values.tolist()]
        f_dict["values"] = [str(x) for x in pd.to_datetime(f_df).apply(lambda x: x.strftime("%Y-%m-%d")).values.tolist()]

    elif data_df[ff].dtype == "object":
        f_df = (data_df.groupby(ff).size() / data_df.__len__())
        f_dict["selectivity"] = [str(x) for x in f_df.values.tolist()]
        f_dict["values"] = [str(x) for x in f_df.index.values.tolist()]

    else:
        f_df = data_df[ff].quantile(qs)
        f_dict["selectivity"] = [str(x) for x in f_df.index.values.tolist()]
        f_dict["values"] = [str(x) for x in f_df.values.tolist()]

    return f_dict


def analyze_groupby_field(gf, data_df):
    g = data_df.groupby(gf).size()
    #print(g)
    return str(g.__len__())


def toScala(s):
    s = s.replace("{", "Map(")
    s = s.replace("}", ")")
    s = s.replace("[", "Seq(")
    s = s.replace("]", ")")
    s = s.replace(":", "->")
    s = s.replace("'", '"')
    return s


def main():
    db_meta_dict = TableMetaData.database_meta

    for table_n, table_info_dict in db_meta_dict.items():
        print(f"| Analyzing '{table_n}'")

        data_df = pd.read_csv(data_source + table_n + ".tbl", sep="|", header=None, names=table_info_dict["fields"],
                              parse_dates=table_info_dict["date_fields"], infer_datetime_format=True)

        print(data_df.info())

        print("| FILTER FIELDS")
        filterFields_dict = {}
        for ff in table_info_dict["filter_fields"]:
            print(f"|--> {ff}")
            ff_dict = analyze_filter_field(ff, data_df)
            print(f"|  {ff_dict}")

            filterFields_dict[ff] = ff_dict

        print("Scala:")
        print(toScala(str(filterFields_dict)))

        print("| GROUPBY FIELDS")
        groupby_fields = {}
        for gf in table_info_dict["groupby_fields"]:
            print(f"|--> {gf}")
            groupby_fields[gf] = analyze_groupby_field(gf, data_df) #"-1.0"

        print("Scala:")
        print(toScala(str(groupby_fields)))

        print("===============================================")


if __name__ == '__main__':

    main()



