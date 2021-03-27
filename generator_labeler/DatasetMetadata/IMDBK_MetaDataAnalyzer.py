import pandas as pd
import numpy as np
import sys
from DatasetMetadata import IMDBK_MetaData

data_source = "/Users/researchuser7/PROJECTS/WorkloadAugmentation/Data/IMDB_kaggle/preprocessed/"
source_0 = "name.basics.tsv"


qs = [0.25, 0.5, 0.75, 0.95, 0.97, 0.99]


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


def analyze_filter_field_2(ff, data_df, f_values):
    f_dict = {}

    if f_values.dtype == "object":
        f_dict["selectivity"] = []
        f_dict["values"] = []
        for v in f_values.values:
            selectivity = data_df[data_df[ff].str.contains(v).fillna(False)].__len__() / data_df.__len__()
            f_dict["selectivity"].append(str(selectivity))
            f_dict["values"].append(v)
    else:
        raise Exception()

    return f_dict


def analyze_groupby_field(gf, data_df):
    return data_df[gf].nunique()


def toScala(s):
    s = s.replace("{", "Map(")
    s = s.replace("}", ")")
    s = s.replace("[", "Seq(")
    s = s.replace("]", ")")
    s = s.replace(":", "->")
    s = s.replace("'", '"')
    return s


def main():
    db_meta_dict = IMDBK_MetaData.database_meta

    for table_n, table_info_dict in db_meta_dict.items():
        print(f"| Analyzing '{table_n}'")

        data_df = pd.read_csv(data_source + table_n + ".csv", sep=";", header=None, names=table_info_dict["fields"],
                              parse_dates=table_info_dict["date_fields"], infer_datetime_format=True)

        print(data_df.info())
        print(data_df.head(10))

        print("| JOIN FIELDS")
        print(f"|  {db_meta_dict[table_n]['join_fields']}")
        print("Scala:")
        print(toScala(str(db_meta_dict[table_n]['join_fields'])))
        print("----------------")

        print("| FILTER FIELDS")
        filterFields_dict = {}
        for ff in table_info_dict["filter_fields"]:
            print(f"|--> {ff}")

            if "primaryProfession" in ff:
                df_res = data_df["primaryProfession"].apply(
                    lambda x: np.array(str(x).split(","), dtype='object')).values
                unique_vals = pd.Series(np.unique(np.concatenate(df_res)))
                ff_dict = analyze_filter_field_2(ff, data_df, unique_vals)
            else:
                ff_dict = analyze_filter_field(ff, data_df)

            print(f"|  {ff_dict}")

            filterFields_dict[ff] = ff_dict

        print("Scala:")
        print(toScala(str(filterFields_dict)))
        print("----------------")

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



