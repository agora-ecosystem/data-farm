data_cardinality = {
    "1GB":{
        "customer": 150000,
        "lineitem": 6001215,
        "nation": 25,
        "orders": 1500000,
        "part": 200000,
        "partsupp": 800000,
        "supplier": 10000
    },

    # TODO move to IMDB
    "3GB": {
        "title.principals": 36499704,
        "title.akas": 19344171,
        "title.ratings": 993821,
        "title.basics": 6326545,
        "name.basics": 9711022
    },
    ##################


    "5GB":{
        "customer": 750000,
        "lineitem": 29999795,
        "nation": 25,
        "orders": 7500000,
        "part": 1000000,
        "partsupp": 4000000,
        "supplier": 50000
    },
    "10GB":{
        "customer": 1500000,
        "lineitem": 59986052,
        "nation": 25,
        "orders": 15000000,
        "part": 2000000,
        "partsupp": 8000000,
        "supplier": 100000
    },
    "50GB":{
        "customer": 7500000,
        "lineitem": 300005811,
        "nation": 25,
        "orders": 75000000,
        "part": 10000000,
        "partsupp": 40000000,
        "supplier": 500000
    }
}

database_meta = {
    "lineitem": {
        "fields": ["ORDERKEY",
                   "PARTKEY",
                   "SUPPKEY",
                   "LINENUMBER",
                   "QUANTITY",
                   "EXTENDEDPRICE",
                   "DISCOUNT",
                   "TAX",
                   "RETURNFLAG",
                   "LINESTATUS",
                   "SHIPDATE",
                   "COMMITDATE",
                   "RECEIPTDATE",
                   "SHIPINSTRUCT",
                   "SHIPMODE",
                   "COMMENT"],

        "date_fields": ['SHIPDATE', 'COMMITDATE', 'RECEIPTDATE'],

        "filter_fields": ["QUANTITY",
                          "SHIPDATE",
                          "SHIPMODE"
                          ],

        "join_fields": {
            "ORDERKEY": {
                "orders": "ORDERKEY"
            },
            "PARTKEY": {
                "part": "PARTKEY",
                "partsupp": "PARTKEY"
            },
            "SUPPKEY": {
                "supplier": "SUPPKEY",
                "partsupp": "SUPPKEY"
            }
        },

        "groupby_fields": ["ORDERKEY",
                           "PARTKEY",
                           "SUPPKEY",
                           "SHIPMODE"
                           ]
    },

    "orders": {
        "fields": ["ORDERKEY",
                   "CUSTKEY",
                   "ORDERSTATUS",
                   "TOTALPRICE",
                   "ORDERDATE",
                   "ORDERPRIORITY",
                   "CLERK",
                   "SHIPPRIORITY",
                   "COMMENT",
                   ],

        "date_fields": ["ORDERDATE"],

        "filter_fields": ["ORDERSTATUS", "TOTALPRICE", "ORDERDATE"],

        "join_fields": {
            "ORDERKEY": {
                "lineitem": "ORDERKEY"
            },
            "CUSTKEY": {
                "customer": "CUSTKEY"
            }
        },

        "groupby_fields": ["ORDERKEY",
                           "CUSTKEY",
                           "ORDERSTATUS"
                           ]
    },

    "customer": {
        "fields": ["CUSTKEY",
                   "NAME",
                   "ADDRESS",
                   "NATIONKEY",
                   "PHONE",
                   "ACCTBAL",
                   "MKTSEGMENT",
                   "COMMENT"
                   ],

        "date_fields": [],

        "filter_fields": ["ACCTBAL", "MKTSEGMENT"],

        "join_fields": {
            "CUSTKEY": {
                "orders": "CUSTKEY"
            },
            "NATIONKEY": {
                "nation": "NATIONKEY"
            }
        },

        "groupby_fields": ["MKTSEGMENT"]
    },

    "nation": {

        "fields": [
            "NATIONKEY",
            "NAME",
            "REGIONKEY",
            "COMMENT",
        ],

        "date_fields": [],

        "filter_fields": ["REGIONKEY"],

        "join_fields": {
            "NATIONKEY": {
                "customer": "NATIONKEY",
                "supplier": "NATIONKEY"
            }
        },

        "groupby_fields": ["REGIONKEY"]

    },

    "part": {

        "fields": ["PARTKEY",
                   "NAME",
                   "MFGR",
                   "BRAND",
                   "TYPE",
                   "SIZE",
                   "CONTAINER",
                   "RETAILPRICE",
                   "COMMENT",
                   ],

        "date_fields": [],

        "filter_fields": ["BRAND", "TYPE", "RETAILPRICE"],

        "join_fields": {
            "PARTKEY": {
                "partsupp": "PARTKEY",
                "lineitem": "PARTKEY"
            }
        },

        "groupby_fields": ["BRAND", "TYPE"]
    },

    "partsupp": {

        "fields": ["PARTKEY",
                   "SUPPKEY",
                   "AVAILQTY",
                   "SUPPLYCOST",
                   "COMMENT"
                   ],

        "date_fields": [],

        "filter_fields": ["AVAILQTY", "SUPPLYCOST"],

        "join_fields": {
            "PARTKEY": {
                "part": "PARTKEY"
            },
            "SUPPKEY": {
                "supplier": "SUPPKEY"
            }
        },

        "groupby_fields": ["PARTKEY", "SUPPKEY"]
    },

    "supplier": {

        "fields": ["SUPPKEY",
                   "NAME",
                   "ADDRESS",
                   "NATIONKEY",
                   "PHONE",
                   "ACCTBAL",
                   "COMMENT"
                   ],

        "date_fields": [],

        "filter_fields": ["ACCTBAL", "NATIONKEY"],

        "join_fields": {
            "SUPPKEY": {
                "partsupp": "SUPPKEY",
                "lineitem": "SUPPKEY"
            },
            "NATIONKEY": {
                "nation": "NATIONKEY"
            }
        },

        "groupby_fields": ["NATIONKEY"]
    }
}
