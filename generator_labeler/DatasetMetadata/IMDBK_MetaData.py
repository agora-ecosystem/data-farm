tables = [
"title.principals.csv",
"title.akas.csv",
"title.ratings.csv",
"title.basics.csv",
"name.basics.csv"
]

data_cardinality = {
    "3GB":{
        "title.principals": 36499704,
        "title.akas": 19344171,
        "title.ratings": 993821,
        "title.basics": 6326545,
        "name.basics": 9711022
    }
}

database_meta = {
    "title.principals": {
        "schema": "(String, Int, String, String, String, String)",
        "fields": ['titleId', 'ordering', 'nconst', 'category', 'job', 'characters'],

        "date_fields": [],

        "filter_fields": ["category"],

        "join_fields": {
            "titleId": {
                "title.akas": "titleId",
                "title.ratings": "titleId",
                "title.basics": "titleId",
            },
            "nconst": {
                "name.basics":"nconst"
            }
        },

        "groupby_fields": ["category", "nconst", "titleId"]
    },

    "title.akas": {
        "schema": "(String, Int, String, String, String, String, String, String)",

        "fields": ['titleId', 'ordering', 'title', 'region', 'language', 'types', 'attributes', 'isOriginalTitle'],

        "date_fields": [],

        "filter_fields": ["region", "types", "isOriginalTitle"],

        "join_fields": {
            "titleId": {
                "title.principals": "titleId",
                "title.ratings": "titleId",
                "title.basics": "titleId",
            }
        },

        "groupby_fields": ["region",
                           "types",
                           "isOriginalTitle"
                           ]
    },

    "title.ratings": {
        "schema": "(String, Float, Int)",

        "fields": ['titleId', 'averageRating', 'numVotes'],

        "date_fields": [],

        "filter_fields": ["averageRating", "numVotes"],

        "join_fields": {
            "titleId": {
                "title.principals": "titleId",
                "title.akas": "titleId",
                "title.basics": "titleId",
            }
        },

        "groupby_fields": []
    },

    "title.basics": {
        "schema": "(String, String, String, String, Int, Int, Int, Int, String)",

        "fields": ['titleId', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult',
       'startYear', 'endYear', 'runtimeMinutes', 'genres'],

        "date_fields": [],

        "filter_fields": ["titleType", "isAdult", "startYear"],

        "join_fields": {
            "titleId": {
                "title.principals": "titleId",
                "title.akas": "titleId",
                "title.ratings": "titleId"
            }
        },

        "groupby_fields": ["isAdult", "titleType"]

    },

    "name.basics": {
        "schema": "(String, String, Int, Int, String, String)",

        "fields": ['nconst', 'primaryName', 'birthYear', 'deathYear', 'primaryProfession', 'knownForTitles'],

        "date_fields": [],

        "filter_fields": ["birthYear", "deathYear", "primaryProfession"],

        "join_fields": {
            "nconst": {
                "title.principals": "nconst"
            }
        },
        "groupby_fields": ["primaryProfession"]
    }
}
