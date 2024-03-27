from itertools import chain
import pandas as pd
import os
import json
from datetime import datetime, date, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pprint import pprint
import matplotlib.pyplot as plt
from urllib.parse import urlparse
from IPython.display import display
from collections import Counter


relations = [
    'child',
    'father',
    'godparent',
    'mother',
    'relative',
    'sibling',
    'spouse',
    'wife', ###
    "husband", ###
    "parent", ###
    'son', ###
    'daughter', ###
    'brother', ###
    'sister', ###
    "cousin", ###
    "grnadfather", ###
    "grandmother", ###
    'stepparent',
    'unmarried partner',
    "partner", ###
]

def try_convert_to_datetime(datetime_str):
    try:
        return pd.to_datetime(datetime_str).date()
    except:
        return


def load_enriched_entities(path, verbose=True):
    entities_enriched = []
    with open(path, "r") as fh:
        for row in fh:
            entities_enriched.append(json.loads(row))
    
    return entities_enriched


def analyze_relatives(df, verbose=True):
    relatives_df = df["web_search_properties.relatives"].apply(pd.Series).add_prefix("web_search_properties.relatives.")
    relatives_df = relatives_df[[col for col in relatives_df.columns if any([relation in col.split(".")[-1].lower() for relation in relations])]]
    if verbose:
        print("% values filled, relatives:")
        display((1 - relatives_df.isna().sum() / relatives_df.shape[0]) * 100)
    
    return relatives_df


def analyze_associates(df, verbose=True):
    associates_df = df["web_search_properties.associates"].apply(lambda x: len(x) if isinstance(x, list) else 0)
    if verbose:
        print("% values filled, associates:",
            (1 - (associates_df == 0).sum() / df.shape[0])*100)
        associates_df = df["web_search_properties.associates"].apply(lambda x: len(x) if isinstance(x, list) else 0)
        associates_df.hist(bins=20)
        plt.title("Number of associates per entity histogram")
        plt.show()
    
    return associates_df

def analyze_properties(df, verbose=True):
    web_search_properties = df["web_search_properties.properties"].apply(pd.Series)
    if verbose:
        print("% values filled, properties:")
        display((1 - web_search_properties.isna().sum() / web_search_properties.shape[0]) * 100)
        
        print("% values filled, position held:", (1 - df["web_search_properties.positions_held"].isna().sum() / df.shape[0]) * 100)

    return web_search_properties


def analyze_positions(df, run_date, verbose=True):
    position_held_start_df = df["web_search_properties.positions_held"].apply(
        lambda x: [(x[position].get("start_date")) for position in x] if isinstance(x, dict) else []
    ).apply(
        lambda x: [[try_convert_to_datetime(_date) for _date in dates] if isinstance(dates, list) else [] for dates in x]
    ).apply(lambda x: sum(any(isinstance(_date, date) for _date in dates) for dates in x))
    
    position_held_start = position_held_start_df.sum() / \
        df["web_search_properties.positions_held"].apply(
            lambda x: len(x) if isinstance(x, dict) else 0
        ).sum()

    position_held_end_df = df["web_search_properties.positions_held"].apply(
        lambda x: [(x[position].get("end_date")) for position in x] if isinstance(x, dict) else []
    ).apply(
        lambda x: [[try_convert_to_datetime(_date) for _date in dates] if isinstance(dates, list) else [] for dates in x]
    ).apply(lambda x: sum(any(isinstance(_date, date) for _date in dates) for dates in x))
    position_held_end = position_held_end_df.sum() / \
        df["web_search_properties.positions_held"].apply(
            lambda x: len(x) if isinstance(x, dict) else 0
        ).sum()

    if verbose:
        print("% values filled, position held start:", position_held_start * 100)
        print("% values filled, position held end:", position_held_end * 100)
    
    return position_held_start_df, position_held_end_df


def analyze_enrichment_sources(df, run_date, verbose=True):
    enrichment_sources_df = pd.DataFrame(
        df["web_search_properties.source"].apply(lambda x: len(x) if isinstance(x, list) else 0),
        # columns=["source_len"]
    )
    
    if verbose:
        enrichment_sources_df.hist(bins=6)
        plt.title("Number of sources - entity enrichment")
        plt.xlabel("Number of sources")
        plt.show()

    enrichment_sources_df["source_age"] = df["web_search_properties.source"].apply(
        lambda x: [source["date"] for source in x] if isinstance(x, list) else []
    ).apply(
        lambda x: [(try_convert_to_datetime(ds) - run_date.date()).days for ds in x if ds is not None]
    )
    
    if verbose:
        plt.hist(
            enrichment_sources_df["source_age"].sum(),
            bins=100,
        )
        # enrichment_sources_df["source_age"].sum().hist(bins=100)
        plt.title("Age of web page source - entity enrichment")
        plt.xlabel("Days")
        plt.show()
    
    return enrichment_sources_df


def examine_run(path, rundate, verbose=True):
    
    entities_enriched = load_enriched_entities(path, verbose=verbose)  
    if verbose:          
        print("Sucessfully enriched entities:", sum([len(entity["web_search_properties"]) != 0 for entity in entities_enriched]))
        print("\n\n")
    
    df = pd.json_normalize(entities_enriched, max_level=1)
    run_date = datetime.strptime(rundate, "%Y%m%d")
    
    enrichment_sources_df = analyze_enrichment_sources(df, run_date, verbose=verbose)

    relatives_df = analyze_relatives(df, verbose=verbose)
    associates_df = analyze_associates(df, verbose=verbose)
    properties_df = analyze_properties(df, verbose=verbose)
    position_held_start_df, position_held_end_df = analyze_positions(df, run_date, verbose=verbose)
    
    return (
        df,
        relatives_df,
        associates_df,
        properties_df,
        position_held_start_df,
        position_held_end_df,
        enrichment_sources_df,
    )


def e2e_entities(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    tmp_df = pd.concat(
        [
            pd.concat(
                [
                    pd.DataFrame(
                        {country: _df["properties.name"]}
                    ).melt().rename(columns={"variable": "country", "value": "name"}) for country, _df in df_dict.items()
                ],
                axis=0
            ),
            pd.concat(
                [
                    pd.DataFrame(
                        {country: _df["web_search_properties.source"]}
                    ).melt().rename(columns={"variable": "country", "value": "web_properties"}) for country, _df in df_dict.items()
                ],
                axis=0
            )["web_properties"]
        ],
        axis=1
    )
    tmp_df["web_properties"] = tmp_df["web_properties"].apply(lambda x: "enriched" if isinstance(x, list) else "non-enriched")

    try:
        tmp_df.groupby("country").apply(
            lambda x: x["web_properties"].value_counts()
        ).reset_index().pivot(
            index="web_properties",
            columns="country",
            values="count"
        ).plot(kind="bar")
        plt.grid()
        plt.xlabel("country")
        plt.ylabel("count")
        plt.title("Number of entities per country")
        plt.legend()
        plt.show()
        
        display(
            tmp_df.groupby("country").apply(
                lambda x: x["web_properties"].value_counts(normalize=False)
            ).reset_index().groupby("country")[["web_properties", "count"]].apply(pd.DataFrame)
        )
    except KeyError:
        pass

def e2e_sources(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    tmp_df = pd.concat({country: _df["web_search_properties.source"] for country, _df in enrichment_sources_df_dict.items()}, axis=1)
    tmp_df.apply(lambda x: x.value_counts(normalize=True) * 100).plot.bar(grid=True)
    plt.xlabel("Number of sources per entity (enrichment)")
    plt.ylabel("Percentage")
    plt.title("Web source count by enriched entity - histogram")
    plt.show()
    
    tmp_df = pd.concat({country: _df["source_age"] for country, _df in enrichment_sources_df_dict.items()}, axis=1).map(
        lambda x: x if isinstance(x, list) else []
    ).sum().to_frame().explode(0).reset_index().rename(columns={"index": "country", 0: "age"})
    tmp_df["range"] = pd.cut(tmp_df["age"], bins=[-999999, -365*5, -365*4, -365*3, -365*2, -365, -30, 0])
    xticks = ["5Y+", "5Y", "4Y", "3Y", "2Y", "1Y", "1m"]

    plot_df = tmp_df.groupby(['range', 'country'])["age"].agg("count").reset_index().pivot(
        index="range",
        columns="country",
        values="age"
    )
    plot_df = plot_df / plot_df.sum() * 100

    plot_df.plot(kind="bar")
    plt.xlabel("Web source age (enrichment) [days]")
    plt.ylabel("Percentage")
    plt.title("Web source age by dataset - histogram")
    # plt.xticks(labels=xticks)
    plt.grid()
    plt.legend()
    plt.show()

    plot_df.apply(lambda x: np.cumsum(x[::-1]))[::-1].plot()
    plt.xticks(rotation=90)
    plt.xlabel("Web source cumulative age (extraction) [days]")
    plt.ylabel("Percentage")
    plt.title("Cumulative age of web sources")
    plt.grid()
    plt.legend()
    plt.show()
    
    source_enrichment_df = pd.concat({country: _df["web_search_properties.source"] for country, _df in df_dict.items()}, axis=1).map(
        lambda x: [urlparse(source["url"]).netloc for source in x] if isinstance(x, list) else x
    ).apply(
        lambda x: list(chain.from_iterable([i for i in x if isinstance(i, list)]))
    ).to_frame().explode(0).reset_index().rename(columns={0: "source", "index": "country"}).pivot(
        columns="country",
        values="source"
    )

    for col in source_enrichment_df:
        display(
            pd.concat([source_enrichment_df[col].value_counts(), source_enrichment_df[col].value_counts(normalize=True)], axis=1)
        )
        
    source_enrichment_df.map(
        lambda x: ("wikipedia" if x.endswith("wikipedia.org") else (
            "opensanctions" if x.endswith("opensanctions.org") else (
                ".gov" if ".gov" in x else "other")) )if isinstance(x, str) else x
    ).apply(
        lambda x: x.value_counts(normalize=True) * 100
    ).plot(kind="bar")
    plt.grid()
    plt.xlabel("source")
    plt.ylabel("Percentage")
    plt.title("Sources of profile enrichment")
    plt.legend()
    plt.show()

def e2e_associates(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    tmp_df = pd.DataFrame(associates_df_dict)
    tmp_df.loc[52:, "chad"] = np.nan

    bins = [0, 2, 4, 6, 8, 10, 12, 99999]
    tmp_df.sort_index().apply(
        lambda x: pd.cut(x.values, bins=bins)
    ).apply(lambda x: x.value_counts(normalize=True) * 100).plot(kind="bar")
    plt.grid()
    plt.xlabel("Number of associates")
    plt.ylabel("Precentage")
    plt.title("Number of associates per entity")
    plt.show()

def e2e_relatives(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    relation_type_map = {
        "wife": "partner",
        "first wife": "partner",
        "second wife": "partner",
        "third wife": "partner",
        "husband": "partner",
        "first husband": "partner",
        "second husband": "partner",
        "third husband": "partner",
        "spouse": "partner",
        "spouses": "partner",
        "former husband": "partner",
        "partner": "partner",
        'ex-husband': "partner",
        'ex-wife': "partner",
        "spouse(s)": "partner",
        "ex-husband/spouse": "partner",

        "father": "parent",
        "mother": "parent",
        "parent": "parent",
        "parents": "parent",
        
        "brother": "sibling",
        "twin brother": "sibling",
        "older brother": "sibling",
        "younger brother": "sibling",
        "brothers": "sibling",
        "sister": "sibling",
        "twin sister": "sibling",
        "older sister": "sibling",
        "younger sister": "sibling",
        "sisters": "sibling",
        "siblings": "sibling",
        "sibling": "sibling",
        "elder brother": "sibling",
        
        "son": "child",
        "eldest son": "child",
        "daughter": "child",
        "sons": "child",
        "daughters": "child",
        "child": "child",
        "children": "child",
        "youngest child": "child",
        
        "brother-in-law": "in-law",
        "sister-in-law": "in-law",
        "father-in-law": "in-law",
        "mother-in-law": "in-law",
        "son-in-law": "in-law",
        "daughter-in-law": "in-law",
        "sister's husband": "in-law",
        "mother in law": "in-law",
        
        "grandfather": "grand-relative",
        "great-grandfather": "grand-relative",
        "grandmother": "grand-relative",
        "great-grandmother": "grand-relative",
        "great grandchildren": "grand-relative",
        "grandchildren": "grand-relative",
        "grandson": "grand-relative",
        "grandsons": "grand-relative",
        "granddaughter": "grand-relative",
        "granddaughters": "grand-relative",
        'grandparents': "grand-relative",
        'great grandparents': "grand-relative",
        'great-grandparents': "grand-relative",
        'great-grandson': "grand-relative",
        
        "step-father": "step",
        "step-mother": "step",
        "step-son": "step",
        "step-daughter": "step",
        "step-sibling": "step",
        "step-brother": "step",
        "step-brothers": "step",
        "half-brother": "step",
        "half-brothers": "step",
        "step-sister": "step",
        "step-sisters": "step",
        "half-sister": "step",
        "half-sisters": "step",
        
        "cousin": "cousin",
        "family relatives": "cousin",
        "second cousin": "cousin",
        "cousin": "cousin",
        "cousin": "cousin",
        "cousins": "cousin",
        "relatives": "cousin",
        "relative": "cousin",
    }

    tmp_df = (~pd.concat(relatives_df_dict, axis=1).isna()).sum().reset_index().rename(columns={"level_0": "country", "level_1": "relation", 0:"count"})
    tmp_df["relation"] = tmp_df["relation"].apply(lambda x: x.split(".")[-1].lower())

    print(f"Unmapped relations ({len(set(tmp_df['relation'].unique()) - set(relation_type_map.keys()))}):")
    pprint(set(tmp_df["relation"].unique()) - set(relation_type_map.keys()))

    tmp_df["relation"] =  tmp_df["relation"].apply(lambda x: relation_type_map.get(x))

    tmp_df.groupby(["country", "relation"]).agg("sum").reset_index().pivot(
        index="relation",
        columns="country",
        values="count",
    ).fillna(0).apply(lambda x: x / relatives_df_dict[x.name].shape[0] * 100).plot(kind="bar")
    plt.grid()
    plt.ylabel("Percent")
    plt.title("Relation type breakdown")
    plt.legend()
    plt.show()

def e2e_properties(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    properties_cols = [
        'country',
        'last_name',
        'residence_country',
        'middle_name',
        'gender',
        'title',
        'first_name',
        'nationality',
        'address',
        'alias',
        'date_of_birth',
        'nalionality',
        'date_of_death',
        'title\u200b\u200b',
        'residence_country\u200b',
        'id_numbers',
        'Nationality',
    ]

    tmp_df = pd.concat(properties_df_dict, axis=0).reset_index().rename(columns={"level_0": "country"}).drop(columns="level_1")
    tmp_df = tmp_df[[col for col in properties_cols if col in tmp_df.columns]]

    tmp_sure_properties_df = tmp_df.map(
        lambda x: [key for key, value in Counter([value.lower() for value in x]).items() if value > 1] if isinstance(x, list) else x
    ).map(
        lambda x: x if isinstance(x, list) and len(x) > 0 else (np.nan if isinstance(x, list) else x)
    )

    col_map = {
        col: col.lower().encode('ascii', "ignore").decode() for col in tmp_df.columns
    }
    col_map["nalionality"] = "nationality"

    for col in set(col_map.values()):
        if col == "country":
            continue

        tmp_col = tmp_df[[_col for _col in tmp_df.columns if col_map[_col] == col]]
        if tmp_col.shape[1] == 1:
            tmp_df[col] = tmp_col
            continue
        
        tmp_col = tmp_col.apply(lambda x: list([_col for _col in x if isinstance(_col, list)]), axis=1).apply(lambda x: list(chain.from_iterable(x))).apply(lambda x: np.nan if len(x) == 0 else x)
        tmp_df[col] = tmp_col

    tmp_df = tmp_df[list(set(col_map.values()))]

    property_columns = [col for col in [
        "country",
        
        "first_name",
        "middle_name",
        "last_name",
        "alias",
        "title",
        
        "nationality",
        "residence_country",
        
        "date_of_birth",
        "date_of_death",
        
        "gender",
        
        # "address",    
    ] if col in tmp_df.columns]

    tmp_df[property_columns].groupby("country").agg("count").T.apply(lambda x: x / properties_df_dict[x.name].shape[0] * 100).plot(kind="bar")
    plt.grid()
    plt.xlabel("Property type")
    plt.ylabel("Percentage")
    plt.title("Properties fill coverage by dataset")
    plt.legend()
    plt.show()

    tmp_sure_properties_df[property_columns].groupby("country").agg("count").T.apply(lambda x: x / properties_df_dict[x.name].shape[0] * 100).plot(kind="bar")
    plt.grid()
    plt.xlabel("Property type")
    plt.ylabel("Percentage")
    plt.title("Properties fill coverage by dataset with 'property must occur more than once' condition")
    plt.legend()
    plt.show()

    (
        - tmp_df[property_columns].groupby("country").agg("count").T.apply(lambda x: x / properties_df_dict[x.name].shape[0] * 100) + \
            tmp_sure_properties_df[property_columns].groupby("country").agg("count").T.apply(lambda x: x / properties_df_dict[x.name].shape[0] * 100)
    ).plot(kind="bar")
    plt.grid()
    plt.xlabel("Property type")
    plt.ylabel("Percentage")
    plt.title("Properties fill drop after adding condition 'property must occur more than once'")
    plt.legend()
    plt.show()

def e2e_positions(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    pd.DataFrame({country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}).map(lambda x: len(x) if isinstance(x, dict) else x).apply(
        lambda x: x.value_counts(normalize=True) * 100
    ).fillna(0).plot()

    # plt.show()

    pd.DataFrame(
        {country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}
    ).map(
        lambda x: len(x) if isinstance(x, dict) else x
    ).melt()["value"].value_counts(normalize=True).apply(lambda x: x*100).sort_index().plot(kind="bar")

    plt.grid()
    plt.ylabel("Percent")
    plt.xlabel("Number of positions")
    plt.title("Number of positions of entities")
    plt.show()

    tmp_df = pd.DataFrame({country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}).map(lambda x: len(x) if isinstance(x, dict) else x).apply(
        lambda x: x.value_counts(normalize=False)
    ).fillna(0).sum(axis=1)
    tmp_df = tmp_df / tmp_df.sum() * 100
    tmp_df.cumsum().plot()
    plt.grid()

    plt.ylabel("Percent cumulated")
    plt.xlabel("Number of positions")
    plt.title("Number of positions of entities")
    plt.show()

    pd.DataFrame(
        {
            "position_start":
                pd.DataFrame(position_held_start_df_dict).sum() / \
                    pd.DataFrame({country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}).map(lambda x: len(x) if isinstance(x, dict) else x).sum() * 100,
            "position_end": pd.DataFrame(position_held_end_df_dict).sum() / \
                    pd.DataFrame({country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}).map(lambda x: len(x) if isinstance(x, dict) else x).sum() * 100
        }
    ).plot(kind="bar")
    plt.xlabel("country")
    plt.ylabel("Answer available [%]")
    plt.title("Timefremes of positions found")
    plt.grid()
    plt.show()

    bins = [0, 1, 3, 5, 10, 15, 20, 30, 99999]

    def analyze_position_timeframes(start=True, first=True):
        col = "start_date" if start else "end_date"
        agg_fun = min if first else max
        
        return pd.DataFrame({country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}).map(
            lambda x: [details.get(col) for position, details in x.items()] if isinstance(x, dict) else np.nan
        ).map(
            lambda x: [start_date for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
        ).map(
            lambda x: list(chain.from_iterable(x)) if isinstance(x, list) else np.nan
        ).map(
            lambda x: [try_convert_to_datetime(start_date) for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
        ).map(
            lambda x: [start_date for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
        ).map(
            lambda x: agg_fun(x) if isinstance(x, list) and len(x) > 0 else np.nan
        ).map(
            lambda x: date.today().year - x.year if isinstance(x, date) else np.nan
        ).apply(
            lambda x: pd.cut(x.values, bins=bins)
        ).apply(
            lambda x: x.value_counts(normalize=True) * 100
        )

    first_position_start_df = analyze_position_timeframes(True, True)
    last_position_start_df = analyze_position_timeframes(True, False)
    first_position_end_df = analyze_position_timeframes(False, True)
    last_position_end_df = analyze_position_timeframes(False, False)

    first_position_start_df.plot(kind="bar")
    plt.grid()
    plt.title("Time since first position started ")
    plt.ylabel("Percentage")
    plt.xlabel("Time delta [years]")
    plt.show()

    last_position_start_df.plot(kind="bar")
    plt.grid()
    plt.ylabel("Percentage")
    plt.xlabel("Time delta [years]")
    plt.title("Time since last position started")
    plt.show()

    first_position_end_df.plot(kind="bar")
    plt.ylabel("Percentage")
    plt.xlabel("Time delta [years]")
    plt.title("Time since first position ended")
    plt.grid()
    plt.show()

    last_position_end_df.plot(kind="bar")
    plt.ylabel("Percentage")
    plt.xlabel("Time delta [years]")
    plt.title("Time since last position ended")
    plt.grid()
    plt.show()

    bins = [0, 5, 10 ,15, 20, 25, 999]
    (
        pd.DataFrame({country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}).map(
            lambda x: [details.get("end_date") for position, details in x.items()] if isinstance(x, dict) else np.nan
        ).map(
            lambda x: [start_date for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
        ).map(
            lambda x: list(chain.from_iterable(x)) if isinstance(x, list) else np.nan
        ).map(
            lambda x: [try_convert_to_datetime(start_date) for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
        ).map(
            lambda x: [start_date for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
        ).map(
            lambda x: max(x) if isinstance(x, list) and len(x) > 0 else np.nan
        ) - \
            pd.DataFrame({country: _df["web_search_properties.positions_held"] for country, _df in df_dict.items()}).map(
                lambda x: [details.get("start_date") for position, details in x.items()] if isinstance(x, dict) else np.nan
            ).map(
                lambda x: [start_date for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
            ).map(
                lambda x: list(chain.from_iterable(x)) if isinstance(x, list) else np.nan
            ).map(
                lambda x: [try_convert_to_datetime(start_date) for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
            ).map(
                lambda x: [start_date for start_date in x if start_date is not None] if isinstance(x, list) else np.nan
            ).map(
                lambda x: min(x) if isinstance(x, list) and len(x) > 0 else np.nan
            )
    ).map(
        lambda x: (x.days / 365) // 1 if isinstance(x, timedelta) else np.nan
    ).apply(
            lambda x: pd.cut(x.values, bins=bins)
        ).apply(
            lambda x: x.value_counts(normalize=True) * 100
        ).plot(kind="bar")
    plt.title("Position held duration time - histogram")
    plt.xlabel("Time delta [years]")
    plt.ylabel("Percentage")
    plt.grid()

def e2e_organizations(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    org_df = pd.concat(
        [
            pd.DataFrame(
                {country: _df["web_search_properties.organizations"]}
            ) for country, _df in df_dict.items() if "web_search_properties.organizations" in _df
        ],
        axis=1
    )
    org_df.map(lambda x: len(x) if isinstance(x, list) else np.nan).apply(
        lambda x: x.value_counts(normalize=True).sort_index() * 100
    ).plot(kind="bar")
    plt.grid()
    plt.xlabel("number of orgqanizations")
    plt.title("Number of organizations per entity")
    plt.show()
    
def e2e_addresses(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    address_df = pd.concat(
        [
            pd.DataFrame(
                {country: _df["web_search_properties.addresses"]}
            ) for country, _df in df_dict.items() if "web_search_properties.addresses" in _df
        ],
        axis=1
    )
    address_df.map(lambda x: len(x) if isinstance(x, list) else np.nan).apply(
        lambda x: x.value_counts(normalize=True).sort_index() * 100
    ).plot(kind="bar")
    plt.grid()
    plt.xlabel("number of addresses")
    plt.title("Number of addresses per entity")
    plt.show()


def e2e_analysis(
    df_dict,
    relatives_df_dict,
    associates_df_dict,
    properties_df_dict,
    position_held_start_df_dict,
    position_held_end_df_dict,
    enrichment_sources_df_dict,
):
    e2e_input = [
        df_dict,
        relatives_df_dict,
        associates_df_dict,
        properties_df_dict,
        position_held_start_df_dict,
        position_held_end_df_dict,
        enrichment_sources_df_dict,
    ]

    e2e_entities(*e2e_input)
    e2e_sources(*e2e_input)
    e2e_associates(*e2e_input)
    e2e_relatives(*e2e_input)
    e2e_properties(*e2e_input)
    e2e_positions(*e2e_input)
    e2e_organizations(*e2e_input)
    e2e_addresses(*e2e_input)
