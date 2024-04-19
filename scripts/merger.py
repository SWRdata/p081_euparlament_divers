import pandas as pd
import numpy as np
from os import path

dir = path.dirname(__file__)

# Load all dataframes
start_df = pd.read_csv(path.join(dir, "..", "data/start.csv"), sep = ";")
details_df = pd.read_csv(path.join(dir, "..", "data/details.csv"), sep = ";")
scraped_df = pd.read_csv(path.join(dir, "..", "data/scraped.csv"), sep = ";")
wikidata_df = pd.read_csv(path.join(dir, "..", "data/wikidata.csv"), sep = ";")
disability_df = pd.read_csv(path.join(dir, "..", "data", "disability.csv"), sep = ";")

# First merge simple ones
first_merge_df = pd.merge(start_df, details_df, on = "identifier", how = "left")
second_merge_df = pd.merge(first_merge_df, scraped_df, on = "identifier", how = "left")

# Prepare names to be merged on
second_merge_df["name"] = second_merge_df["name"].str.lower().str.strip()
wikidata_df["name"] = wikidata_df["name"].str.lower().str.strip()

# Do the complicated fill-merges for birthplace & -date
wikidata_place_df = wikidata_df[["name", "born_place"]]
place_missing = second_merge_df.loc[second_merge_df["born_place"].isna()]["name"].tolist()
place_in_df = second_merge_df.loc[second_merge_df["born_place"].notna()][["name", "born_place"]]
wikidata_place_df = wikidata_place_df.loc[wikidata_place_df["name"].isin(place_missing)]
place_filled_df = pd.concat([place_in_df, wikidata_place_df], ignore_index = True)

wikidata_date_df = wikidata_df[["name", "born_day", "born_month", "born_year"]]
date_missing = second_merge_df.loc[second_merge_df["born_year"].isna()]["name"].tolist()
date_in_df = second_merge_df.loc[second_merge_df["born_year"].notna()][["name", "born_day", "born_month", "born_year"]]
wikidata_date_df = wikidata_date_df.loc[wikidata_date_df["name"].isin(date_missing)]
date_filled_df = pd.concat([date_in_df, wikidata_date_df], ignore_index = True)

# Rest

wikidata_rest_df = wikidata_df[["name", "born_lat", "born_lon", "relatives", "degrees", "educated_at", "occupation"]]
second_merge_df = second_merge_df.drop(columns = ["born_day", "born_month", "born_year", "born_place"])

third_merge_df = pd.merge(second_merge_df, place_filled_df, on = "name", how = "left")
fourth_merge_df = pd.merge(third_merge_df, date_filled_df, on = "name", how = "left")
fifth_merge_df = pd.merge(fourth_merge_df, disability_df, on = "identifier", how = "left")
merged_df = pd.merge(fifth_merge_df, wikidata_rest_df, on = "name", how = "left")

# Merge degrees and occupation columns
merged_df = merged_df.fillna("")
merged_df["degrees"] = merged_df.apply(lambda row: ",".join(set(row["degrees_x"].split(",") + row["degrees_y"].split(","))), axis = 1)
merged_df["occupation"] = merged_df.apply(lambda row: ",".join(set(row["occupation_x"].split(",") + row["occupation_y"].split(","))), axis = 1)
merged_df["degrees"] = merged_df["degrees"].apply(lambda x: x.strip(","))
merged_df["occupation"] = merged_df["occupation"].apply(lambda x: x.strip(","))
merged_df["degrees"] = merged_df["degrees"].apply(lambda x: x.replace(",,", ","))
merged_df["occupation"] = merged_df["occupation"].apply(lambda x: x.replace(",,", ","))
merged_df = merged_df.replace("", np.nan)

# Only keep the highest degree
def keep_highest_degree(degree_string):
    degree_string = str(degree_string)
    if degree_string != "nan":
        degree_hierarchy = ["phd", "university", "secondary", "vocational"]
        degree_list = degree_string.split(",")
        for degree in degree_hierarchy:
            if degree in degree_list:
                return degree
    return np.nan
merged_df["degrees"] = merged_df["degrees"].apply(lambda x: keep_highest_degree(x))
merged_df = merged_df.rename(columns = {"degrees": "highest_degree"})

# Drop all unwanted columns
merged_df = merged_df.drop(columns = ["id", "type", "sortLabel", "officialFamilyName", "officialGivenName",
                                      "degrees_x", "degrees_y", "occupation_x", "occupation_y"])

# Drop duplicate rows
merged_df = merged_df[~merged_df["identifier"].duplicated()]

# Convert dates to int
for column in ["born_day", "born_month", "born_year"]:
    merged_df[column] = merged_df[column].fillna(0).astype(int)
    merged_df[column] = merged_df[column].replace(0, np.nan)

# Save
merged_df.to_csv(path.join(dir, "..", "data", "merged" + ".csv"), sep =  ";", encoding = "utf-8", index = False)