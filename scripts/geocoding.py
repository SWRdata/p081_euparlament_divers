import requests
import json
import pandas as pd
import numpy as np
from os import path

dir = path.dirname(__file__)

def geocode(born_place):
    born_place = str(born_place)
    # If no data on birth place, return nan
    if born_place == "nan":
        return pd.Series([np.nan, np.nan, np.nan])
    print(born_place)
    key = open(path.join(dir, "..", "opencagekey.txt"), "r").read()
    url = "https://api.opencagedata.com/geocode/v1/json?q=" + born_place + "&key=" + key + "&proximity=50.0594725,14.1538226"
    response = requests.get(url)
    geocoded_dict = json.loads(response.content)
    geocoded_df = pd.json_normalize(geocoded_dict["results"])
    if "confidence" in geocoded_df.columns:
        geocoded_df = geocoded_df.sort_values(by = "confidence", ascending = False)
    # Exclude establishments named after places
    if "components.house_number" in geocoded_df.columns:
        geocoded_df = geocoded_df.loc[geocoded_df["components.house_number"].isna()].reset_index()
    # If there are no results left, return nan
    print(born_place, geocoded_df.columns)
    if len(geocoded_df.index) == 0:
        return pd.Series([np.nan, np.nan, np.nan])
    if "components.ISO_3166-1_alpha-2" in geocoded_df.columns:
        born_country = geocoded_df["components.ISO_3166-1_alpha-2"][0]
    else:
        born_country = np.nan
    if "annotations.DMS.lat" in geocoded_df.columns:
        born_lat = geocoded_df["annotations.DMS.lat"][0]
    else:
        born_lat = np.nan
    if "annotations.DMS.lng" in geocoded_df.columns:
        born_lng = geocoded_df["annotations.DMS.lng"][0]
    else:
        born_lng = np.nan
    return pd.Series([born_country, born_lat, born_lng])

def geoclassify(born_country, country):
    eu_country_codes = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
                       "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL",
                       "PT", "RO", "SE", "SI", "SK"]
    if born_country == np.nan:
        return np.nan
    if born_country == country:
        return "native"
    if born_country in eu_country_codes:
        return "eu"
    return "other"

meps_df = pd.read_csv(path.join(dir, "..", "data", "merged.csv"), sep = ";")
meps_df[["born_country", "born_lat", "born_lng"]] = meps_df.apply(lambda row: geocode(row.born_place), axis = 1)
meps_df["born_region"] = meps_df.apply(lambda row: geoclassify(row.born_country, row.country), axis = 1)

# Save
meps_df.to_csv(path.join(dir, "..", "data", "output" + ".csv"), sep =  ";", encoding = "utf-8", index = False)