import pandas as pd
from os import path
import numpy as np
import requests
import json

dir = path.dirname(__file__)

meps_df = pd.read_csv(path.join(dir, "..", "data", "merged.csv"), sep = ";")

# Geocode remaining names
geonames_df = pd.read_csv(path.join(dir, "..", "data", "geonames.csv"), sep = ";")
geonames_df["Name"] = geonames_df["Name"].str.lower()
geonames_df["Alternate Names"] = geonames_df["Alternate Names"].str.lower()
geonames_df = geonames_df.sort_values("Population", ascending = False)
alt_geonames_df = geonames_df.loc[geonames_df["Alternate Names"].notna()]

def get_coordinates(place_raw):
    place = str(place_raw).lower()
    if place != "nan":
        for sign in ["(", "/", "-", ","]:
            place = place.split(sign)[0].strip()
        filter_df = geonames_df.loc[geonames_df["Name"] == place]
        if len(filter_df.index) > 0:
            coordinates = filter_df["Coordinates"].tolist()[0]
            return coordinates
        elif geonames_df["Alternate Names"].str.contains(place, na = False).any():
            alt_coordinates_df = alt_geonames_df.loc[alt_geonames_df["Alternate Names"].str.contains(place)]
            if len(alt_coordinates_df.index) > 0:
                coordinates = alt_coordinates_df["Coordinates"].tolist()[0]
                return coordinates
            else:
                print(place_raw, place)
        # place names not being recognised
        return np.nan
        
uncoded_df = meps_df.loc[~meps_df["born_lat"].notna()]
uncoded_df["coordinates"] = uncoded_df["born_place"].apply(lambda x: get_coordinates(x))
uncoded_df = uncoded_df.loc[uncoded_df["coordinates"].notna()]
uncoded_df[["born_lat", "born_lat"]] = uncoded_df["coordinates"].str.split(", ", expand = True)
uncoded_df = uncoded_df.drop(["coordinates"], axis = 1)

# Merge
meps_df = pd.concat([meps_df, uncoded_df])
meps_df = meps_df[~meps_df["identifier"].duplicated(keep = "last")]

# Now get everything classified
def get_classification(lat, lon, elected_country):
    if str(lat) == "nan":
        return np.nan
    coordinates = str(lat) + "," + str(lon)
    # If no data on birth place, return nan
    key = open(path.join(dir, "..", "opencagekey.txt"), "r").read()
    url = "https://api.opencagedata.com/geocode/v1/json?q=" + coordinates + "&key=" + key + "&proximity=50.0594725,14.1538226"
    response = requests.get(url)
    response_dict = json.loads(response.content)
    response_df = pd.json_normalize(response_dict["results"])
    if "components.ISO_3166-1_alpha-2" in response_df.columns:
        born_country = response_df["components.ISO_3166-1_alpha-2"].values[0]
        eu_country_codes = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
                        "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL",
                        "PT", "RO", "SE", "SI", "SK"]
        if born_country == np.nan:
            return np.nan
        elif born_country == elected_country:
            return "native"
        elif born_country in eu_country_codes:
            return "eu"
        return "other"
    return np.nan

meps_df["born_region"] = meps_df.apply(lambda x: get_classification(x["born_lat"], x["born_lon"], x["country"]), axis = 1)

# Save
meps_df.to_csv(path.join(dir, "..", "data", "output" + ".csv"), sep =  ";", encoding = "utf-8", index = False)