import requests
import json
import pandas as pd
from os import path, makedirs

dir = path.dirname(__file__)

def querying(identifier):
    query_result = requests.get("https://data.europarl.europa.eu/person/" + str(identifier), headers = {"Accept": "application/ld+json"})
    mep_dict = json.loads(query_result.content)
    mep_df = pd.json_normalize(mep_dict["@graph"])
    mep_df = mep_df.rename(columns = {"label": "name"})
    gender = str(mep_df["hasGender"].dropna().values[0]).split("/")[-1]
    return gender

meps_df = pd.read_csv(path.join(dir, "..", "data/start.csv"), sep =  ";")
mep_identifiers = meps_df["identifier"].tolist()
mep_details_df = pd.DataFrame(mep_identifiers)
mep_details_df = mep_details_df.rename(columns = {0: "identifier"})
mep_details_df["gender"] = mep_details_df["identifier"].apply(querying)
mep_details_df.to_csv(path.join(dir, "..", "data", "details" + ".csv"), sep =  ";", encoding = "utf-8", index = False)