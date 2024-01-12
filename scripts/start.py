import requests
import json
import pandas as pd
from os import path, makedirs

dir = path.dirname(__file__)

query_result = requests.get("https://data.europarl.europa.eu/api/v1/meps/show-current", headers = {"Accept": "application/ld+json"})
meps_dict = json.loads(query_result.content)
meps_df = pd.json_normalize(meps_dict["data"])
meps_df = meps_df.rename(columns = {"label": "name", "api:country-of-representation": "country", "api:political-group": "group"})
meps_df.to_csv(path.join(dir, "..", "data", "start" + ".csv"), sep =  ";", encoding = "utf-8", index = False)