import pandas as pd
import requests
import json
from os import path
from wikidata.client import Client

dir = path.dirname(__file__)

# One big SPARQL query
query = '''SELECT ?mep ?mepLabel ?fatherLabel ?motherLabel ?birthdateLabel ?birthplaceLabel ?relativeLabel ?degreeLabel ?educatedatLabel ?occupationLabel
WHERE { 
  ?mep p:P39 ?position. 
  ?position (ps:P39/(wdt:P279*)) wd:Q27169. 
  ?position pq:P2937 ?term. 
  FILTER(?term = wd:Q64038205).  
  OPTIONAL{ ?mep wdt:P22 ?father. }
  OPTIONAL{ ?mep wdt:P25 ?mother. }
  OPTIONAL{ ?mep wdt:P569 ?birthdate. }
  OPTIONAL{ ?mep wdt:P19 ?birthplace. }
  OPTIONAL{ ?mep wdt:P1038 ?relative. }
  OPTIONAL{ ?mep wdt:P512 ?degree. }
  OPTIONAL{ ?mep wdt:P69 ?educatedat. }
  OPTIONAL{ ?mep wdt:P106 ?occupation. }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}'''
wikidata_url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"
query_result = requests.get(wikidata_url, params = {"query": query, "format": "json"})
meps_dict = json.loads(query_result.content)
meps_df = pd.json_normalize(meps_dict["results"]["bindings"])
meps_df = meps_df.fillna("")
# Group the rows of MEPs who have several relatives, degrees, educations or occupations
merged_meps_df = meps_df.groupby(["mep.value", "mepLabel.value", "fatherLabel.value", "motherLabel.value", 
                                  "birthdateLabel.value", "birthplaceLabel.value"]).agg({
    "relativeLabel.value": lambda x: ",".join(list(set(x.astype(str)))),
    "degreeLabel.value": lambda x: ",".join(list(set(x.astype(str)))),
    "educatedatLabel.value": lambda x: ",".join(list(set(x.astype(str)))),
    "occupationLabel.value": lambda x: ",".join(list(set(x.astype(str)))),
}).reset_index()

# Rename columns
merged_meps_df = merged_meps_df.rename(columns = {
    "mepLabel.value": "name",
    "fatherLabel.value": "father",
    "motherLabel.value": "mother",
    "birthdateLabel.value": "born_date",
    "birthplaceLabel.value": "born_place",
    "relativeLabel.value": "relatives",
    "degreeLabel.value": "degrees",
    "educatedatLabel.value": "educated_at",
    "occupationLabel.value": "occupation"
})

# Split born_date column
merged_meps_df["born_day"] = merged_meps_df["born_date"]
merged_meps_df["born_day"] = merged_meps_df["born_day"].str.split("-").str.get(2).str[:2]
merged_meps_df["born_month"] = merged_meps_df["born_date"]
merged_meps_df["born_month"] = merged_meps_df["born_month"].str.split("-").str.get(1)
merged_meps_df["born_year"] = merged_meps_df["born_date"]
merged_meps_df["born_year"] = merged_meps_df["born_year"].str.split("-").str.get(0)
merged_meps_df = merged_meps_df.drop(columns = ["mep.value", "born_date"])

# Group father, mother and relative columns
def join_strings(row, columns):
    return ",".join(value for column, value in zip(merged_meps_df.columns, row) if column in columns and pd.notna(value) and value != "")
merged_meps_df["relatives"] = merged_meps_df.apply(lambda row: join_strings(row, ["father", "mother", "relatives"]), axis = 1)

# Manual name overrides if Wikidata name not identical to Parliament database
merged_meps_df["name"] = merged_meps_df["name"].replace({
    "Rosa Estaràs": "Rosa ESTARÀS FERRAGUT",
    "Tomasz Poręba": "Tomasz Piotr PORĘBA",
    "Carles Puigdemont": "Carles Puigdemont i Casamajó",
    "Soraya Rodríguez": "María Soraya RODRÍGUEZ RAMOS",
    "Petros S. Kokkalis": "Petros KOKKALIS",
    "Graça Carvalho": "Maria da Graça CARVALHO",
    "Diana Riba Giner": "Diana RIBA I GINER",
    "Eva-Maria Poptcheva": "Eva Maria Poptcheva",
    "Yannis Lagos": "Ioannis Lagos"
})

# Save
merged_meps_df.to_csv(path.join(dir, "..", "data", "wikidata" + ".csv"), sep =  ";", encoding = "utf-8", index = False)