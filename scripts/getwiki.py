import pandas as pd
import requests
import json
from os import path
from wikidata.client import Client
import numpy as np

dir = path.dirname(__file__)

# Degree & occupation dictionaries
degree_dict = {
    "secondary": ["secondary", "gymnasium"],
    "vocational": ["vocat", "apprentice"],
    "university": ["master", "bachelor", "diplom", "magister", "laurea", "degree", "law"],
    "phd": ["doctor", "habilitation", "professor", "candidat"]
}

occupation_dict = {
    "politician": ["politician", "member of the european parliament"],
    "lawyer": ["lawyer", "judge", "jurist", "justiciar", "legal", "barrister", "officer of the court"],
    "professor": ["professor", "university teacher", "academic"],
    "engineer": ["engineer"],
    "farmer": ["farmer", "farm operator", "rancher", "vigneron"],
    "consultant": ["consultant"],
    "researcher": ["researcher", "scientist", "historian", "sociologist", "chemist", 
                   "physicist", "agronomist", "pharmacist", "philologist", "scientist", "mathematician",
                   "psychiatrist", "psychologist", "pedagogue", "economist", "ecologist", "geographer", "philosopher"],
    "journalist": ["presenter", "journalist", "press", "blogger", "correspondent"],
    "activist": ["environmentalist", "activist", "dissident", "humanitarian"],
    "athlete": ["athlete", "football", "hurler", "swimmer", "athletics", "badminton player"],
    "official": ["civil servant", "minister", "official", "eurocrat", "magistrate"],
    "teacher": ["teacher"],
    "actor": ["actor"],
    "manager": ["manager", "executive"],
    "businessperson": ["businessperson", "entrepeneur", "shopkeeper", "self-employment"],
    "doctor": ["doctor", "nurse", "physician", "veterinarian", "pharmacist", "surgeon"],
}

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

# Categorise degrees & occupations
def categorise(entry, dict):
    if not entry:
        return np.nan
    new_entry = []
    entry_list = entry.split(",")
    for entry_part in entry_list:
        entry_part = entry_part.lower().strip()
        found = False
        for occupation_category in dict.keys():
            for categorised_occupation in dict[occupation_category]:
                if categorised_occupation in entry_part:
                    found = True
                    if occupation_category not in new_entry:
                        new_entry.append(occupation_category)
        if not found:
            new_entry.append(entry_part)
    new_entry = ",".join(new_entry)
    return new_entry

merged_meps_df["degrees"] = merged_meps_df["degrees"].apply(lambda x: categorise(x, degree_dict))
merged_meps_df["occupation"] = merged_meps_df["occupation"].apply(lambda x: categorise(x, occupation_dict))

# Simplify university names
#def simplify(universities_string):
#    new_universities_list = []
#    universities_list = universities_string.split(",")
#    for university_string in universities_list:
#        print(university_string)
#        if "University of" in university_string:
#            new_string = university_string.split("University of")[1]
#            new_string = new_string.strip()
#            print(new_string)
#            new_universities_list.append(new_string)
#        else:
#            new_universities_list.append(university_string)
#    new_universities_string = ",".join(new_universities_list)
#    return new_universities_string
#merged_meps_df["educated_at"] = merged_meps_df["educated_at"].apply(lambda x: simplify(x))

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
    "Yannis Lagos": "Ioannis Lagos",
    "Cristian Terheș": "Cristian TERHEŞ"
})

# Save
merged_meps_df.to_csv(path.join(dir, "..", "data", "wikidata" + ".csv"), sep =  ";", encoding = "utf-8", index = False)