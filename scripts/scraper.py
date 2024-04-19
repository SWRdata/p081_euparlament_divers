from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
from os import path

dir = path.dirname(__file__)

# Define dictionaries for degrees and careers
degree_dict = {
    "secondary": ["secondary", "gymnasium", "vocat", "apprentice"],
    "university": ["university", "college", "degree", "diplom", "bachelor", "master", "graduate", "studied", "bsc", "msc", " ba ", " ma "],
    "phd": ["doctor", "postgraduate", "phd", "ph.d"]
}

career_dict = {
    "farmer": ["farmer"],
    "lawyer": ["lawyer", "legal", "judge", "associate"],
    "journalist": ["journalist", "reporter", "editor", "newsroom"],
    "politician": ["mep", "party", "parliament", "policy officer", "mayor"],
    "official": ["official", "ministry", "public service"],
    "diplomat": ["diplomat"],
    "doctor": ["doctor"],
    "teacher": ["teacher", "instructor"],
    "professor": ["lecturer", "professor"],
    "engineer": ["engineer"],
    "researcher": ["researcher", "expert"],
    "manager": ["manager", "director", "head of", "member of the board", "chair of the"],
    "consultant": ["consultant"],
    "marketeer": ["press", "advertis"]
}

# Construct urls for MEP profile pages
meps_df = pd.read_csv(path.join(dir, "..", "data/start.csv"), sep =  ";")
mep_urls = []
for mep in meps_df.index:
    identifier = str(meps_df["identifier"][mep])
    given_name = str(meps_df["givenName"][mep])
    family_name = str(meps_df["familyName"][mep])
    url = "https://www.europarl.europa.eu/meps/en/" + identifier + "/" + given_name + "_" + family_name
    mep_urls.append([identifier, url])

# Scrape each profile
def scrape_mep(url):
    mep_dict = {}

    # Main elements
    response = requests.get(url + "/home")
    html = response.text
    doc = BeautifulSoup(html, "html.parser")

    # Birth date
    try:
        birthdate = doc.find("time", {"class": "sln-birth-date"})
        birthdate = birthdate.text.strip().split("-")
        mep_dict["born_day"] = int(birthdate[0])
        mep_dict["born_month"] = int(birthdate[1])
        mep_dict["born_year"] = int(birthdate[2])
    except:
        mep_dict["born_day"] = np.nan
        mep_dict["born_month"] = np.nan
        mep_dict["born_year"] = np.nan

    # Birth place
    try:
        birthplace = doc.find("span", {"class": "sln-birth-place"})
        mep_dict["born_place"] = birthplace.text
    except:
        mep_dict["born_place"] = np.nan

    # Memberships
    mep_dict["memberships"] = np.nan
    status_list = doc.findAll("div", {"class": "erpl_meps-status"})
    for status in status_list:
        title = status.find("h4", {"class": "erpl_title-h4"}).text
        badges = status.findAll("a", {"class": "erpl_badge"})
        for badge in badges:
            if not pd.isna(mep_dict["memberships"]):
                mep_dict["memberships"] += ","
                mep_dict["memberships"] += badge.text
            else:
                mep_dict["memberships"] = badge.text

    # CV
    headers = {"Accept-Language": "en;q=1.0"}
    response = requests.get(url + "/cv", headers = headers)
    html = response.text
    doc = BeautifulSoup(html, "html.parser")
            
    # Potentially also try to find education institutions here
    #mep_dict["educated_at"]
    mep_dict["degrees"] = np.nan
    mep_dict["occupation"] = np.nan

    activity_list = doc.findAll("div", {"class": "erpl_meps-activity"})
    for activity in activity_list:
        category = activity.find("h4", {"class": "erpl_title-h4"}).text
        activity_content = activity.find("ul", {"class": "pl-2"})
        if category == "Education (qualifications and diplomas)":
            education_str = activity_content.text.strip().lower()
            # Potentially also try to find education institutions here
            for key in degree_dict.keys(): 
                add = False
                for word in degree_dict[key]:
                    if word in education_str:
                        add = True
                if add == True:
                    if not pd.isna(mep_dict["degrees"]):
                        mep_dict["degrees"] += ","
                        mep_dict["degrees"] += key
                    else:
                        mep_dict["degrees"] = key                  
        if category == "Professional career":
            career_str = activity_content.text.strip().lower()
            for key in career_dict.keys(): 
                add = False
                for word in career_dict[key]:
                    if word in career_str:
                        add = True
                if add == True:
                    if not pd.isna(mep_dict["occupation"]):
                        mep_dict["occupation"] += ","
                        mep_dict["occupation"] += key
                    else:
                        mep_dict["occupation"] = key

    # Done, yay
    return mep_dict

# Put everything into one big dictionary, convert to dataframe and save
dict_of_dicts = {}
for identifier, url in mep_urls:
    dict_of_dicts[identifier] = scrape_mep(url)
scraped_df = pd.DataFrame.from_dict(dict_of_dicts).transpose().reset_index().rename(columns = {"index": "identifier"})
scraped_df.to_csv(path.join(dir, "..", "data", "scraped" + ".csv"), sep =  ";", encoding = "utf-8", index = False)