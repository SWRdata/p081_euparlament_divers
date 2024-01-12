from bs4 import BeautifulSoup
import requests
import numpy as np
import pandas as pd
from os import path

dir = path.dirname(__file__)

# Construct urls for MEP profile pages
meps_df = pd.read_csv(path.join(dir, "..", "data/start.csv"), sep =  ";")
mep_urls = []
for mep in meps_df.index:
    identifier = str(meps_df["identifier"][mep])
    given_name = str(meps_df["givenName"][mep])
    family_name = str(meps_df["familyName"][mep])
    url = "https://www.europarl.europa.eu/meps/en/" + identifier + "/" + given_name + "_" + family_name + "/home"
    mep_urls.append([identifier, url])

# Scrape each profile
def scrape_mep(url):
    response = requests.get(url)
    html = response.text
    doc = BeautifulSoup(html, "html.parser")
    mep_dict = {}

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
        mep_dict[title] = ""
        badges = status.findAll("a", {"class": "erpl_badge"})
        for badge in badges:
            if len(str(mep_dict[title])) != 0:
                mep_dict[title] += ","
            mep_dict[title] += badge.text
            if len(str(mep_dict["memberships"])) != 0:
                mep_dict["memberships"] += ","
            mep_dict["memberships"] += badge.text
    return mep_dict

# Put everything into one big dictionary, convert to dataframe and save
dict_of_dicts = {}
for identifier, url in mep_urls:
    dict_of_dicts[identifier] = scrape_mep(url)
scraped_df = pd.DataFrame.from_dict(dict_of_dicts).transpose().reset_index().rename(columns = {"index": "identifier"})

scraped_df.to_csv(path.join(dir, "..", "data", "scraped" + ".csv"), sep =  ";", encoding = "utf-8", index = False)