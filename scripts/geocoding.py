import pandas as pd
from os import path

dir = path.dirname(__file__)

meps_df = pd.read_csv(path.join(dir, "..", "data", "merged.csv"), sep = ";")
geonames_df = pd.read_csv(path.join(dir, "..", "data", "geonames.csv"), sep = ";")
geonames_df["Name"] = geonames_df["Name"].str.lower()
geonames_df = geonames_df.sort_values("Population", ascending = False)
geonames_df["Alternate Names"] = geonames_df["Alternate Names"].str.lower()
alt_geonames_df = geonames_df.loc[geonames_df["Alternate Names"].notna()]

def born_classifier(place_raw, home_country_code):
    eu_country_codes = ["AT", "BE", "BG", "CY", "CZ", "DE", "DK", "EE", "ES", "FI",
                       "FR", "GR", "HR", "HU", "IE", "IT", "LT", "LU", "LV", "MT", "NL", "PL",
                       "PT", "RO", "SE", "SI", "SK"]
    other_country_codes = eu_country_codes
    other_country_codes.remove(home_country_code)
    # clean place name
    place = str(place_raw).lower()
    if place != "nan":
        for sign in ["(", "/", "-", ","]:
            place = place.split(sign)[0].strip()
        country_codes = geonames_df.loc[geonames_df["Name"] == place]["Country Code"].values
        if len(country_codes) > 0:
            if country_codes[0] == home_country_code:
                return "native"
            elif country_codes[0] in other_country_codes:
                return "eu"
            elif geonames_df["Alternate Names"].str.contains(place, na = False).any():
                alt_country_codes = alt_geonames_df.loc[alt_geonames_df["Alternate Names"].str.contains(place)]["Country Code"].values
                if len(alt_country_codes) > 0:
                    if alt_country_codes[0] == home_country_code:
                        return "native"
                    elif alt_country_codes[0] in other_country_codes:
                        return "eu"
                    else:
                        return "other"
        elif geonames_df["Alternate Names"].str.contains(place, na = False).any():
            alt_country_codes = alt_geonames_df.loc[alt_geonames_df["Alternate Names"].str.contains(place)]["Country Code"].values
            if len(alt_country_codes) > 0:
                if alt_country_codes[0] == home_country_code:
                    return "native"
                elif alt_country_codes[0] in other_country_codes:
                    return "eu"
                else:
                    return "other"
            else:
                print(place_raw, place)
        # place names not being recognised
        else:
           return "not recognised"
        
meps_df["born_region"] = meps_df.apply(lambda x: born_classifier(x.born_place, x.country), axis = 1)

# Save
meps_df.to_csv(path.join(dir, "..", "data", "output" + ".csv"), sep =  ";", encoding = "utf-8", index = False)