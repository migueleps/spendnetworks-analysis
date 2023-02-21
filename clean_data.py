import warnings
import pandas as pd
import numpy as np
import sys

#Takes the merged.csv file and returns a cleaned, processed csv

warnings.filterwarnings("ignore")

CSV_dir = sys.argv[1] + "csv_data/"
DICT = "ocds_orgs.csv"  # location of the dictionary with cpvs

df = pd.read_csv(CSV_dir+"merged.csv")

df.drop(df.columns[0], axis=1, inplace=True)

print(f"Total number of notices seen: {len(df)}")

# filter only valid suppliers (!=null)
df = df[~df["supplier"].isna()]
print(f"Number of notices with supplier = {len(df)}")

df = df[~df.buyer.isna()]
print(f"Number of notices with supplier AND buyer: {len(df)}")


# read the dictionary of organization names
dictionary = pd.read_csv(DICT, error_bad_lines=False).dropna()
# keep only org_string and legal_name
dictionary = dictionary[["org_string", "legal_name"]]
print("The shape of the dictionary is " + str(dictionary.shape[0]) + "x" + str(dictionary.shape[1]))
dictionary.sort_values(by=['org_string'])


changes = {}

unique_buyers = df["buyer"].unique()
unique_suppliers = df["supplier"].unique()
entities = sorted(np.union1d(unique_buyers, unique_suppliers))

changed_org = 0
changed_upper = 0
for i, ent in enumerate(entities):
    ent_string = ent.upper().strip()
    dict_entry = dictionary[dictionary["org_string"] == ent_string]
    if dict_entry.shape[0] != 0:
        changes[ent] = str(dict_entry["legal_name"].values[0]).upper().strip()
        changed_org += 1
    else:
        changed_upper += 1
        changes[ent] = ent_string

    if i % 1000 == 0:
        print(f"{i}/{len(entities)}")


print(f"Changed by looking at dict: {changed_org}")
print(f"Changed otherwise: {changed_upper}")

df["supplier"].replace(changes, inplace=True)
df["buyer"].replace(changes, inplace=True)

df.to_csv(CSV_dir + "preeditdist_processed.csv")
