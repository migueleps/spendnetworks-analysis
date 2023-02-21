import pandas as pd
import sys
import csv

# Takes the merged.csv file and returns a cleaned, processed csv

CSV_dir = sys.argv[1] + "csv_data/"
DICT = "swap_dict_dists.csv"


# read dataset
df = pd.read_csv(CSV_dir+"preeditdist_processed.csv")
df = df[(~df.supplier.isna()) & (~df.buyer.isna())]
# drop the first column (index column)
df.drop(df.columns[0], axis=1, inplace=True)

replace_dict = {}

with open(DICT, "r") as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        if row[0] == row[1]:
            continue
        replace_dict[row[0]] = row[1]


while len(df[df.buyer.isin(replace_dict.keys())]) > 0:
    print(f"Trying to replace {len(df[df.buyer.isin(replace_dict.keys())])} buyers")
    df.buyer.replace(replace_dict, inplace=True)

while len(df[df.supplier.isin(replace_dict.keys())]) > 0:
    print(f"Trying to replace {len(df[df.supplier.isin(replace_dict.keys())])} suppliers")
    df.supplier.replace(replace_dict, inplace=True)

df.to_csv(CSV_dir + "processed.csv")
