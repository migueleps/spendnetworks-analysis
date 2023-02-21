import os
import json
import warnings
import pandas as pd
import sys
import re

warnings.filterwarnings("ignore")

JSON_dir = sys.argv[1] + "json_data/"
CSV_dir = sys.argv[1] + "csv_data/"

if not os.path.isdir(CSV_dir):
    os.mkdir(CSV_dir)

csv = pd.DataFrame(columns=["ocds", "award_supp_id", "buyer", "supplier", "award_date",
                            "award_description", "tender_value", "buyer_country", "supplier_country",
                            "procurement_method", "source"])

csv.to_csv(CSV_dir + "merged_source.csv")


no_date_sources = ['cn_eu_supply', 'cn_eu_supply_ie', 'td_dm_gov_uk', 'cn_due_north_uk', 'cn_millstream',
                   'find_a_tender', 'cn_bluelight_uk', 'cn_westminster_uk', 'cn_bund_de', 'cn_eu_supply_eu']


for file_name in sorted(os.listdir(JSON_dir)):
    print(file_name, end=" ")
    if file_name == ".DS_Store":
        continue

    with open(JSON_dir + str(file_name)) as json_file:
        current_json = json.load(json_file)
        current_csv = pd.DataFrame(columns=csv.columns)

        ocds = []
        ids = []
        buyers = []
        suppliers = []
        award_dates = []
        award_descriptions = []
        tender_values = []
        buyer_countries = []
        supplier_countries = []
        procurement_methods = []
        sources = []

        for item in current_json:

            tender_value = item["json"]["releases"][0]["tender"].get("value",{}).get("amount")
            procurement_method = item["json"]["releases"][0]["tender"].get("procurementMethod")
            ocid = item["ocid"]
            source = item["source"]
            buyer = item["json"]["releases"][0]["buyer"].get("name")
            buyer_country = item["json"]["releases"][0]["buyer"].get("address",{}).get("countryName")

            releasedate = item["releasedate"].split("T")[0]

            for award in item["json"]["releases"][0].get("awards",[]):
                award_date = award.get("date")
                try:
                    award_date = award_date.split("T")[0]
                except:
                    if award_date is not None:
                        print(award_date)
                    award_date = None

                if source in no_date_sources:
                    award_date = releasedate

                award_value = award.get("value", {}).get("amount")
                if award_value is None:
                    award_value = tender_value

                award_description = re.sub(r'[^A-Za-z0-9 ]+', '', award.get("description",""))

                for supp in award.get("suppliers", []):
                    supplier = supp.get("name")
                    supplier_country = supp.get("address", {}).get("countryName")
                    award_supp_id = supp.get("id")
                    if award_supp_id is None:
                        award_supp_id = award.get("id")

                    ocds.append(ocid)
                    ids.append(award_supp_id)
                    buyers.append(buyer)
                    suppliers.append(supplier)
                    award_dates.append(award_date)
                    award_descriptions.append(award_description)
                    tender_values.append(award_value)
                    buyer_countries.append(buyer_country)
                    supplier_countries.append(supplier_country)
                    procurement_methods.append(procurement_method)
                    sources.append(source)

        current_csv.ocds = ocds
        current_csv.award_supp_id = ids
        current_csv.buyer = buyers
        current_csv.supplier = suppliers
        current_csv.award_date = award_dates
        current_csv.award_description = award_descriptions
        current_csv.tender_value = tender_values
        current_csv.buyer_country = buyer_countries
        current_csv.supplier_country = supplier_countries
        current_csv.procurement_method = procurement_methods
        current_csv.source = sources

        current_csv.to_csv(CSV_dir + str(file_name.split(".")[0]) + ".csv")
        current_csv.to_csv(CSV_dir + "merged.csv", mode="a", header=False)
    