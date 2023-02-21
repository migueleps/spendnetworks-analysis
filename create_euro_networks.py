import numpy as np
import pandas as pd
import networkx as nx
import datetime as dt
import os
import re


def add_month(date, delta):
    y = int(date.split("-")[0])
    m = int(date.split("-")[1])
    d = date.split("-")[2]
    if m + delta > 12:
        return f"{y+1}-{((m+delta)%12):02d}-{d}"
    else:
        return f"{y}-{m+delta:02d}-{d}"


def gen_dates_month_rolling(delta, rolling_delta, start_date = "2014-01-01"):
    cur_date = start_date
    dates = []
    while True:
        new_date = add_month(cur_date, delta)
        if np.datetime64(new_date) > np.datetime64("2021-05-01"):
            break
        dates.append((np.datetime64(cur_date),np.datetime64(new_date)))
        cur_date = add_month(cur_date, rolling_delta)
    return dates


countries = ["uk", "germany", "france", "netherlands", "ireland"]
country_data = {}
for country in countries:
    df = pd.read_csv(f"{country}_data/csv_data/processed.csv",
                    dtype={"tender_value": "string",
                           "procurement_method": "string"})
    df.award_date = df.award_date.map(lambda x: dt.datetime.strptime(x, "%Y-%m-%d"))
    df["country"] = country
    df["buyer"] = list(map(lambda x: re.sub("\W+"," ", x.upper().strip()).replace(",",";"),df["buyer"].values))
    df["supplier"] = list(map(lambda x: re.sub("\W+"," ", x.upper().strip()).replace(",",";"),df["supplier"].values))
    country_data[country] = df[(~df.supplier.isna()) & (~df.buyer.isna())]

aggregated_df = pd.concat([country_data[country] for country in countries])
sum_buyers = sum([len(country_data[country].buyer.unique()) for country in countries])
sum_suppliers = sum([len(country_data[country].supplier.unique()) for country in countries])
sum_both = sum([len(np.intersect1d(country_data[country].buyer.unique(), country_data[country].supplier.unique())) for country in countries])
sum_tot = sum([len(np.union1d(country_data[country].buyer.unique(), country_data[country].supplier.unique())) for country in countries])

print(f"[BUYERS] in agg: {len(aggregated_df.buyer.unique())} \t sum of countries: {sum_buyers} \t diff: {sum_buyers - len(aggregated_df.buyer.unique())}")

print(f"[SUPPLIERS] in agg: {len(aggregated_df.supplier.unique())} \t sum of countries: {sum_suppliers} \t diff: {sum_suppliers - len(aggregated_df.supplier.unique())}")

print(f"[BOTH] in agg: {len(np.intersect1d(aggregated_df.buyer.unique(), aggregated_df.supplier.unique()))} \t sum of countries: {sum_both} \t diff: {sum_both - len(np.intersect1d(aggregated_df.buyer.unique(), aggregated_df.supplier.unique()))}")

print(f"[TOTAL] in agg: {len(np.union1d(aggregated_df.buyer.unique(), aggregated_df.supplier.unique()))} \t sum of countries: {sum_tot} \t diff: {sum_tot - len(np.union1d(aggregated_df.buyer.unique(), aggregated_df.supplier.unique()))}")


aggregated_df.drop(aggregated_df.columns[0], axis=1, inplace=True)
aggregated_df.to_csv(os.path.join("joined_data", "europe_aggregated.csv"), index=False)

rolling_3month = gen_dates_month_rolling(3, 1)

for label, data_slice in [("rolling_3month", rolling_3month)]:

    path = os.path.join("joined_data", "networks")
    if not os.path.exists(path):
        os.mkdir(path)
    nnodes = []
    nedges = []
    density = []
    deg = []
    wdeg = []
    lcc = []
    nccs = []
    in_out_deg_gt1 = []
    tot_net = nx.from_pandas_edgelist(aggregated_df,source="buyer",target="supplier",edge_attr = True,
                                create_using=nx.DiGraph())
    #nx.write_edgelist(tot_net,os.path.join(path,"europe_total.edges"),delimiter=",")
    for lower, upper in data_slice:
        #print(lower)
        temp_df = aggregated_df[(aggregated_df.award_date >= lower) & (aggregated_df.award_date < upper)]
        network = temp_df.loc[:,["buyer","supplier","award_supp_id"]].groupby(["buyer","supplier"]).count().reset_index()
        #with_cpv = processed.loc[:,["buyer","supplier","ocds","cpv"]].groupby(["buyer",
        #                                                                   "supplier","cpv"]).count().reset_index()
        g = nx.from_pandas_edgelist(network,source="buyer",target="supplier",edge_attr = True,
                                    create_using=nx.DiGraph())
        nnodes.append(g.number_of_nodes())
        nedges.append(g.number_of_edges())
        density.append(nx.density(g))
        deg.append(sum([d for u,d in g.degree()])/g.number_of_nodes())
        wdeg.append(sum([d for u,d in g.degree(weight="award_supp_id")])/g.number_of_nodes())
        lcc.append(max(map(len,list(nx.weakly_connected_components(g)))))
        nccs.append(nx.number_weakly_connected_components(g))
        indeg_gt1 = [u for u,d in g.out_degree(weight="award_supp_id") if d > 0]
        outdeg_gt1 = [u for u,d in g.in_degree(weight="award_supp_id") if d > 0]
        in_out_deg_gt1.append(len(np.intersect1d(indeg_gt1, outdeg_gt1)))
        net_path = os.path.join(path,"europe_{}.edges".format(''.join(str(lower).split('-'))))
        nx.write_edgelist(g,net_path,delimiter=",")


    print(f"Avg nnodes: {np.mean(nnodes)}")
    print(f"Avg nedges: {np.mean(nedges)}")
    print(f"Avg density: {np.mean(density)}")
    print(f"Avg degree: {np.mean(deg)}")
    print(f"Avg weighted degree: {np.mean(wdeg)}")
    print(f"Avg LCC size: {np.mean(lcc)}")
    print(f"Avg number CCs: {np.mean(nccs)}")
    print(f"Avg nnodes with in and out conns: {np.mean(in_out_deg_gt1)}")
