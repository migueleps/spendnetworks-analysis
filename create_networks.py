import numpy as np
import pandas as pd
import networkx as nx
import datetime as dt
import sys
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



processed = pd.read_csv(f"{sys.argv[1]}_data/csv_data/processed.csv",
                        dtype = {"tender_value": "string",
                                "procurement_method": "string"})

#processed = processed.loc[~processed.award_date.isin(['9998-01-01']),:]

processed.award_date = processed.award_date.map(lambda x: dt.datetime.strptime(x, "%Y-%m-%d"))

processed["buyer"] = list(map(lambda x: re.sub("\W+"," ", x.upper().strip()).replace(",",";"),processed["buyer"].values))
processed["supplier"] = list(map(lambda x: re.sub("\W+"," ", x.upper().strip()).replace(",",";"),processed["supplier"].values))

processed = processed.loc[(~processed.buyer.isna()) & (~processed.supplier.isna()),:]

#processed = processed[processed["tender.date"] < dt.datetime.strptime("2021-01-01", "%Y-%m-%d")]

#print(f"Buyers: {len(processed.buyer.unique())}, Suppliers: {len(processed.supplier.unique())}, Both: {len(np.intersect1d(processed.buyer.unique(),processed.supplier.unique()))}, total: {len(np.union1d(processed.buyer.unique(),processed.supplier.unique()))}")


rolling_3month = gen_dates_month_rolling(3, 1)

for label, data_slice in [("rolling_3month", rolling_3month)]:

    path = os.path.join(f"{sys.argv[1]}_data", "networks")
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
    for lower, upper in data_slice:
        temp_df = processed[(processed.award_date >= lower) & (processed.award_date < upper)]
        network = temp_df.loc[:,["buyer","supplier","award_supp_id"]].groupby(["buyer","supplier"]).count().reset_index()

        g = nx.from_pandas_edgelist(network,source="buyer",target="supplier",edge_attr = True,
                                    create_using=nx.DiGraph())
        if g.number_of_nodes() == 0:
            print(f"Country {sys.argv[1]} has no data in slice starting: {''.join(str(lower).split('-'))}")
            continue
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
        net_path = os.path.join(path,"{}_{}.edges".format(sys.argv[1],''.join(str(lower).split('-'))))
        nx.write_edgelist(g,net_path,delimiter=",")

    print(f"Avg nnodes: {np.mean(nnodes)}")
    print(f"Avg nedges: {np.mean(nedges)}")
    print(f"Avg density: {np.mean(density)}")
    print(f"Avg degree: {np.mean(deg)}")
    print(f"Avg weighted degree: {np.mean(wdeg)}")
    print(f"Avg LCC size: {np.mean(lcc)}")
    print(f"Avg number CCs: {np.mean(nccs)}")
    print(f"Avg nnodes with in and out conns: {np.mean(in_out_deg_gt1)}")
