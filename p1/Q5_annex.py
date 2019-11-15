import numpy as np
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt
import seaborn as sns
import socket

def update_dic(df, dic):
    for i in range(len(df)):
        key = df.iat[i,0]
        value = df.iat[i,1]

        if key in dic:
            dic[key] += value
        else:
            dic[key] = value

def flow_per_prefix(chunk, label, dic):
    chunk[["first_8", "second_8", "third_8", "fourth_8"]] = chunk[label].str.split(".", expand=True)
    chunk.drop(label, axis=1)

    chunk["prefix"] = chunk["first_8"] + "." + chunk["second_8"] + ".0.0"
    chunk["count"] = [1 for j in range(len(chunk["prefix"]))]
    tmp_table = chunk[["prefix", "count"]].groupby("prefix", sort=False).sum().reset_index()

    update_dic(tmp_table, dic)

if __name__ == "__main__":
    chunksize = 1000000
    sender_dic_count = {}
    dest_dic_count = {}
    nb_loop = 1

    for chunk in pd.read_csv("netflow.csv.gz", compression="gzip", chunksize=chunksize, delimiter=",", usecols=["sa", "da", "ibyt", "ipkt"], skip_blank_lines=True, error_bad_lines=False, nrows=92507636-4):
        print("Loop : "+str(nb_loop))
        nb_loop += 1

        #Discard the IPV6 addresses
        chunk = chunk[~chunk["sa"].str.contains(":")]
        #Process chunks
        flow_per_prefix(chunk, "sa", sender_dic_count)
        flow_per_prefix(chunk, "da", dest_dic_count)

    #Show the most used prefix
    tab = pd.DataFrame({"sender prefixes" : list(sender_dic_count.keys()), "nb of flows" : list(sender_dic_count.values())})
    tab["% of flows"] = tab["nb of flows"].div(tab["nb of flows"].sum()).multiply(100)
    print(tab.sort_values("% of flows", ascending=False))

    tab = pd.DataFrame({"dest prefixes" : list(dest_dic_count.keys()), "nb of flows" : list(dest_dic_count.values())})
    tab["% of flows"] = tab["nb of flows"].div(tab["nb of flows"].sum()).multiply(100)
    print(tab.sort_values("% of flows", ascending=False))
