import numpy as np
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt
import seaborn as sns

def update_dic(df, dic):
    for i in range(len(df)):
        key = df.iat[i,0]
        value = df.iat[i,1]

        if key in dic:
            dic[key] += value
        else:
            dic[key] = value

def flow_per_prefix(tab, label, bytes_dic, pkt_dic, nbits=16):

    test = tab[label].str.split(".", expand=True)
    print(test)
    tab.insert(0, "fourth_8", test[3])
    tab.insert(0, "third_8", test[2])
    tab.insert(0, "second_8", test[1])
    tab.insert(0, "first_8", test[0])
    tab.drop(label, axis=1)

    if nbits == 24:
        tab.insert(0, "prefix", tab["first_8"] + "." + tab["second_8"] + "." + tab["third_8"] + ".0")
    else:
        tab.insert(0, "prefix", tab["first_8"] + "." + tab["second_8"] + ".0.0")

    tmp_table = tab[["prefix", "ibyt", "ipkt"]].groupby("prefix", sort=False).sum().reset_index()

    update_dic(tmp_table[["prefix", "ibyt"]], bytes_dic)
    update_dic(tmp_table[["prefix", "ipkt"]], pkt_dic)

if __name__ == "__main__":

    chunksize = 1000000
    sender_dic_pkt = {}
    dest_dic_pkt = {}
    sender_dic_bytes = {}
    dest_dic_bytes = {}
    nb_loop = 1

    for chunk in pd.read_csv("netflow.csv.gz", compression="gzip", chunksize=chunksize, delimiter=",", usecols=["sa", "da", "ibyt", "ipkt"], skip_blank_lines=True, error_bad_lines=False, nrows=1500000):
        print("Loop : "+str(nb_loop))
        nb_loop += 1

        #Discard the IPV6 addresses
        chunk = chunk[~chunk["sa"].str.contains(":")]
        #Process chunks
        dest_prefixes = chunk[chunk["da"].str.contains("139.91.")]
        sender_prefixes = chunk[chunk["sa"].str.contains("139.91.")]

        flow_per_prefix(sender_prefixes, "sa", sender_dic_bytes, sender_dic_pkt, nbits=24)
        flow_per_prefix(dest_prefixes, "da", dest_dic_bytes, dest_dic_pkt, nbits=24)

    # Flow traffic by packets
    tab = pd.DataFrame({"sender prefixes" : list(sender_dic_pkt.keys()), "nb of packets" : list(sender_dic_pkt.values())})
    tab["% of traffic (packets)"] = tab["nb of packets"].div(tab["nb of packets"].sum()).multiply(100)
    print(tab.sort_values("% of traffic (packets)", ascending=False))

    tab = pd.DataFrame({"dest prefixes" : list(dest_dic_pkt.keys()), "nb of packets" : list(dest_dic_pkt.values())})
    tab["% of traffic (packets)"] = tab["nb of packets"].div(tab["nb of packets"].sum()).multiply(100)
    print(tab.sort_values("% of traffic (packets)", ascending=False))

    # Flow traffic by bytes
    tab = pd.DataFrame({"sender prefixes" : list(sender_dic_bytes.keys()), "nb of bytes" : list(sender_dic_bytes.values())})
    tab["% of traffic (bytes)"] = tab["nb of bytes"].div(tab["nb of bytes"].sum()).multiply(100)
    print(tab.sort_values("% of traffic (bytes)", ascending=False))

    tab = pd.DataFrame({"dest prefixes" : list(dest_dic_bytes.keys()), "nb of bytes" : list(dest_dic_bytes.values())})
    tab["% of traffic (bytes)"] = tab["nb of bytes"].div(tab["nb of bytes"].sum()).multiply(100)
    print(tab.sort_values("% of traffic (bytes)", ascending=False))
