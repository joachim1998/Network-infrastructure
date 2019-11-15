import numpy as np
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt
import seaborn as sns

def update_dic(df, dic, nb_col):
    for i in range(len(df)):
        if nb_col == 1:
            key = df.iat[i]
            value = 1
        else:
            key = df.iat[i,0]
            value = df.iat[i,1]

        if key in dic:
            dic[key] += value
        else:
            dic[key] = value

def plot_hist_from_dic(dic, name_hist, cumulative, log_scale):
    print("PLOT THE HISTOGRAM")
    plt.figure()
    plt.hist(list(dic.keys()), weights=list(dic.values()), cumulative=cumulative, density=True, bins=100, log=log_scale)

    if log_scale:
        plt.xscale("log")

    plt.savefig(name_hist)

def flow_per_prefix(chunk, label, dic, label_to_study=None):
    chunk[["first_8", "second_8", "third_8", "fourth_8"]] = chunk[label].str.split(".", expand=True)
    chunk.drop(label, axis=1)

    # Si on veut prendre une taille de préfixe autre qu'un multiple de 8 bits on doit faire ca :

    #chunk[["first_8", "second_8", "third_8", "fourth_8"]] = chunk[["first_8", "second_8", "third_8", "fourth_8"]].astype(int)
    #chunk["first_8"] = chunk["first_8"] & 0xff
    #chunk["second_8"] = chunk["second_8"] & 0xff
    #chunk["third_8"] = chunk["third_8"] & 0x00
    #chunk["fourth_8"] = chunk["fourth_8"] & 0x00
    #chunk[["first_8", "second_8", "third_8", "fourth_8"]] = chunk[["first_8", "second_8", "third_8", "fourth_8"]].astype(str)
    #chunk["prefix"] = chunk["first_8"] + "." + chunk["second_8"] + "." + chunk["third_8"] + "." + chunk["fourth_8"]

    chunk["prefix"] = chunk["first_8"] + "." + chunk["second_8"] + ".0.0"

    if label_to_study == None:
        chunk["count"] = [1 for j in range(len(chunk["prefix"]))]
        tmp_table = chunk[["prefix", "count"]].groupby("prefix", sort=False).sum().reset_index()
    else:
        tmp_table = chunk[["prefix", label_to_study]].groupby("prefix", sort=False).sum().reset_index()

    update_dic(tmp_table, dic)

def Q1(chunk, dic1, nb_byt_tot, nb_pkt_tot):
    


if __name__ == "__main__":
    chunksize = 1000000
    nb_loop = 1

    ### Q1 ###
    dic1 = {}
    nb_pkt_tot = 0
    nb_byt_tot = 0

    ### Q2 ###
    dic2_duration = {}
    dic2_byt = {}
    dic2_pkt = {}

    ### Q3 ###
    dic3_source = {}
    dic3_dest = {}

    for chunk in pd.read_csv('netflow.csv.gz', compression="gzip", chunksize=chunksize, delimiter=',', skip_blank_lines=True, error_bad_lines=False, nrows=92507636-4):
        print("Loop : "+str(nb_loop))
        nb_loop += 1
        ### Q1 ###
        #we extract the column in which we have an interest:
        new_tab = chunk.loc[:,["ipkt", "ibyt"]]

        #compute the number of bytes per packet (in average)
        new_tab["byt_per_pkt"] = new_tab["ibyt"].div(new_tab["ipkt"])

        tmp_table = (new_tab.loc[:,["ipkt", "byt_per_pkt"]]).groupby("byt_per_pkt").sum().reset_index() #on a notre table qui a compté le nbr de pkt qui ont la meme taille

        update_dic(tmp_table, dic1, 2)

        #to compute the average packet size
        nb_pkt_tot += sum(new_tab["ipkt"])
        nb_byt_tot += sum(new_tab["ibyt"])

        ### Q2 ###
        #flow durations
        update_dic(chunk["td"], dic2_duration, 1)
        #flow size (number of bytes)
        update_dic(chunk["ibyt"], dic2_byt, 1)
        #flow size (number of packets)
        update_dic(chunk["ipkt"], dic2_pkt, 1)

        ### Q3 ###
        #for the source port
        tmp_table_sp = ((chunk.loc[:,["sp", "ibyt"]]).groupby("sp").sum()).reset_index() #.sort_values(by="ibyt", ascending=False).head(10)
        update_dic(tmp_table_sp, dic3_source, 2) 
        #table_sender["percentage"] = table_sender["ibyt"].div(table_sender["ibyt"].sum()).mul(100)
        #print(table_sender)

        #for the destination port
        tmp_table_dp = ((chunk.loc[:,["dp", "ibyt"]]).groupby("dp").sum()).reset_index() #.sort_values(by="obyt", ascending=False).head(10)
        update_dic(tmp_table_dp, dic3_dest, 2)
        #table_sender["percentage"] = table_sender["obyt"].div(table_sender["obyt"].sum()).mul(100)
        #print(table_sender)

        ### Q5 ###
        # On vire les ipv6 pcq trop casse couilles (il y en a un peu moins que 3.5 millions donc c"est ok)
        chunk = chunk[~chunk["sa"].str.contains(":")]
        # Process les chunk
        flow_per_prefix(chunk, "sa", sender_dic_count)
        flow_per_prefix(chunk, "da", dest_dic_count)
        flow_per_prefix(chunk, "sa", sender_dic_bytes, label_to_study="ibyt")
        flow_per_prefix(chunk, "da", dest_dic_bytes, label_to_study="ibyt")
        

    ### Q1 ###
    plot_hist_from_dic(dic1, "Packet_dist.png", 1, False)
    print("The average packet size across all the trafic is: " + str(nb_byt_tot/nb_pkt_tot))

    ### Q2 ###
    plot_hist_from_dic(dic2_duration, "flow_duration.png", -1, False)
    plot_hist_from_dic(dic2_byt, "flow_size_byt.png", -1, False)
    plot_hist_from_dic(dic2_pkt, "flow_size_pkt.png", -1, False)
    #log scale
    plot_hist_from_dic(dic2_duration, "flow_duration_log.png", -1, True)
    plot_hist_from_dic(dic2_byt, "flow_size_byt_log.png", -1, True)
    plot_hist_from_dic(dic2_pkt, "flow_size_pkt_log.png", -1, True)

    ### Q3 ###
    #for the source port
    sender_trafic = {"sp" : list(dic3_source.keys()) , "ibyt" : list(dic3_source.values())}
    df_st = pd.DataFrame(sender_trafic)
    df_st = df_st.sort_values(by="ibyt", ascending=False).head(10)
    df_st["percentage"] = df_st["ibyt"].div(nb_byt_tot).mul(100)
    print(df_st)
    #for the destination port
    receiver_trafic = {"sp" : list(dic3_dest.keys()) , "ibyt" : list(dic3_dest.values())}
    df_rt = pd.DataFrame(receiver_trafic)
    df_rt = df_rt.sort_values(by="ibyt", ascending=False).head(10)
    df_rt["percentage"] = df_rt["ibyt"].div(nb_byt_tot).mul(100)
    print(df_rt)

    ### Q5 ###
    # Permet de voir quel est le préfixe de l'unif
    tab = pd.DataFrame({"sender prefixes" : list(sender_dic_count.keys()), "nb of flows" : list(sender_dic_count.values())})
    tab["% of flows"] = tab["nb of flows"].div(tab["nb of flows"].sum()).multiply(100)
    print(tab.sort_values("% of flows", ascending=False))

    tab = pd.DataFrame({"dest prefixes" : list(dest_dic_count.keys()), "nb of flows" : list(dest_dic_count.values())})
    tab["% of flows"] = tab["nb of flows"].div(tab["nb of flows"].sum()).multiply(100)
    print(tab.sort_values("% of flows", ascending=False))

    # Pourcentage du traffic (en bytes) par préfixe
    tab = pd.DataFrame({"sender prefixes" : list(sender_dic_bytes.keys()), "nb of bytes" : list(sender_dic_bytes.values())})
    tab["% of bytes"] = tab["nb of bytes"].div(tab["nb of bytes"].sum()).multiply(100)
    print(tab.sort_values("% of bytes", ascending=False))

    tab = pd.DataFrame({"dest prefixes" : list(dest_dic_bytes.keys()), "nb of bytes" : list(dest_dic_bytes.values())})
    tab["% of bytes"] = tab["nb of bytes"].div(tab["nb of bytes"].sum()).multiply(100)
    print(tab.sort_values("% of bytes", ascending=False))









        #mtn test_new va contenir une grande colonne avec tout les pkt,... du chunk => on doit les mettre dans le dictionnaire!!!
        # for _X, _y in zip(X, y):
        #    if _y in dictionary:
        #        dictionary[_y][0] += 1
        #        dictionary[_y][1].append(_X)
        #    else:
        #        dictionary[_y] = [1, [_X]]


    
    #table = pd.read_csv('netflow.csv.gz', compression="gzip", usecols=["opkt", "obyt"], delimiter=',', skip_blank_lines=True, error_bad_lines=False, nrows=92507636-4)
    #print("Extraction done")
    #table["byt_per_pkt"] = table["ibyt"].div(table["ipkt"])
    #print("obyt : " + str(sum(table["obyt"])))
    #print()
    #print("opkt : " + str(sum(table["opkt"])))

    #plt.figure()
    #on peut ajouter le paramètre bins=nombre pour diviser en un certain nombre : de base bins=10 => on a notre hist diviser en 10 barres
    #plt.hist(table["byt_per_pkt"], weights=table["ipkt"], cumulative=True, density=True, bins=100) 
    #plt.savefig("usecols.png")
    #plt.show()

