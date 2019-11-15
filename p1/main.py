import numpy as np
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt
import seaborn as sns

def update_dic(df, dic, nb_col):
    """ Update the dictionary in two ways depending of the value of nb_col thanks to the dataframe.

    Parameters:
        df (dataframe): The dataframe used to update the dictionary
        dic (dictionary): The dictionary which is to be updated
        nb (int): The number of columns in the dataframe

    Returns:
        /   
    """
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

def plot_hist_from_dic(dic, name_hist, cumulative, log_scale, name_x_axis):
    """ Plot an histogram from the dictionary.

    Parameters:
        dic (dictionary): The dictionary which has to be updated
        name_hist (string): The file name of the histogram to be plot
        cumulative (int): if =1, plot a CDF histogram, if =-1, plot a CCDF histogram
        log_scale (boolean): if True, set the x and y axis into a logarithmic scale, else linear scale
        name_x_axis (string): The name on the x axis

    Returns:
        /   
    """
    plt.figure()
    plt.hist(list(dic.keys()), weights=list(dic.values()), cumulative=cumulative, density=True, bins=100, log=log_scale)

    if log_scale:
        plt.xscale("log")

    plt.xlabel(name_x_axis)

    plt.savefig(name_hist)

def flow_per_prefix(tab, label, bytes_dic, pkt_dic=None, nbits=16):
    """ Get prefixes of ip addresses in tab and aggregate them according to prefixes.

    Parameters:
        tab (dataframe): The dictionary which is to be updated
        label (string): The column of the dataframe
        bytes_dic (dictionary): The dictionary of bytes to update
        pkt_dic (dictionary): The dictionary of packets to update
        n_bits (int): The size of the prefix

    Returns:
        /   
    """
    test = tab[label].str.split(".", expand=True)
    tab.insert(0, "fourth_8", test[3])
    tab.insert(0, "third_8", test[2])
    tab.insert(0, "second_8", test[1])
    tab.insert(0, "first_8", test[0])
    tab.drop(label, axis=1)

    if nbits == 24:
        tab.insert(0, "prefix", tab["first_8"] + "." + tab["second_8"] + "." + tab["third_8"] + ".0")
    else:
        tab.insert(0, "prefix", tab["first_8"] + "." + tab["second_8"] + ".0.0")

    tmp_table = tab[["prefix", "ibyt"]].groupby("prefix", sort=False).sum().reset_index()
    update_dic(tmp_table, bytes_dic, 2)

    if not pkt_dic == None: 
        tmp_table = tab[["prefix", "ipkt"]].groupby("prefix", sort=False).sum().reset_index()
        update_dic(tmp_table, pkt_dic, 2)

def Q1(dic1, nb_byt_tot, nb_pkt_tot):
    """ Compute question 1

    Parameters:
        dic1 (dictionary): The dictionary from which we have to take the information
        nb_byt_tot (int): The total number of bytes
        nb_pkt_tot (int): The total number of packets

    Returns:
        /   
    """
    plot_hist_from_dic(dic1, "Packet_dist.png", 1, False, "Packet size (bytes)")
    print("The average packet size across all the trafic is: " + str(nb_byt_tot/nb_pkt_tot))

def Q1_loop(chunk, dic1):
    """ Compute question 1

    Parameters:
        chunk (dataframe): The dataframe from which we have to update the dictionary
        dic1 (dictionary): The dictionary which has to be updated

    Returns:
        /   
    """
    #we extract the column in which we have an interest:
    new_tab = chunk.loc[:,["ipkt", "ibyt"]]

    #compute the number of bytes per packet (in average)
    new_tab["byt_per_pkt"] = new_tab["ibyt"].div(new_tab["ipkt"])

    tmp_table = (new_tab.loc[:,["ipkt", "byt_per_pkt"]]).groupby("byt_per_pkt").sum().reset_index()

    update_dic(tmp_table, dic1, 2)

def Q2(dic2_duration, dic2_byt, dic2_pkt):
    """ Compute question 2

    Parameters:
        dic2_duration (dictionary): the dictionary containing the flow durations
        dic2_byt (dictionary): the dictionary containing the flow size in bytes
        dic2_pkt (dictionary): the dictionary containing the flow size in packets

    Returns:
        /   
    """
    plot_hist_from_dic(dic2_duration, "flow_duration.png", -1, False, "Flow duration (s) with a linear scale")
    plot_hist_from_dic(dic2_byt, "flow_size_byt.png", -1, False, "Flow size (bytes) with a linear scale")
    plot_hist_from_dic(dic2_pkt, "flow_size_pkt.png", -1, False, "Flow size (packets) with a linear scale")
    #log scale
    plot_hist_from_dic(dic2_duration, "flow_duration_log.png", -1, True, "Flow duration (s) with a logarithmic scale")
    plot_hist_from_dic(dic2_byt, "flow_size_byt_log.png", -1, True, "Flow size (bytes) with a logarithmic scale")
    plot_hist_from_dic(dic2_pkt, "flow_size_pkt_log.png", -1, True, "Flow size (packets) with a logarithmic scale")

def Q2_loop(chunk, dic2_duration, dic2_byt, dic2_pkt):
    """ Compute question 2

    Parameters:
        chunk (dataframe): the dataframe from which we have to update the dictionaries
        dic2_duration (dictionary): the dictionary containing the flow durations
        dic2_byt (dictionary): the dictionary containing the flow size in bytes
        dic2_pkt (dictionary): the dictionary containing the flow size in packets

    Returns:
        /   
    """
    #flow durations
    update_dic(chunk["td"], dic2_duration, 1)
    #flow size (number of bytes)
    update_dic(chunk["ibyt"], dic2_byt, 1)
    #flow size (number of packets)
    update_dic(chunk["ipkt"], dic2_pkt, 1)

def Q3(dic3_source, dic3_dest, nb_byt_tot):
    """ Compute question 3

    Parameters:
        dic3_source (dictionary): the dictionary containing the source port numbers
        dic3_dest (dictionary): the dictionary containing the distination port numbers
        nb_byt_tot (int): the total number of bytes

    Returns:
        /   
    """
    #flow durations
    #for the source port
    sender_trafic = {"sp" : list(dic3_source.keys()) , "ibyt" : list(dic3_source.values())}
    df_st = pd.DataFrame(sender_trafic)
    df_st = df_st.sort_values(by="ibyt", ascending=False).head(10)
    df_st["percentage"] = df_st["ibyt"].div(nb_byt_tot).mul(100)
    print(df_st)
    #for the destination port
    receiver_trafic = {"dp" : list(dic3_dest.keys()) , "ibyt" : list(dic3_dest.values())}
    df_rt = pd.DataFrame(receiver_trafic)
    df_rt = df_rt.sort_values(by="ibyt", ascending=False).head(10)
    df_rt["percentage"] = df_rt["ibyt"].div(nb_byt_tot).mul(100)
    print(df_rt)

def Q3_loop(chunk, dic3_source, dic3_dest):
    """ Compute question 3

    Parameters:
        chunk (dataframe): the dataframe from which we have to update the dictionaries
        dic3_source (dictionary): the dictionary containing the source port numbers
        dic3_dest (dictionary): the dictionary containing the distination port numbers

    Returns:
        /   
    """
    #flow durations
    #for the source port
    tmp_table_sp = ((chunk.loc[:,["sp", "ibyt"]]).groupby("sp").sum()).reset_index()
    update_dic(tmp_table_sp, dic3_source, 2)

    #for the destination port
    tmp_table_dp = ((chunk.loc[:,["dp", "ibyt"]]).groupby("dp").sum()).reset_index()
    update_dic(tmp_table_dp, dic3_dest, 2)

def Q4(dic, nb_byt_tot):
    """ Compute question 4

    Parameters:
        dic (dictionary): the dictionary containing the source ip prefixe and the number of bytes
        nb_byt_tot (int): the total number of bytes

    Returns:
        /   
    """
    #Show the most used prefix
    tab = pd.DataFrame({"sender prefixes" : list(dic.keys()), "nb of bytes" : list(dic.values())})
    tab["% of traffic (bytes)"] = tab["nb of bytes"].div(nb_byt_tot).multiply(100)
    tab = tab.sort_values("% of traffic (bytes)", ascending=False)

    plt.figure()
    plt.bar(range(100), height=list(tab["% of traffic (bytes)"].head(100)))
    plt.xlabel("The 100 biggest IP prefixes by taffic volume")
    plt.ylabel("% of traffic volume (bytes)")
    plt.savefig("Q4_all.png")
    
    len_tab = len(tab)

    print("fraction of the traffic (by bytes) that comes from the most popular 0.1% of source IP prefixes : "+ str(tab["% of traffic (bytes)"].head(round(len_tab*0.001)).sum()))
    print("fraction of the traffic (by bytes) that comes from the most popular 1% of source IP prefixes : "+ str(tab["% of traffic (bytes)"].head(round(len_tab*0.01)).sum()))
    print("fraction of the traffic (by bytes) that comes from the most popular 10% of source IP prefixes : "+ str(tab["% of traffic (bytes)"].head(round(len_tab*0.1)).sum()))
    print()

    limit_aggregation = 0.001
    aggregate = tab[tab["% of traffic (bytes)"] > limit_aggregation]
    rest = tab[tab["% of traffic (bytes)"] <= limit_aggregation]

    print("fraction of traffic (by bytes) that is not aggregate " + str(rest["% of traffic (bytes)"].sum()))
    print()

    #recompute the %
    total_bytes_aggregate = aggregate["nb of bytes"].sum()
    aggregate.insert(0, "new % of traffic (bytes)", aggregate["nb of bytes"].div(total_bytes_aggregate).multiply(100))

    len_aggregate = len(aggregate)
    print("Top 0.1% of soure prefixes that have a positive mask lengths : "+ str(aggregate["new % of traffic (bytes)"].head(round(len_aggregate*0.001)).sum()))
    print("Top 1% of soure prefixes that have a positive mask lengths : "+ str(aggregate["new % of traffic (bytes)"].head(round(len_aggregate*0.01)).sum()))
    print("Top 10% of soure prefixes that have a positive mask lengths : "+ str(aggregate["new % of traffic (bytes)"].head(round(len_aggregate*0.1)).sum()))

def Q4_loop(chunk, dic):
    """ Compute question 4

    Parameters:
        chunk (dataframe): the dataframe from which we have to update the dictionary
        dic (dictionary): the dictionary containing the source ip prefixe and the number of bytes

    Returns:
        /   
    """
    #Discard the IPV6 addresses
    chunk = chunk[~chunk["sa"].str.contains(":")]
    #Process chunks
    flow_per_prefix(chunk, "sa", dic)

def Q5(sender_dic_pkt, dest_dic_pkt, sender_dic_bytes, dest_dic_bytes, nb_byt_tot, nb_pkt_tot):
    """ Compute question 5

    Parameters:
        sender_dic_pkt (dictionary): the dictionary containing the traffic (in packets) from an IP prefix
        dest_dic_pkt (dictionary): the dictionary containing the traffic (in packets) to an IP prefix
        sender_dic_bytes (dictionary): the dictionary containing the traffic (in bytes) from an IP prefix
        dest_dic_bytes (dictionary): the dictionary containing the traffic (in bytes) to an IP prefix
        nb_byt_tot (int): the total number of bytes
        nb_pkt_tot (int): the total number of packets

    Returns:
        /   
    """
    # Flow traffic by packets
    tab = pd.DataFrame({"sender prefixes" : list(sender_dic_pkt.keys()), "nb of packets" : list(sender_dic_pkt.values())})
    tab["% of traffic (packets)"] = tab["nb of packets"].div(nb_pkt_tot).multiply(100)
    tab = tab.sort_values("% of traffic (packets)", ascending=False)
    tab.to_csv("sp_pkt.csv",sep=",")

    tab = pd.DataFrame({"dest prefixes" : list(dest_dic_pkt.keys()), "nb of packets" : list(dest_dic_pkt.values())})
    tab["% of traffic (packets)"] = tab["nb of packets"].div(nb_pkt_tot).multiply(100)
    tab = tab.sort_values("% of traffic (packets)", ascending=False)
    tab.to_csv("dp_pkt.csv",sep=",")

    # Flow traffic by bytes
    tab = pd.DataFrame({"sender prefixes" : list(sender_dic_bytes.keys()), "nb of bytes" : list(sender_dic_bytes.values())})
    tab["% of traffic (bytes)"] = tab["nb of bytes"].div(nb_byt_tot).multiply(100)
    tab = tab.sort_values("% of traffic (bytes)", ascending=False)
    tab.to_csv("sp_byt.csv",sep=",")

    tab = pd.DataFrame({"dest prefixes" : list(dest_dic_bytes.keys()), "nb of bytes" : list(dest_dic_bytes.values())})
    tab["% of traffic (bytes)"] = tab["nb of bytes"].div(nb_byt_tot).multiply(100)
    tab = tab.sort_values("% of traffic (bytes)", ascending=False)
    tab.to_csv("dp_byt.csv",sep=",")

def Q5_loop(chunk, sender_dic_pkt, dest_dic_pkt, sender_dic_bytes, dest_dic_bytes):
    """ Compute question 5

    Parameters:
        chunk (dataframe): the dataframe from which we have to update the dictionaries
        sender_dic_pkt (dictionary): the dictionary containing the traffic (in packets) from an IP prefix
        dest_dic_pkt (dictionary): the dictionary containing the traffic (in packets) to an IP prefix
        sender_dic_bytes (dictionary): the dictionary containing the traffic (in bytes) from an IP prefix
        dest_dic_bytes (dictionary): the dictionary containing the traffic (in bytes) to an IP prefix

    Returns:
        /   
    """
    # On vire les ipv6 pcq trop casse couilles (il y en a un peu moins que 3.5 millions donc c"est ok)
    chunk = chunk[~chunk["sa"].str.contains(":")]
    # Process the chunks
    dest_prefixes = chunk[chunk["da"].str.contains("139.91.")]
    sender_prefixes = chunk[chunk["sa"].str.contains("139.91.")]

    flow_per_prefix(sender_prefixes, "sa", sender_dic_bytes, pkt_dic=sender_dic_pkt, nbits=24)
    flow_per_prefix(dest_prefixes, "da", dest_dic_bytes, pkt_dic=dest_dic_pkt, nbits=24)

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

    ### Q4 ###
    dic4 = {}

    ### Q5 ###
    sender_dic5_pkt = {}
    dest_dic5_pkt = {}
    sender_dic5_bytes = {}
    dest_dic5_bytes = {}

    for chunk in pd.read_csv('netflow.csv.gz', compression="gzip", chunksize=chunksize, delimiter=',', skip_blank_lines=True, error_bad_lines=False, nrows=92507636-4):
        print("Loop : "+str(nb_loop))
        nb_loop += 1

        #to compute the average packet size
        nb_pkt_tot += sum(chunk["ipkt"])
        nb_byt_tot += sum(chunk["ibyt"])

        ### Q1 ###
        Q1_loop(chunk, dic1)

        ### Q2 ###
        Q2_loop(chunk, dic2_duration, dic2_byt, dic2_pkt)

        ### Q3 ###
        Q3_loop(chunk, dic3_source, dic3_dest)

        ### Q4 ###
        Q4_loop(chunk, dic4)

        ### Q5 ###
        Q5_loop(chunk, sender_dic5_pkt, dest_dic5_pkt, sender_dic5_bytes, dest_dic5_bytes)
        

    ### Q1 ###
    Q1(dic1, nb_byt_tot, nb_pkt_tot)

    ### Q2 ###
    Q2(dic2_duration, dic2_byt, dic2_pkt)    

    ### Q3 ###
    Q3(dic3_source, dic3_dest, nb_byt_tot)

    ### Q4 ###
    Q4(dic4, nb_byt_tot)

    ### Q5 ###
    Q5(sender_dic5_pkt, dest_dic5_pkt, sender_dic5_bytes, dest_dic5_bytes, nb_byt_tot, nb_pkt_tot)