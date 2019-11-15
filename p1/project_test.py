import numpy as np
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt

if __name__ == "__main__":
    test = pd.read_csv("netflow.csv.gz", compression="gzip", delimiter=",")
    print("EXTRACTION TERMINEE")
    #### Q1 ####
    #extract the colunm in which we have an interest
    new_tab = test.loc[:,["ipkt", "ibyt", "opkt", "obyt"]]

    #create two new columns
    new_tab["all_pkt"] =  new_tab["ipkt"] + new_tab["opkt"]
    new_tab["all_byt"] = new_tab["ibyt"] + new_tab["obyt"]

    #compute the number of bytes per packet (in average)
    new_tab["byt_per_pkt"] = new_tab["all_byt"].div(new_tab["all_pkt"])

    #create a new dataframe with one empty column
    test_new = pd.DataFrame(columns=["nb_bytes"])

    iterator = 0

    for i in new_tab["all_pkt"]: #we take the total number of packets a each line
        for j in range(i): #we add a line for each packet
            test_new = test_new.append({"nb_bytes" : new_tab["byt_per_pkt"][iterator]}, ignore_index=True)

        iterator += 1

    #plot CDF of the packet size distribution
    plt.figure()
    plt.hist(test_new["nb_bytes"], cumulative=True, density=True)
    plt.savefig("Packet_dist.png")

    #average packet size
    print("The average packet size accros the all trafic is: " + str(sum(new_tab["all_byt"]/(sum(new_tab["all_pkt"])))))

    ### Q2 ###
    #plot the CCDF of flow durations with a linear scale
    plt.figure()
    plt.hist(test["td"], cumulative=-1)
    plt.savefig("flow_duration.png")

    #plot the CCDF of flow size (number of bytes) with a linear scale
    plt.figure()
    plt.hist(new_tab["all_byt"], cumulative=-1)
    plt.savefig("flow_size_byt.png")

    #plot the CCDF of flow size (number of packets) with a linear scale
    plt.figure()
    plt.hist(new_tab["all_pkt"], cumulative=-1)
    plt.savefig("flow_size_pkt.png")

    #plot the CCDF of flow durations with a logarithmic scale
    plt.figure()
    plt.hist(test["td"], cumulative=-1, log=True)
    plt.savefig("flow_duration_log.png")

    #plot the CCDF of flow size (number of bytes) with a logarithmic scale
    plt.figure()
    plt.hist(new_tab["all_byt"], cumulative=-1, log=True)
    plt.savefig("flow_size_byt_log.png")

    #plot the CCDF of flow size (number of packets) with a logarithmic scale
    plt.figure()
    plt.hist(new_tab["all_pkt"], cumulative=-1, log=True)
    plt.savefig("flow_size_pkt_log.png")

    ### Q3 ###
    #for the source port
    table_sender = ((test.loc[:,["sp", "ibyt"]]).groupby("sp").sum()).reset_index().sort_values(by="ibyt", ascending=False).head(10)
    table_sender["percentage"] = table_sender["ibyt"].div(table_sender["ibyt"].sum()).mul(100)
    print(table_sender)

    #for the destination port
    table_sender = ((test.loc[:,["dp", "obyt"]]).groupby("dp").sum()).reset_index().sort_values(by="obyt", ascending=False).head(10)
    table_sender["percentage"] = table_sender["obyt"].div(table_sender["obyt"].sum()).mul(100)
    print(table_sender)
