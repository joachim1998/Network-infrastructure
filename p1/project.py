import numpy as np
import pandas as pd
from scipy import stats
from matplotlib import pyplot as plt

import csv #pour tester de lire le fichier du prof

if __name__ == "__main__":
    #test = pd.read_csv("test.zip",compression='zip', delimiter=",")
    test = pd.read_csv("test.csv", delimiter=",")

    #### Q1 ####

    # Tenir compte du nbre de packets -> rajouter des lignes à doctets
    #test["doctets"].hist(cumulative=True)
    plt.figure()
    plt.hist(test["doctets"], cumulative=True, density=True)
    plt.savefig("Packet_dist.png")

    print(np.multiply(test["doctets"], test["dpkts"]).mean())

    #### Q2 ####
    test.insert(test.columns.get_loc("doctets") + 1,"flow", np.multiply(test["dpkts"], test["doctets"]), True)

    plt.figure()
    plt.hist(np.subtract(test["last"], test["first"]), cumulative=-1)
    plt.savefig("flow_duration.png")

    plt.figure()
    plt.hist(test["flow"], cumulative=-1)
    plt.savefig("flow_size.png")

    # Logarithmic scale
    plt.figure()
    plt.hist(np.subtract(test["last"], test["first"]), cumulative=-1, log=True)
    plt.xscale("log")
    plt.savefig("flow_duration_log.png")

    plt.figure()
    plt.hist(test["flow"], cumulative=-1, log=True)
    plt.xscale("log")
    plt.savefig("flow_size_log.png")

    #### Q3 ####
    #for the source port
    table_sender = ((test.loc[:,["srcport", "flow"]]).groupby("srcport").sum()).reset_index().sort_values(by="flow", ascending=False).head(10)
    table_sender["percentage"] = table_sender["flow"].div(table_sender["flow"].sum()).mul(100)
    print(table_sender)

    #for the destination port
    table_receiver = ((test.loc[:,["dstport", "flow"]]).groupby("dstport").sum()).reset_index().sort_values(by="flow", ascending=False).head(10)
    table_receiver["percentage"] = table_receiver["flow"].div(table_receiver["flow"].sum()).mul(100)
    print(table_receiver)

   # test = pd.read_csv("netflow.csv.gz", compression="gzip", delimiter=",", nrows=20)

    #print(test)

    #test.to_csv("20_lignes.csv",sep=",")
