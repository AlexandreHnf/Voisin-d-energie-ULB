from constants import *
import pyToCassandra as ptc

import os
import sys
import pandas as pd

# ====================================================================================


def read_sensor_info(path, sensor_file):
    """
    read csv file of sensors data
    """
    path += sensor_file
    sensors = pd.read_csv(path, header=0, index_col=1)
    return sensors


def getTiming(t, now):
    """
    get the timestamp of the "since"
    ex : the timestamp 20 min ago
    """
    print("since {}".format(t))
    timing = 0
    if t:
        if t[0] == "s":
            e = t[1:].split("-")
            timing = pd.Timestamp(year=int(e[0]), month=int(e[1]), day=int(e[2]),
                                  hour=int(e[3]), minute=int(e[4]), tz="CET")
        else:
            print("time delta : ", pd.Timedelta(t))
            print("now : ", now)
            timing = now - pd.Timedelta(t)

    print("timing : ", timing)
    return timing


def getRawData(session, since):
    """ 
    get raw flukso data from cassandra since a certain amount of time
    """
    now = pd.Timestamp.now(tz="UTC")
    timing = getTiming(since, now)

    print("timing date : ", str(timing.date()))
    print("now date : ", str(now.date()))
    dates = list(set(["'"+str(timing.date())+"'", "'"+str(now.date())+"'"]))
    print(dates)

    dates = ",".join(dates)
    where_clause = "home_id = {} and day IN ({}) AND ts > {}".format("'CDB011'", dates, "'"+str(timing)+"'")
    data = ptc.selectQuery(session, CASSANDRA_KEYSPACE, "raw_data", "*", where_clause, "LIMIT 10")

    # for row in data:
    #     print(row)

    print(data)
        

def main():
    session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
    since = "s2022-01-10-16-24-32"
    getRawData(session, since) 


if __name__ == "__main__":
    main()