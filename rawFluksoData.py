""" 
Author : Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and 
- format it into tables of time series
- save the raw data in Cassandra database
"""

from home import *
from gui import *
from constants import *
import pyToCassandra as ptc

import argparse
import os
import sys

import copy
import pandas as pd
import tmpo
import matplotlib.pyplot as plt

# Hide warnings :
import matplotlib as mpl
import urllib3
import warnings

# max open warning
mpl.rc('figure', max_open_warning=0)

# security warning & Future warning
warnings.simplefilter('ignore', urllib3.exceptions.SecurityWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

# ====================================================================================


def read_sensor_info(path, sensor_file):
    """
    read csv file of sensors data
    """
    path += sensor_file
    sensors = pd.read_csv(path, header=0, index_col=1)
    return sensors


def getTiming(t):
    """
    get the timestamp of the "since"
    ex : the timestamp 20 min ago
    """
    # print("since {}".format(since))
    timing = 0
    if t:
        if t[0] == "s":
            e = t[1:].split("-")
            timing = pd.Timestamp(year=int(e[0]), month=int(e[1]), day=int(e[2]),
                                  hour=int(e[3]), minute=int(e[4]), tz="CET").tz_convert("UTC")
        else:
            print("time delta : ", pd.Timedelta(t))
            timing = pd.Timestamp.now(tz="UTC") - pd.Timedelta(t)

    print("timing : ", timing)
    return timing


def getProgDir():
    import __main__
    main_path = os.path.abspath(__main__.__file__)
    main_path = os.path.dirname(main_path) + os.sep
    return main_path


def generateHomes(session, sensors, since, since_timing, to_timing, home_ids):
    homes = {}

    for hid in home_ids:
        print("========================= HOME {} =====================".format(hid))
        home = Home(session, sensors, since, since_timing, to_timing, hid)
        homes[hid] = home

    return homes


def generateGroupedHomes(homes, groups):
    grouped_homes = []
    for i, group in enumerate(groups):  # group = tuple (home1, home2, ..)
        print("========================= GROUP {} ====================".format(i + 1))
        home = copy.copy(homes[group[0]])
        for j in range(1, len(group)):
            print(homes[group[j]].getHomeID())
            home.appendFluksoData(homes[group[j]].getPowerDF(), homes[group[j]].getHomeID())
            home.addConsProd(homes[group[j]].getConsProdDF())
        home.setHomeID("group_" + str(i + 1))

        grouped_homes.append(home)

    return grouped_homes


def getFluksoData(sensor_file, path=""):
    """
    get Flukso data (via API) 
    """
    if not path:
        path = getProgDir()

    sensors = read_sensor_info(path, sensor_file)
    # print(sensors.head(5))
    session = tmpo.Session(path)
    for hid, hn, sid, tk, n, c, p in sensors.values:
        session.add(sid, tk)

    session.sync()

    return sensors, session


def getFLuksoGroups():
    """
    returns Groups with format : [[home_ID1, home_ID2], [home_ID3, home_ID4], ...]
    """
    groups = []
    with open(GROUPS_FILE) as f:
        lines = f.readlines()
        for line in lines:
            groups.append(line.strip().split(","))

    return groups


# ====================================================================================
def getColumnsNames(columns):
    res = []
    for i in range(len(columns)):
        index = str(i+1)
        # if len(index) == 1:
        #     index = "0" + index
        res.append("phase" + index)

    return res


def saveFluksoDataToCassandra(homes):
    """
    Save flukso data to Cassandra cluster
    """
    print("saving in Cassandra...")
    session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

    for hid, home in homes.items():
        print(hid)
        power_df = home.getPowerDF()

        col_names = ["home_id", "day", "ts"] + getColumnsNames(power_df.columns)
        # print(col_names)
        for timestamp, row in power_df.iterrows():
            # print(timestamp, list(row))
            day = str(timestamp.date())
            ts = str(timestamp)
            values = [hid, day, ts] + list(row)
            ptc.insert(session, CASSANDRA_KEYSPACE, "raw_data", col_names, values)

    print("Successfully Saved in Cassandra")


# ====================================================================================


def main():
    # TODO : add argument for choosing between different features (visualizeFluksoData, identifyPhaseState)
    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    argparser.add_argument("--since",
                           type=str,
                           default="",
                           help="Period to query until now, e.g. "
                                "'30days', "
                                "'1hours', "
                                "'20min',"
                                "'s2021-12-06-16-30-00', etc. Defaults to all data.")

    argparser.add_argument("--to",
                           type=str,
                           default="",
                           help="Query a defined interval, e.g. "
                                "'2021-10-29-00-00-00>2021-10-29-23-59-52'")

    args = argparser.parse_args()
    since = args.since
    print("since : ", since)

    to = args.to
    print("to: ", to)

    # =========================================================

    start_timing = getTiming(since)
    to_timing = getTiming(to)
    sensors, session = getFluksoData(UPDATED_SENSORS_FILE)

    home_ids = set(sensors["home_ID"])
    nb_homes = len(home_ids)
    print("Number of Homes : ", nb_homes)
    print("Number of Fluksos : ", len(sensors))

    # =========================================================

    groups = getFLuksoGroups()
    print("groups : ", groups)
    homes = generateHomes(session, sensors, since, start_timing, to_timing, home_ids)
    # grouped_homes = generateGroupedHomes(homes, groups)

    saveFluksoDataToCassandra(homes)

    # 29-10-2021 from Midnight to midnight :
    # --since s2021-10-29-00-00-00 --to s2021-10-30-00-00-00
    # 17-12-2021 from Midnight to midnight :
    # --since s2021-12-17-00-00-00 --to s2021-12-18-00-00-00


if __name__ == "__main__":
    main()
