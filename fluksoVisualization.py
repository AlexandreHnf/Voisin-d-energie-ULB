"""
Authors :
    - Guillaume Levasseur
    - Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and
    - export it as a CSV file.
    - visualize it (time series)

Input
-----
This script requires to have a file named `sensors.csv` or `updated_sensors.csv` in the "sensors/" directory.
This `sensors.csv` file contains the home_ID, home name, sensor ID, sensor token and
coefficients for network, consommation and production for each row.

Example input:

> cat sensors.csv or updated_sensors.csv
home_ID,phase,home_name,sensor_id,token,net,con,pro
1,phase1+,Flukso1,fed676021dacaaf6a12a8dda7685be34,b371402dc767cc83e41bc294b63f9586,-1.0,0.0,0.0

Output
------
File `[installation_id].csv` in output/fluksoData
/!\ The previous file will be overwritten!
The file contains the power time series, each sensor/phase a column.
"""

from home import *
from gui import *
from constants import *
import webapp.pyToCassandra as ptc

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


def saveFluksoData(homes):
    print("saving...")
    for home in homes:
        # filepath = "output/fluksoData/{}.csv".format(home.getHomeID())
        power_df = home.getPowerDF()
        cons_prod_df = home.getConsProdDF()

        combined_df = power_df.join(cons_prod_df)

        outname = home.getHomeID() + '.csv'
        outdir = OUTPUT_FILE
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        filepath = os.path.join(outdir, outname)

        combined_df.to_csv(filepath)

    print("Successfully Saved")


def sortHomesByName(homes):
    """
    homes of the form : {"CDB001": home_object, ...}
    return the list of homes objects sorted by name
    """
    sorted_homes = []
    for hid in sorted(homes):
        sorted_homes.append(homes[hid])

    return sorted_homes


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


def visualizeFluksoData(homes, grouped_homes):
    plt.style.use('ggplot')  # plot style
    plt.style.use("tableau-colorblind10")

    # launch window with flukso visualization (using PYQT GUI)
    app = QtWidgets.QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    GUI1 = Window(sortHomesByName(homes), 'Flukso visualization')
    GUI2 = Window(grouped_homes, 'Group Flukso visualization')
    sys.exit(app.exec_())
    # app.exec_()


def getFluksoData(sensor_file, path=""):
    """
    get Flukso data (via API) then visualize the data
    """
    if not path:
        path = getProgDir()

    sensors = read_sensor_info(path, sensor_file)
    print(sensors.head(5))
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


def showFluksosActivity(homes):
    activity_df = None
    for hid, home in homes.items():
        fluksos_df = home.getAggregatedDataPerFlukso()

        if activity_df is None:
            activity_df = fluksos_df
        else:
            activity_df = activity_df.join(fluksos_df)

    i = 10
    ticks = []
    for fid in activity_df.columns:
        ticks.append(i)
        activity_df[fid] = activity_df[fid].mask(activity_df[fid] > 0.0, i)
        activity_df[fid] = activity_df[fid].mask(activity_df[fid] == 0.0, 0)
        i += 10

    activity_df.fillna(0, inplace=True)

    # plot flukso activity
    activity_df.plot(color="black",
                     linewidth=0,
                     marker='.',
                     markersize=3,
                     title='Flukso Activity plot',
                     legend=None)

    plt.yticks(ticks, activity_df.columns)
    plt.xlabel('Time')
    plt.ylabel('Flukso activity')
    plt.show()


# ====================================================================================


def getColumnsNames(columns):
    """ 
    get the right format for the columns names 
    " " => "_"
    "-" => "1"
    ex : PV sur phase L1- => pv_sur_phase_l11
    """
    names = []
    for col in columns:
        col = col.replace(" ", "_")
        col = col.replace("-", "1")
        names.append(col)
    return names 

def saveFluksoDataToCassandra(homes):
    """ 
    Save flukso data to Cassandra cluster
    """
    print("saving in Cassandra...")
    session = ptc.connectToCluster("test")

    for hid, home in homes.items():
        print(hid)
        power_df = home.getPowerDF()
        cons_prod_df = home.getConsProdDF()
        combined_df = power_df.join(cons_prod_df)

        col_names = ["timestamp"] + getColumnsNames(list(combined_df.columns))
        print(col_names)
        for timestamp, row in combined_df.iterrows():
            # print(timestamp, list(row))
            values = [str(timestamp)] + list(row)
            ptc.insert(session, "test", hid, col_names, values)

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

    # saveFluksoData(homes.values())
    # saveFluksoData(grouped_homes)

    saveFluksoDataToCassandra(homes)

    # visualizeFluksoData(homes, grouped_homes)

    # identifyPhaseState(getProgDir(), home_ids, session, sensors, "", 0, 0)

    # 29-10-2021 from Midnight to midnight :
    # --since s2021-10-29-00-00-00 --to s2021-10-30-00-00-00
    # 17-12-2021 from Midnight to midnight :
    # --since s2021-12-17-00-00-00 --to s2021-12-18-00-00-00

    # ACTIVITY PLOT
    # showFluksosActivity(homes)


if __name__ == "__main__":
    main()
