"""
Authors :
    - Guillaume Levasseur
    - Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and
    - export it as a CSV file.
    - visualize it (time series)

Input
-----
This script requires to have a file named `sensors.csv` in the "sensors/" directory.
This `sensors.csv` file contains the home_ID, home name, sensor ID, sensor token and
state for each row.

Example input:

> cat sensors.csv
home_ID,phase,home_name,sensor_id,token,state
1,phase1+,Flukso1,fed676021dacaaf6a12a8dda7685be34,b371402dc767cc83e41bc294b63f9586,+

Output
------
File `output.csv` in output/
/!\ The previous file will be overwritten!
The file contains the power time series, each sensor/phase a column.
"""

import argparse
import os
import sys

import copy
import pandas as pd
import numpy as np
import tmpo
import random
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

# PYQT
from PyQt5.QtCore import QRect
from PyQt5 import QtGui, QtWidgets
from PyQt5.QtWidgets import (QWidget,
                             QPushButton,
                             QScrollArea,
                             QHBoxLayout,
                             QVBoxLayout,
                             QApplication)
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt4agg import NavigationToolbar2QT as NavigationToolbar

from matplotlib.lines import Line2D

# constants
SENSOR_FILE = "sensors/sensors.csv"
OUTPUT_FILE = 'output/fluksoData/'
UPDATED_SENSORS_FILE = "sensors/updated_sensors.csv"
GROUPS_FILE = "sensors/grouped_homes_sensors.txt"
FREQ = [8, "S"]  # 8 sec.


# =================================================


class Window(QtWidgets.QWidget):

    def __init__(self, homes, window_name):
        super(Window, self).__init__()

        self.window_name = window_name
        self.homes = homes

        self.initUI()

    def initUI(self):
        self.setGeometry(1000, 1000, 1000, 1000)
        self.center()
        self.setWindowTitle(self.window_name)

        # ==========
        qlayout = QHBoxLayout(self)
        self.setLayout(qlayout)

        qscroll = QScrollArea(self)
        qlayout.addWidget(qscroll)

        self.qscrollContents = QWidget()
        self.qscrollLayout = QVBoxLayout(self.qscrollContents)
        self.qscrollLayout.setGeometry(QRect(0, 0, 100, 100))

        qscroll.setWidget(self.qscrollContents)
        qscroll.setWidgetResizable(True)
        # ==========

        self.plot()

        self.qscrollContents.setLayout(self.qscrollLayout)

        self.show()

    def showTimeSeries(self, home, qfigWidget):
        """
        show time series : x = time, y = power (Watt)
        """

        vlayout = QVBoxLayout()  # vertical
        vlayout.setGeometry(QRect(0, 0, 100, 100))

        fig = plt.figure(figsize=(15, 5))
        canvas = FigureCanvas(fig)
        canvas.setParent(qfigWidget)
        toolbar = NavigationToolbar(canvas, qfigWidget)
        ax = fig.add_subplot(111)
        ax.clear()

        power_df = home.getPowerDF()
        # print("== >", power_df.index[0], power_df.index[-1])
        for col in power_df.columns:
            ax.plot(power_df.index, power_df[col], label=col)

        ax.set_title('Electricity consumption over time - home {0} - since {1}'
                     .format(home.getHomeID(), home.getSince()))
        ax.set_xlabel("Time (t)")
        ax.set_ylabel("Power (Watts) - W")
        ax.legend(loc="upper right", fancybox=True)
        canvas.resize(400, 400)

        # place plot components in a layout

        # prevent the canvas to shrink beyond a point
        # original size looks like a good minimum size
        canvas.setMinimumSize(canvas.size())

        vlayout.addWidget(canvas)
        vlayout.addWidget(toolbar)

        return vlayout

    def showConsProdSeries(self, home, qfigWidget):
        """
        show power consumption and production (PV) w.r.t. time
        + total power consumption
        """

        vlayout = QVBoxLayout()  # vertical
        vlayout.setGeometry(QRect(0, 0, 100, 100))

        fig = plt.figure(figsize=(15, 5))
        canvas = FigureCanvas(fig)
        canvas.setParent(qfigWidget)
        toolbar = NavigationToolbar(canvas, qfigWidget)
        ax = fig.add_subplot(111)
        ax.clear()

        cons_prod_df = home.getConsProdDF()
        for col in cons_prod_df.columns:
            ax.plot(cons_prod_df.index, cons_prod_df[col], label=col)

        # show the positive and negative areas defined by the total power consumption line (P_tot)
        timestamps = cons_prod_df.index
        p_tot = cons_prod_df["P_tot"]

        # daltonian colors :
        # '#377eb8','#ff7f00','#4daf4a','#f781bf','#a65628','#984ea3','#999999','#e41a1c','#dede00'

        # positive (yellow)
        plt.fill_between(timestamps, p_tot, where=(p_tot > 0), color='#dede00', alpha=0.3)
        # negative (light green)
        plt.fill_between(timestamps, p_tot, where=(p_tot < 0), color='#4daf4a', alpha=0.3)

        ax.set_title("Power consumption & production - home {0} - since {1}"
                     .format(home.getHomeID(), home.getSince()))
        ax.set_xlabel("Time (t)")
        ax.set_ylabel("Power (Watts) - W")

        # custom legend for injection and taking (prélèvement)
        custom_lines = [Line2D([0], [0], color="#dede00", lw=4),  # yellow
                        Line2D([0], [0], color="#4daf4a", lw=4)]  # light green

        legend1 = ax.legend(loc="upper right", fancybox=True, framealpha=0.4)
        legend2 = ax.legend(handles=custom_lines, labels=["Prélèvement", "Injection"],
                            loc=4, fancybox=True, framealpha=0.4)
        plt.gca().add_artist(legend1)
        plt.gca().add_artist(legend2)

        canvas.resize(400, 400)

        # place plot components in a layout

        # prevent the canvas to shrink beyond a point
        # original size looks like a good minimum size
        canvas.setMinimumSize(canvas.size())

        vlayout.addWidget(canvas)
        vlayout.addWidget(toolbar)

        return vlayout

    def plot(self):
        """
        plot the 2 columns
        | power over time | network consumption/injection |
        """

        for home in self.homes:
            qfigWidget = QWidget(self.qscrollContents)

            plotLayout = QHBoxLayout()  # o  |  o  |  o  |

            plotLayout.addLayout(self.showTimeSeries(home, qfigWidget))
            plotLayout.addLayout(self.showConsProdSeries(home, qfigWidget))

            qfigWidget.setLayout(plotLayout)

            self.qscrollLayout.addWidget(qfigWidget)

    def center(self):
        qr = self.frameGeometry()
        cp = QtWidgets.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())


# ================================================================


class Home:

    def __init__(self, session, sensors, since, since_timing, to_timing, indexes, home_id):
        self.session = session
        self.home_sensors = sensors.loc[sensors["home_ID"] == home_id]
        self.since = since
        self.since_timing = since_timing
        self.to_timing = to_timing
        self.indexes = indexes
        self.home_id = home_id

        self.power_df = self.createFluksoPowerDF()
        self.cons_prod_df = self.getConsumptionProductionDF()

    def getPowerDF(self):
        return self.power_df

    def getConsProdDF(self):
        return self.cons_prod_df

    def getHomeID(self):
        return self.home_id

    def setHomeID(self, hid):
        self.home_id = hid

    def getNbFluksoSensors(self):
        return len(self.power_df.columns)

    def getSince(self):
        return self.since

    def getSinceTiming(self):
        return self.since_timing

    def getToTiming(self):
        return self.to_timing

    def getColumnsTotal(self):
        return self.power_df.sum(axis=0, numeric_only=True)

    @staticmethod
    def energy2power(energy_df):
        """
        from cumulative energy to power (Watt)
        """
        power_df = energy_df.diff() * 1000
        power_df.fillna(0, inplace=True)
        power_df = power_df.resample(str(FREQ[0]) + FREQ[1]).mean()
        return power_df

    def getZeroSeries(self):
        to = pd.Timestamp.now(tz="UTC")
        if self.to_timing != 0:
            to = self.to_timing

        period = (to - self.since_timing).total_seconds() / FREQ[0]
        # print("s : {}, t : {}, to : {}, period : {}".format(self.since_timing, self.to_timing, to, period))
        zeros = pd.date_range(self.since_timing, periods=period, freq=str(FREQ[0]) + FREQ[1])
        # print("datetime range : ", zeros)
        zeros_series = pd.Series(int(period) * [0], zeros)
        # print("zeros series :", zeros_series)

        return zeros_series

    def createSeries(self):
        """
        create a list of time series from the sensors data
        """
        data_dfs = []

        for id in self.home_sensors.sensor_id:
            # print("{} :".format(id))
            # print("- first timestamp : {}".format(self.session.first_timestamp(id)))
            # print("- last timestamp : {}".format(self.session.last_timestamp(id)))

            if self.to_timing == 0:
                dff = self.session.series(id, head=self.since_timing)
            else:
                dff = self.session.series(id, head=self.since_timing, tail=self.to_timing)
            if len(dff.index) == 0:
                dff = self.getZeroSeries()
                print("--> zeros")
            data_dfs.append(dff)

        return data_dfs

    def createFluksoPowerDF(self):
        """
        create a dataframe where the colums are the phases of the Flukso and the rows are the
        data : 1 row = 1 timestamp = 1 power value
        """
        data_dfs = self.createSeries()
        energy_df = pd.concat(data_dfs, axis=1)
        del data_dfs
        # print("nb index : ", len(energy_df.index))
        energy_df.index = pd.DatetimeIndex(energy_df.index, name="time")
        energy_df.columns = self.home_sensors.index
        power_df = self.energy2power(energy_df)
        del energy_df

        ah = getLocalTimestampsIndex(power_df)
        ah = [tps for tps in ah]
        # print(ah[0], ah[-1])
        power_df.index = ah


        power_df.fillna(0, inplace=True)

        # print("nb elements : ", len(power_df.index))
        # print("=======> 10 first elements : ")
        # print(power_df.head(10))

        return power_df

    def getConsumptionProductionDF(self):
        cons_prod_df = pd.DataFrame([[0, 0, 0] for _ in range(len(self.power_df))],
                                    self.power_df.index,
                                    ["P_cons", "P_prod", "P_tot"])

        # P_cons = P_tot - P_prod
        # P_net = P_prod + P_cons

        # ===============

        for phase in self.home_sensors.index:
            # c = self.home_sensors.loc[phase]["con"]
            p = self.home_sensors.loc[phase]["pro"]
            n = self.home_sensors.loc[phase]["net"]

            # cons_prod_df["P_cons"] = cons_prod_df["P_cons"] + c * self.power_df[phase]
            cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * self.power_df[phase]
            cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * self.power_df[phase]

            print(n, p)

        cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

        return cons_prod_df

    def getColNamesWithID(self, df, home_id):
        col_names = df.columns
        new_col_names = {}
        for col_name in col_names:
            new_col_names[col_name] = "{}_{}".format(home_id, col_name)

        return new_col_names

    def appendFluksoData(self, power_df, home_id):
        print(len(self.power_df), len(power_df))

        if not self.home_id in self.power_df.columns[0]:
            self.power_df = self.power_df.rename(self.getColNamesWithID(self.power_df, self.home_id), axis=1)
        power_df = power_df.rename(self.getColNamesWithID(power_df, home_id), axis=1)
        print(self.power_df.head(2))
        print(power_df.head(2))

        self.power_df = self.power_df.join(power_df)

    def addConsProd(self, cons_prod_df):
        self.cons_prod_df = self.cons_prod_df.add(cons_prod_df, fill_value=0)


# ====================================================================================


def read_sensor_info(path):
    """
    read csv file of sensors data
    """
    path += UPDATED_SENSORS_FILE
    sensors = pd.read_csv(path, header=0, index_col=1)
    return sensors


def getLocalTimestampsIndex(power_df):
    """
    set timestamps to local timezone
    """

    # NAIVE
    if power_df.index.tzinfo is None or power_df.index.tzinfo.utcoffset(power_df.index) is None:
        # first convert to aware timestamp, then local
        return power_df.index.tz_localize("CET").tz_convert("CET")
    else:
        return power_df.index.tz_convert("CET")


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


def getPhasesIndexes(sensors, home_ids):
    """
    return a dictionary of the form :
    {hid1 : {"cons": []}}
    """
    indexes = {}
    for hid in home_ids:
        indexes[hid] = {"cons": {}, "prod": {}}
        home_df = sensors.loc[sensors["home_ID"] == hid]
        indexes[hid]["cons"]["+"] = list(home_df.loc[home_df['state'] == "++"].index)
        indexes[hid]["prod"]["+"] = list(home_df.loc[home_df['state'] == "-+"].index)
        indexes[hid]["cons"]["-"] = list(home_df.loc[home_df['state'] == "+-"].index)
        indexes[hid]["prod"]["-"] = list(home_df.loc[home_df['state'] == "--"].index)

    return indexes


# def getHomePhases(sensors, home_ids):
#     """
#     return a dictionary of the form :
#     {hid: [fid1, fid2, ...]}
#     """
#     home_phases = {}
#     for hid in home_ids:
#         phases = list(sensors.loc[sensors["home_ID"] == hid].index)
#         home_phases[hid] = phases
#
#     return home_phases


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
    indexes = getPhasesIndexes(sensors, home_ids)
    # print("home phases: ", getHomePhases(sensors, home_ids))
    print("indexes : ", indexes)

    homes = {}

    # for hid in range(1, nb_homes + 1):
    for hid in home_ids:
        print("========================= HOME {} =====================".format(hid))
        home = Home(session, sensors, since, since_timing, to_timing, indexes[hid], hid)
        homes[hid] = home

    return homes


def generateGroupedHomes(homes, groups):
    grouped_homes = []
    for i, group in enumerate(groups):  # group = tuple (home1, home2, ..)
        print("========================= GROUP {} ====================".format(i + 1))
        home = copy.copy(homes[group[0]])
        for j in range(1, len(group)):
            home.appendFluksoData(homes[group[j]].getPowerDF(), homes[group[j]].getHomeID())
            home.addConsProd(homes[group[j]].getConsProdDF())
        home.setHomeID("group_" + str(i + 1))

        print(home.getPowerDF().head(1))
        grouped_homes.append(home)

    return grouped_homes


def visualizeFluksoData(homes, grouped_homes):
    print(homes)
    plt.style.use('ggplot')  # plot style
    plt.style.use("tableau-colorblind10")

    # launch window with flukso visualization (using PYQT GUI)
    app = QtWidgets.QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    GUI1 = Window(sortHomesByName(homes), 'Flukso visualization')
    GUI2 = Window(grouped_homes, 'Group Flukso visualization')
    sys.exit(app.exec_())
    # app.exec_()


def identifyPhaseState(path, home_ids, session, sensors, since, since_timing, to_timing):
    states_df = pd.DataFrame(columns=["tot_power", "state"])
    indexes = getPhasesIndexes(sensors, home_ids)

    for hid in home_ids:
        col_names = indexes[hid]["+"] + indexes[hid]["-"]
        # session, sensors, since, since_timing, to_timing, indexes, home_id
        home = Home(session, sensors, since, since_timing, to_timing, indexes[hid], hid)
        sums = home.getColumnsTotal()
        sums = sums.to_frame()  # to dataframe
        sums.columns = ["tot_power"]
        sums = sums.assign(state=np.where(sums["tot_power"] > 0.0, '+', '-'))
        states_df = states_df.append(sums)

        print("sums home " + str(hid))
        print(sums)

    updated_sensors_states = sensors
    updated_sensors_states["state"] = states_df["state"]

    print(states_df)
    print(updated_sensors_states)

    print(path + UPDATED_SENSORS_FILE)
    updated_sensors_states.to_csv(path + UPDATED_SENSORS_FILE)


def getFluksoData(path=""):
    """
    get Flukso data (via API) then visualize the data
    """
    if not path:
        path = getProgDir()

    sensors = read_sensor_info(path)
    print(sensors.head(5))
    session = tmpo.Session(path)
    for hid, hn, sid, tk, n, c, p, st in sensors.values:
        session.add(sid, tk)

    session.sync()

    return sensors, session


def getFLuksoGroups():
    """
    Groups format : [[home_ID1, home_ID2], [home_ID3, home_ID4], ...]
    """
    groups = []
    with open(GROUPS_FILE) as f:
        lines = f.readlines()
        for line in lines:
            groups.append(line.strip().split(","))

    return groups


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
    sensors, session = getFluksoData()

    home_ids = set(sensors["home_ID"])
    nb_homes = len(home_ids)
    print("Number of Homes : ", nb_homes)
    print("Number of Fluksos : ", len(sensors))

    # =========================================================

    groups = getFLuksoGroups()
    print("groups : ", groups)
    homes = generateHomes(session, sensors, since, start_timing, to_timing, home_ids)
    grouped_homes = generateGroupedHomes(homes, groups)

    # saveFluksoData(homes.values())
    # saveFluksoData(grouped_homes)

    visualizeFluksoData(homes, grouped_homes)

    # identifyPhaseState(getProgDir(), home_ids, session, sensors, "", 0, 0)

    # 29-10-2021 from Midnight to midnight (-2 for the UTC timestamps) :
    # --since s2021-10-28-22-00-00 --to s2021-10-29-22-00-00
    # --since s2021-10-29-00-00-00 --to s2021-10-30-00-00-00


if __name__ == "__main__":
    main()
