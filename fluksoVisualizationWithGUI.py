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

import pandas as pd
import numpy as np
import tmpo
import random
import matplotlib.pyplot as plt

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
OUTPUT_FILE = "output/output.csv"
UPDATED_SENSORS_FILE = "sensors/updated_sensors.csv"
FREQ = [8, "S"]  # 8 sec.


# =================================================


class Window(QtWidgets.QWidget):

    def __init__(self, homes):
        super(Window, self).__init__()

        self.homes = homes

        self.initUI()

    def initUI(self):
        self.setGeometry(1000, 1000, 1000, 1000)
        self.center()
        self.setWindowTitle('Flukso visualization')

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

        self.plot1()

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
        for col in power_df.columns:
            ax.plot(power_df[col], label=col)

        ax.set_title('Electricity consumption over time - home {0} - since {1}'
                     .format(home.getHomeID(), home.getSince()))
        ax.set_xlabel("Time (t)")
        ax.set_ylabel("Power (kiloWatts) - KW")
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
            ax.plot(cons_prod_df[col], label=col)

        # show the positive and negative areas defined by the total power consumption line (P_tot)
        timestamps = cons_prod_df.index
        p_tot = cons_prod_df["P_tot"]

        # positive (green)
        plt.fill_between(timestamps, p_tot, where=(p_tot > 0), color='r', alpha=0.3)
        # negative (red)
        plt.fill_between(timestamps, p_tot, where=(p_tot < 0), color='g', alpha=0.3)

        ax.set_title("Power consumption & production over time - home {0} - since {1}"
                     .format(home.getHomeID(), home.getSince()))
        ax.set_xlabel("Time (t)")
        ax.set_ylabel("Power (kiloWatts) - KW")

        # custom legend for injection and taking (prélèvement)
        custom_lines = [Line2D([0], [0], color="salmon", lw=4),
                        Line2D([0], [0], color="limegreen", lw=4)]

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

    def plot1(self):
        """ plot some random stuff """
        # random data

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

    def __init__(self, session, sensors, since, since_timing, indexes, home_id):
        self.session = session
        self.home_sensors = sensors.loc[sensors["home_ID"] == home_id]
        self.since = since
        self.since_timing = since_timing
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

    def getSince(self):
        return self.since

    def getSinceTiming(self):
        return self.since_timing

    def getColumnsTotal(self):
        return self.power_df.sum(axis=0, numeric_only=True)

    def energy2power(self, energy_df):
        """
        from cumulative energy to power (Watt)
        """
        power_df = energy_df.diff() * 1000
        power_df.fillna(0, inplace=True)
        power_df = power_df.resample(str(FREQ[0]) + FREQ[1]).mean()
        return power_df

    def getZeroSeries(self):
        period = pd.Timedelta(self.since).total_seconds() / FREQ[0]
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
            print("{} :".format(id))
            # print("- first timestamp : {}".format(self.session.first_timestamp(id)))
            # print("- last timestamp : {}".format(self.session.last_timestamp(id)))

            dff = self.session.series(id, head=self.since_timing)
            if len(dff.index) == 0:
                dff = self.getZeroSeries()
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
        print("nb index : ", len(energy_df.index))
        energy_df.index = pd.DatetimeIndex(energy_df.index, name="time")
        energy_df.columns = self.home_sensors.index
        power_df = self.energy2power(energy_df)
        del energy_df

        power_df.index = getLocalTimestampsIndex(power_df)

        power_df.fillna(0, inplace=True)

        print("nb elements : ", len(power_df.index))
        print("=======> 10 first elements : ")
        print(power_df.head(10))

        return power_df

    def getConsumptionProductionDF(self):
        cons_prod_df = pd.DataFrame([[0, 0, 0] for _ in range(len(self.power_df))],
                                    self.power_df.index,
                                    ["P_cons", "P_prod", "P_tot"])

        print(cons_prod_df.head(4))

        for i in range(len(self.indexes["+"])):
            cons_prod_df["P_cons"] = cons_prod_df["P_cons"] + self.power_df[self.indexes["+"][i]]
            cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + self.power_df[self.indexes["-"][i]]

        # TEST : TEMPORARY
        # cons_prod_df["P_prod"] = [random.randint(-500, 500) for _ in range(len(cons_prod_df))]

        cons_prod_df["P_tot"] = cons_prod_df["P_cons"] - cons_prod_df["P_prod"]
        print("after sums :")
        print(cons_prod_df.head(4))

        return cons_prod_df


# ====================================================================================


def read_sensor_info(path):
    """
    read csv file of sensors data
    """
    path += SENSOR_FILE
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


def getTiming(since):
    """
    get the timestamp of the "since"
    ex : the timestamp 20 min ago
    """
    print("since {}".format(since))
    if not since:
        since_timing = 0
    else:
        since_timing = pd.Timestamp.now(tz="UTC") - pd.Timedelta(since)
        print("timing in sec : ", pd.Timedelta(since).total_seconds())
        print("Since : ", since_timing, type(since_timing))

    print("since timing : ", since_timing)
    return since_timing


def getPhasesIndexes(sensors, nb_homes):
    indexes = {}
    for hid in range(1, nb_homes + 1):
        indexes[hid] = {}
        home_df = sensors.loc[sensors["home_ID"] == hid]
        indexes[hid]["+"] = list(home_df.loc[home_df['state'] == "+"].index)
        indexes[hid]["-"] = list(home_df.loc[home_df['state'] == "-"].index)

    return indexes


def getProgDir():
    import __main__
    main_path = os.path.abspath(__main__.__file__)
    main_path = os.path.dirname(main_path) + os.sep
    return main_path


def visualizeFluksoData(nb_homes, session, sensors, since, since_timing, indexes):
    plt.style.use('ggplot')  # plot style

    homes = []

    for hid in range(1, nb_homes + 1):
        print("========================= HOME {} =====================".format(hid))
        home = Home(session, sensors, since, since_timing, indexes[hid], hid)
        homes.append(home)

    # launch window with flukso visualization (using PYQT GUI)
    app = QtWidgets.QApplication(sys.argv)
    app.aboutToQuit.connect(app.deleteLater)
    GUI = Window(homes)
    sys.exit(app.exec_())


def identifyPhaseState(path, nb_homes, session, sensors, since, since_timing, indexes):
    states_df = pd.DataFrame(columns=["tot_power", "state"])

    for hid in range(1, nb_homes + 1):
        col_names = indexes[hid]["+"] + indexes[hid]["-"]
        home = Home(session, sensors, since, since_timing, indexes[hid], hid)
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


def getFluksoData(path="", since=""):
    """
    get Flukso data (via API) then visualize the data
    """
    if not path:
        path = getProgDir()

    since_timing = getTiming(since)

    sensors = read_sensor_info(path)
    print(sensors.head(5))
    session = tmpo.Session(path)
    for hid, hn, sid, tk, st in sensors.values:
        session.add(sid, tk)

    session.sync()

    return sensors, session, since_timing


def main():
    # TODO : add argument for choosing between different features (visualizeFluksoData, identifyPhaseState)
    argparser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    argparser.add_argument("--since",
                           type=str,
                           default="",
                           help="Period to query until now, e.g. '30days', '1hours', '20min' etc. Defaults to all data.")

    args = argparser.parse_args()
    since = args.since

    sensors, session, since_timing = getFluksoData(since=since)

    nb_homes = len(set(sensors["home_ID"]))
    print("Homes : ", nb_homes)

    indexes = getPhasesIndexes(sensors, nb_homes)
    print("indexes : ", indexes)

    # =========================================================

    visualizeFluksoData(nb_homes, session, sensors, since, since_timing, indexes)
    # visualizeFluksoData(1, session, sensors, since, since_timing, indexes)
    # identifyPhaseState(getProgDir(), nb_homes, session, sensors, since, since_timing, indexes)


if __name__ == "__main__":
    main()
