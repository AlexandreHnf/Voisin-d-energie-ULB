from constants import *

import pandas as pd

# PYQT
from PyQt5.QtCore import QRect
from PyQt5 import QtGui, QtWidgets
from PyQt5.QtWidgets import (QWidget,
                             QPushButton,
                             QScrollArea,
                             QHBoxLayout,
                             QVBoxLayout,
                             QApplication)

# myplotlib
import matplotlib.pyplot as plt

from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt4agg import NavigationToolbar2QT as NavigationToolbar

from matplotlib.lines import Line2D

# Hide warnings :
import matplotlib as mpl
import urllib3
import warnings

# max open warning
mpl.rc('figure', max_open_warning=0)

# security warning & Future warning
warnings.simplefilter('ignore', urllib3.exceptions.SecurityWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)


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