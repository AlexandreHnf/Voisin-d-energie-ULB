#####################################################################################
#                                                                                   #
#                PLOT A LIVE GRAPH IN A PYQT WINDOW                                 #
#                EXAMPLE 1                                                          #
#               ------------------------------------                                #
# This code is inspired on:                                                         #
# https://matplotlib.org/3.1.1/gallery/user_interfaces/embedding_in_qt_sgskip.html  #
# from https://stackoverflow.com/questions/57891219/how-to-make-a-fast-matplotlib-live-plot-in-a-pyqt5-gui                                                                                  #
#####################################################################################


import datetime as dt
from typing import *
import sys
import os
from PyQt5 import QtWidgets, QtCore
# from matplotlib.backends.backend_qt5agg import FigureCanvas
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.figure as mpl_fig
import matplotlib.animation as anim
import numpy as np
import random


class ApplicationWindow(QtWidgets.QMainWindow):
    '''
    The PyQt5 main window.

    '''
    def __init__(self):
        super().__init__()
        # 1. Window settings
        self.setGeometry(300, 300, 800, 400)
        self.setWindowTitle("Matplotlib live plot in PyQt - example 1")
        self.frm = QtWidgets.QFrame(self)
        self.frm.setStyleSheet("QWidget { background-color: #eeeeec; }")
        self.lyt = QtWidgets.QVBoxLayout()
        self.frm.setLayout(self.lyt)
        self.setCentralWidget(self.frm)

        # 2. Place the matplotlib figure
        # self.myFig = MyFigureCanvas(x_len=200, y_range=[0, 100], interval=20)
        self.myFig = MyFigureCanvas(x_len=20, y_range=[0, 100], interval=1000)
        self.lyt.addWidget(self.myFig)

        # Format plot
        self.myFig.
        plt.xticks(rotation=45, ha='right')
        plt.subplots_adjust(bottom=0.30)

        # 3. Show
        self.show()
        return

class MyFigureCanvas(FigureCanvas):
    '''
    This is the FigureCanvas in which the live plot is drawn.

    '''
    def __init__(self, x_len: int, y_range: List, interval: int) -> None:
        '''
        :param x_len:       The nr of data points shown in one plot.
        :param y_range:     Range on y-axis.
        :param interval:    Get a new datapoint every .. milliseconds.

        '''
        super().__init__(mpl.figure.Figure())
        # Range settings
        self._x_len_ = x_len
        self._y_range_ = y_range

        # Store two lists _x_ and _y_
        self._x_ = list(range(0, x_len))
        self._y_ = [0] * x_len

        # Store a figure ax
        self._ax_ = self.figure.subplots()

        # Initiate the timer
        self._timer_ = self.new_timer(interval, [(self._update_canvas_, (), {})])
        self._timer_.start()
        return

    def _update_canvas_(self) -> None:
        '''
        This function gets called regularly by the timer.

        '''
        # self._y_.append(round(get_next_datapoint(), 2))     # Add new datapoint
        self._y_.append(random.randint(20, 40))          # Add new datapoint
        self._x_.append(dt.datetime.now().strftime('%H:%M:%S.%f'))
        self._y_ = self._y_[-self._x_len_:]                 # Truncate list _y_
        self._x_ = self._x_[-self._x_len_:]
        self._ax_.clear()                                   # Clear ax
        self._ax_.plot(self._x_, self._y_)                  # Plot y(x)
        # self._ax_.set_ylim(ymin=self._y_range_[0], ymax=self._y_range_[1])

        # Format plot
        # plt.xticks(rotation=45, ha='right')
        # plt.subplots_adjust(bottom=0.30)
        self._ax_.set_title('TMP102 Temperature over Time')
        self._ax_.set_ylabel('Temperature (deg C)')

        self.draw()
        return

# Data source
# ------------
n = np.linspace(0, 499, 500)
d = 50 + 25 * (np.sin(n / 8.3)) + 10 * (np.sin(n / 7.5)) - 5 * (np.sin(n / 1.5))
i = 0
def get_next_datapoint():
    global i
    i += 1
    if i > 499:
        i = 0
    return d[i]

if __name__ == "__main__":
    qapp = QtWidgets.QApplication(sys.argv)
    app = ApplicationWindow()
    qapp.exec_()
