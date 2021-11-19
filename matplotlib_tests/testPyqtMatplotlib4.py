import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import random
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


class Window(QtWidgets.QWidget):


    def __init__(self):
        super(Window, self).__init__()
        self.initUI()


    def initUI(self):

        self.setGeometry(1000, 1000, 1000, 1000)
        self.center()
        self.setWindowTitle('Flukso visualization')

        # ==========
        qlayout = QHBoxLayout(self)
        self.setLayout(qlayout)

        qscroll = QScrollArea(self)
        # qscroll.setGeometry(0, 0, 0, 0)
        # qscroll.setFrameStyle(QtGui.QFrame.NoFrame)
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


    def plot1(self):
        """ plot some random stuff """
        # random data

        data = [random.random() for _ in range(10)]
        data2 = [random.random() for _ in range(10)]

        for i in range(5):

            qfigWidget = QWidget(self.qscrollContents)
            plotLayout = QHBoxLayout()
            plotLayout.setGeometry(QRect(0, 0, 100, 100))

            for _ in range(2):
                fig = plt.figure(figsize=(15, 5))
                canvas = FigureCanvas(fig)
                canvas.setParent(qfigWidget)
                toolbar = NavigationToolbar(canvas, qfigWidget)
                axes = fig.add_subplot(111)
                axes.plot(data, '*-')
                axes.plot(data2, '*-')

                # place plot components in a layout

                plotLayout.addWidget(canvas)
                plotLayout.addWidget(toolbar)

                # prevent the canvas to shrink beyond a point
                # original size looks like a good minimum size
                canvas.setMinimumSize(canvas.size())

            qfigWidget.setLayout(plotLayout)

            self.qscrollLayout.addWidget(qfigWidget)


    def plot2(self):
        since_timing = "2021-11-15 15:33:26.895163+00:00"
        period = 1200
        zeros = pd.date_range(since_timing, periods=period, freq="1S")
        # print("datetime range : ", zeros)
        zeros_series = pd.Series(int(period) * [0], zeros)

        # create an axis
        ax = self.figure.add_subplot(111)

        # discards the old graph
        ax.clear()

        # plot data
        ax.plot(zeros_series)

        # refresh canvas
        self.canvas.draw()

    def center(self):
        qr = self.frameGeometry()
        cp = QtWidgets.QDesktopWidget().availableGeometry().center()
        qr.moveCenter(cp)
        self.move(qr.topLeft())



app = QtWidgets.QApplication(sys.argv)
app.aboutToQuit.connect(app.deleteLater)
GUI = Window()
sys.exit(app.exec_())