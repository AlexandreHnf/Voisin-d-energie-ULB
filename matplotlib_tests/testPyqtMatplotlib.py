import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import random
from PyQt5 import QtGui, QtWidgets
from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt4agg import NavigationToolbar2QT as NavigationToolbar


class PrettyWidget(QtWidgets.QWidget):


    def __init__(self):
        super(PrettyWidget, self).__init__()
        self.initUI()


    def initUI(self):

        self.setGeometry(100,100,800,600)
        self.center()
        self.setWindowTitle('S Plot')

        grid = QtWidgets.QGridLayout()
        self.setLayout(grid)

        # btn1 = QtWidgets.QPushButton('Plot 1 ', self)
        # btn1.resize(btn1.sizeHint())
        # btn1.clicked.connect(self.plot1)
        # grid.addWidget(btn1,5,0)
        #
        # btn2 = QtWidgets.QPushButton('Plot 2 ', self)
        # btn2.resize(btn2.sizeHint())
        # btn2.clicked.connect(self.plot4)
        # grid.addWidget(btn2,5,1)


        self.figure = plt.figure(figsize = (15,5))
        self.canvas = FigureCanvas(self.figure)
        self.toolbar = NavigationToolbar(self.canvas, self)
        grid.addWidget(self.canvas, 3,0,1,2)
        grid.addWidget(self.canvas, 3,0,1,2)

        self.plot4()

        self.show()

    def plot1(self):
        plt.cla()
        ax1 = self.figure.add_subplot(211)
        x1 = [i for i in range(100)]
        y1 = [i**0.5 for i in x1]
        ax1.plot(x1,y1,'b.-')

        ax2 = self.figure.add_subplot(212)
        x2 = [i for i in range(100)]
        y2 = [i for i in x2]
        ax2.plot(x2,y2,'b.-')
        self.canvas.draw()

    def plot2(self):
        plt.cla()
        ax3 = self.figure.add_subplot(111)
        x = [i for i in range(100)]
        y = [i**0.5 for i in x]
        ax3.plot(x,y,'r.-')
        ax3.set_title('Square Root Plot')
        self.canvas.draw()

    def plot3(self):
        """ plot some random stuff """
        # random data
        data = [random.random() for i in range(10)]
        data2 = [random.random() for i in range(10)]

        # create an axis
        ax = self.figure.add_subplot(111)

        # discards the old graph
        ax.clear()

        # plot data
        ax.plot(data, '*-')
        ax.plot(data2, '*-')

        # refresh canvas
        self.canvas.draw()

    def plot4(self):
        since_timing = "2021-11-15 15:33:26.895163+00:00"
        period = 1200
        zeros = pd.date_range(since_timing, periods=period, freq="1S")
        # print("datetime range : ", zeros)
        y = int(period) * [1]
        zeros_series = pd.Series(y, zeros)

        # create an axis
        ax = self.figure.add_subplot(111)

        # discards the old graph
        ax.clear()

        yy = zeros_series[0]
        plt.fill_between(zeros_series.index, yy, where=(yy > 0), color='g', alpha=0.3)

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
GUI = PrettyWidget()
sys.exit(app.exec_())