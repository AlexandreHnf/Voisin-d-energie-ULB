from PyQt4 import QtCore, QtGui
import os
import sys

from matplotlib.backends.backend_qt4agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt4agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure

qapp = QtGui.QApplication(sys.argv)

qwidget = QtGui.QWidget()
qwidget.setGeometry(QtCore.QRect(0, 0, 500, 500))
qlayout = QtGui.QHBoxLayout(qwidget)
qwidget.setLayout(qlayout)

qscroll = QtGui.QScrollArea(qwidget)
qscroll.setGeometry(QtCore.QRect(0, 0, 500, 500))
qscroll.setFrameStyle(QtGui.QFrame.NoFrame)
qlayout.addWidget(qscroll)

qscrollContents = QtGui.QWidget()
qscrollLayout = QtGui.QVBoxLayout(qscrollContents)
qscrollLayout.setGeometry(QtCore.QRect(0, 0, 1000, 1000))

qscroll.setWidget(qscrollContents)
qscroll.setWidgetResizable(True)

for i in range(5):
    qfigWidget = QtGui.QWidget(qscrollContents)

    fig = Figure((5.0, 4.0), dpi=100)
    canvas = FigureCanvas(fig)
    canvas.setParent(qfigWidget)
    toolbar = NavigationToolbar(canvas, qfigWidget)
    axes = fig.add_subplot(111)
    axes.plot([1, 2, 3], [4, 5, 6])

    # place plot components in a layout
    plotLayout = QtGui.QVBoxLayout()
    plotLayout.addWidget(canvas)
    plotLayout.addWidget(toolbar)
    qfigWidget.setLayout(plotLayout)

    # prevent the canvas to shrink beyond a point
    # original size looks like a good minimum size
    canvas.setMinimumSize(canvas.size())

    qscrollLayout.addWidget(qfigWidget)

qscrollContents.setLayout(qscrollLayout)

qwidget.show()
exit(qapp.exec_())
