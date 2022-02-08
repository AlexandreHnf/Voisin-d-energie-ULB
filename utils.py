import pandas as pd
import os
from constants import *


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