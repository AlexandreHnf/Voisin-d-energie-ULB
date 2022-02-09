import pandas as pd
import os
import math
from constants import *


def read_sensor_info(path, sensor_file):
	"""
	read csv file of sensors data
	"""
	path += sensor_file
	sensors = pd.read_csv(path, header=0, index_col=1)
	return sensors


def setInitSeconds(ts):
	""" 
	SS = 00 if M even, 04 if odd
	"""
	minute = ts.minute
	sec = "00"
	if minute % 2 != 0: # odd
		sec = "04"
	ts = ts.replace(second=int(sec))
	return ts


def getTiming(t, now):
	"""
	get the timestamp of the "since"
	format : YY-MM-DD H-M-S UTC 
	ex : the timestamp 20 min ago
	"""
	# print("since {}".format(since))
	timing = 0
	if t:
		if t[0] == "s":
			e = t[1:].split("-")
			timing = pd.Timestamp(year=int(e[0]), month=int(e[1]), day=int(e[2]),
								  hour=int(e[3]), minute=int(e[4]), second=int(e[5]), 
								  tz="CET").tz_convert("UTC")
		else:
			print("time delta : ", pd.Timedelta(t))
			timing = now - pd.Timedelta(t)
	else:
		timing = now  # now

	# print("timing : ", timing)
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


def getLocalTimestampsIndex(df):
    """
    set timestamps to local timezone
    """

    # NAIVE
    if df.index.tzinfo is None or df.index.tzinfo.utcoffset(df.index) is None:
        # first convert to aware timestamp, then local
        return df.index.tz_localize("CET").tz_convert("CET")
    else: # if already aware timestamp
        return df.index.tz_convert("CET")


def toEpochs(time):
	return int(math.floor(time.value / 1e9))