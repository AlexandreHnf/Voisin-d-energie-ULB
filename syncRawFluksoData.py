""" 
Author : Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and 
- format it into tables of time series
- save the raw data in Cassandra database
"""

from home import *
from gui import *
from constants import *
import pyToCassandra as ptc
from utils import * 
import computePower as cp

import argparse
import os
import sys

import copy
import pandas as pd
import numpy as np
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


def generateHomes(session, sensors, since, since_timing, to_timing, home_ids):
	homes = {}

	for hid in home_ids:
		print("========================= HOME {} =====================".format(hid))
		home = Home(session, sensors, since, since_timing, to_timing, hid)
		homes[hid] = home
		# print(home.getPowerDF().head(10))

	return homes


def generateGroupedHomes(homes, groups):
	grouped_homes = {}
	for i, group in enumerate(groups):  # group = tuple (home1, home2, ..)
		print("========================= GROUP {} ====================".format(i + 1))
		home = copy.copy(homes[group[0]])
		for j in range(1, len(group)):
			print(homes[group[j]].getHomeID())
			home.appendFluksoData(homes[group[j]].getPowerDF(), homes[group[j]].getHomeID())
			home.addConsProd(homes[group[j]].getConsProdDF())
		home.setHomeID("group_" + str(i + 1))

		grouped_homes["group" + str(i+1)] = home

	return grouped_homes


def getFluksoData(sensor_file, path=""):
	"""
	get Flukso data (via API) 
	"""
	if not path:
		path = getProgDir()

	sensors = read_sensor_info(path, sensor_file)
	# print(sensors.head(5))
	session = tmpo.Session(path)
	for hid, hn, sid, tk, n, c, p in sensors.values:
		session.add(sid, tk)

	session.sync()

	return sensors, session


# ====================================================================================

def updateIncompleteRows(to_timing, homes, table_name):
	"""
	Save raw data config to Cassandra cluster
	-> incomplete rows (with null values)
	"""
	print("saving in Cassandra...")
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	col_names = ["home_id", "day", "ts", "phases"]
	for hid, home in homes.items():
		print(hid)
		inc_power_df = home.getIncompletePowerDF()

		inc = []
		for timestamp, row in inc_power_df.iterrows():
			# if valid timestamp
			if (to_timing - timestamp).days < 2: # 2 days max
				day = str(timestamp.date())
				# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
				ts = str(timestamp)[:19] + "Z"
				inc_row = []
				for i in range(len(row)):
					if np.isnan(row[i]): # if null value (NaN)
						inc_row.append("phase"+str(i+1))
				values = [hid, day, ts, inc_row]
				# print("hid : {}, day: {}, ts: {}".format(hid, day, ts))
				# print(inc_row)
				ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	print("Successfully Saved raw config in Cassandra : table {}".format(table_name))


def getColumnsNames(columns):
	res = []
	for i in range(len(columns)):
		index = str(i+1)
		# if len(index) == 1:
		#     index = "0" + index
		res.append("phase" + index)

	return res


def saveRawFluksoDataToCassandra(homes, table_name):
	"""
	Save raw flukso data to Cassandra cluster
	"""
	print("saving in Cassandra...")
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	for hid, home in homes.items():
		print(hid)
		power_df = home.getPowerDF()

		col_names = ["home_id", "day", "ts"] + getColumnsNames(power_df.columns)
		for timestamp, row in power_df.iterrows():
			day = str(timestamp.date())
			# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
			ts = str(timestamp)[:19] + "Z"  
			values = [hid, day, ts] + list(row)
			ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	print("Successfully Saved raw data in Cassandra : table {}".format(table_name))


# ====================================================================================


def main():
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
	to = args.to
	now = pd.Timestamp.now(tz="UTC")

	start_timing = setInitSeconds(getTiming(since, now))
	to_timing = setInitSeconds(getTiming(to, now))
	sensors, session = getFluksoData(UPDATED_SENSORS_FILE)

	home_ids = set(sensors["home_ID"])
	nb_homes = len(home_ids)

	print("since : {} => {}".format(since, start_timing))
	print("to    : {} => {}".format(to, to_timing))
	print("Number of Homes : ", nb_homes)
	print("Number of Fluksos : ", len(sensors))

	# =========================================================

	groups = getFLuksoGroups()
	print("groups : ", groups)
	homes = generateHomes(session, sensors, since, start_timing, to_timing, home_ids)
	grouped_homes = generateGroupedHomes(homes, groups)

	# =========================================================

	# step 1 : save raw flukso data in cassandra
	# saveRawFluksoDataToCassandra(homes, "raw")
	updateIncompleteRows(to_timing, homes, "raw_config")

	# step 2 : save power flukso data in cassandra
	# cp.savePowerDataToCassandra(homes, "power")
	
	# step 3 : save groups of power flukso data in cassandra
	# cp.savePowerDataToCassandra(grouped_homes, "groups_power")

	# =========================================================

	# 29-10-2021 from Midnight to midnight :
	# --since s2021-10-29-00-00-00 --to s2021-10-30-00-00-00
	# 17-12-2021 from Midnight to midnight :
	# --since s2021-12-17-00-00-00 --to s2021-12-18-00-00-00


if __name__ == "__main__":
	main()