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
from sensor import *

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

def getMissingRaw(session, ids, table_name, default_timing, now):
	""" 
	get the rows of raw data with missing values
	(cassandra table 'raw_missing')
	"""
	homes_missing_rows = {}
	for home_id, sensors_ids in ids.items():
		homes_missing_rows[home_id] = {"first_ts": now}

		for sid in sensors_ids:
			where_clause = "sensor_id = {}".format("'"+sid+"'")
			sensor_df = ptc.selectQuery(session, CASSANDRA_KEYSPACE, table_name, "*", where_clause, "")
		
			homes_missing_rows[home_id][sid] = sensor_df
			# print(sensor_df.head(3), len(sensor_df))

			if len(sensor_df) > 0:
				first_ts = sensor_df.iloc[0]["ts"]
				# print(first_ts, first_ts.tz)
				# if 'first_ts' is older (in the past) than the current first_ts
				if (first_ts-homes_missing_rows[home_id]["first_ts"])/np.timedelta64(1,'s') < 0:
					homes_missing_rows[home_id]["first_ts"] = first_ts

		if homes_missing_rows[home_id]["first_ts"] == now:
			homes_missing_rows[home_id]["first_ts"] = default_timing
		print("{} : first ts : {}".format(home_id, homes_missing_rows[home_id]["first_ts"]))
	
	return homes_missing_rows


# ====================================================================================


def generateHomes(session, sensors_info, since, since_timing, to_timing, home_ids):
	homes = {}

	for hid in home_ids:
		print("========================= HOME {} =====================".format(hid))
		home_sensors = sensors_info.loc[sensors_info["home_ID"] == hid]
		sensors = [] # list of Sensor objects
		for i in range(len(home_sensors)):
			sensors.append(Sensor(session, home_sensors.flukso_id[i], home_sensors.sensor_id[i], 
							since_timing, to_timing))
		home = Home(sensors_info, since, since_timing, to_timing, hid, sensors)
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
	Save raw missing data to Cassandra cluster
	-> incomplete rows (with null values)
	"""
	print("saving in Cassandra...")
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	ptc.deleteRows(session, CASSANDRA_KEYSPACE, table_name)  # truncate existing rows

	col_names = ["sensor_id", "day", "ts"]
	for hid, home in homes.items():
		print(hid)
		inc_power_df = home.getIncompletePowerDF()
		sensors_ids = inc_power_df.columns

		for timestamp, row in inc_power_df.iterrows():
			# if valid timestamp
			if (to_timing - timestamp).days < 2: # 2 days max
				day = str(timestamp.date())
				# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
				ts = str(timestamp)[:19] + "Z"
				for i, sensor_id in enumerate(sensors_ids):
					if np.isnan(row[i]):
						values = [sensor_id, day, ts]
						ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	print("Successfully Saved raw missing data in Cassandra : table {}".format(table_name))

def getColumnsNames(columns):
	res = []
	for i in range(len(columns)):
		index = str(i+1)
		# if len(index) == 1:
		#     index = "0" + index
		res.append("phase" + index)

	return res


def saveRawDataToCassandraPerHome(homes, table_name):
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


def saveRawDataToCassandraPerSensor(homes, table_name):
	"""
	Save raw flukso data to Cassandra cluster
	"""
	print("saving in Cassandra...")
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	for hid, home in homes.items():
		print(hid)
		power_df = home.getPowerDF()

		col_names = ["sensor_id", "day", "ts", "power"]
		sensors_ids = power_df.columns
		print(sensors_ids)
		for timestamp, row in power_df.iterrows():
			day = str(timestamp.date())
			# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
			ts = str(timestamp)[:19] + "Z"
			for i, sensor_id in enumerate(sensors_ids):
				values = [sensor_id, day, ts, row[i]]
				ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)


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
	now_local = pd.Timestamp.now()

	start_timing = setInitSeconds(getTiming(since, now))
	to_timing = setInitSeconds(getTiming(to, now))
	default_timing = setInitSeconds(getTiming("5min", now))

	# =============================================================

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	sensors, session = getFluksoData(UPDATED_SENSORS_FILE)
	ids = getSensorsIds(sensors)

	getMissingRaw(cassandra_session, ids, "raw_missing", default_timing, now_local)

	home_ids = set(sensors["home_ID"])
	nb_homes = len(home_ids)

	print("since : {} => {}".format(since, start_timing))
	print("to    : {} => {}".format(to, to_timing))
	print("Number of Homes : ", nb_homes)
	print("Number of Fluksos : ", len(sensors))

	# =========================================================

	# groups = getFLuksoGroups()
	# print("groups : ", groups)
	# homes = generateHomes(session, sensors, since, start_timing, to_timing, home_ids)
	# grouped_homes = generateGroupedHomes(homes, groups)

	# =========================================================

	# step 1 : save raw flukso data in cassandra
	# saveRawDataToCassandraPerSensor(homes, "raw")
	# updateIncompleteRows(to_timing, homes, "raw_missing")

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
