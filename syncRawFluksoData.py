""" 
Author : Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and 
- format it into tables of time series
- save the raw data in Cassandra database
"""

from turtle import home
from home import *
from gui import *
from constants import *
import pyToCassandra as ptc
from sensorConfig import Configuration
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
import time
from datetime import timedelta
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


def getLastRegisteredTimestamp(cassandra_session, table_name, config):
	""" 
	get the last registered timestamp of the raw table
	- None if no timestamp in the table

	We assume that if there is data in raw table, each sensor has the same last timestmap
	registered. 
	"""
	print("Getting last timestamp...")

	# we assume the sensor_id is present in the table
	sid = config.getFirstSensorId()  # we take the first sensor available
	# print("sid for the last ts : ", sid)

	dates = ["'" + d + "'" for d in getLastXDates()]
	ts_df = None
	for date in dates:
		where_clause = "sensor_id = '{}' AND day = {} AND config_id = '{}.000000+0000' ORDER BY ts DESC".format(
						sid, date, str(config.getConfigID()))
		ts_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, 
								["ts"], where_clause, "ALLOW FILTERING", "LIMIT 1")
		if len(ts_df) > 0:
			return ts_df

	return ts_df


def getDefaultTiming(mode, start_timing, cassandra_session, config):
	""" 
	If automatic mode : we find the last registered timestamp in raw table
	If manual mode : the default timing is the specified start_timing

	if last_timestamp comes from raw table, it is a tz-naive Timestamp with CET timezone
	=> convert it to an tz-aware Timestamp with UTC timezone
	"""
	if mode == "automatic":
		# get last registered timestamp in raw table
		last_timestamp = getLastRegisteredTimestamp(cassandra_session, TBL_RAW, config)
		if not last_timestamp.empty:  # != None
			return last_timestamp.iloc[0]['ts'].tz_localize("CET").tz_convert("UTC")
		else:  # if no registered timestamp in raw table yet
			return start_timing
	
	elif mode == "manual":
		return start_timing


# ====================================================================================


def getMissingRaw(cassandra_session, table_name):
	""" 
	get the missing raw data timings
	- for each sensor config, we get for each sensor the first timestamp with missing data (null)
	and the last timestamp with missing data based on the previous performed query
	"""

	all_missing_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, "*", "", "", "")
	# group by config id (id = insertion time)
	by_config_df = all_missing_df.groupby("config_id")  # keys sorted by default
	
	return by_config_df


def getNeededConfigs(all_sensors_configs, all_groups_configs, missing_data, cur_sconfig):
	""" 
	From the missing data table, deduce the configurations to use
	Return a list of Configuration objects
	all_sensors_configs and all_groups_configs : a groupby object : 
		keys = config ids (insertion dates), values = dataframe

	return : list of Configuration objects, each object = 1 sensors config + 1 groups config
	"""
	configs = []
	needed_configs = list(missing_data.groups.keys())
	if cur_sconfig not in needed_configs:
		needed_configs.append(cur_sconfig)

	for config_id in needed_configs:
		config = Configuration(config_id, all_sensors_configs.get_group(config_id).set_index("sensor_id"), 
							   all_groups_configs.get_group(config_id))
		configs.append(config)

	return configs
 

def getSensorTimings(sensor_missing_df, timings, home_id, sid, config, current_config_id, now, default_timing):
	if len(sensor_missing_df) > 0:
		# we get a local timestamp (CET)
		start_ts = sensor_missing_df.iloc[0]["start_ts"]
		end_ts = sensor_missing_df.iloc[0]["end_ts"].tz_localize("CET").tz_convert("UTC")
	
		# if 'start_ts' is older (in the past) than the current start_ts
		if isEarlier(start_ts, timings[home_id]["start_ts"]):
			timings[home_id]["start_ts"] = start_ts

		if timings[home_id]["end_ts"] is None:
			timings[home_id]["end_ts"] = end_ts

		timings[home_id]["sensors"][sid] = start_ts.tz_localize("CET").tz_convert("UTC")
	
	else:  # if no missing data for this sensor, we take all data from its first timestamp
		if config.getConfigID() == current_config_id:  # only if current config
			# TODO : change to tmpo first_timestamp()
			initial_ts = setInitSeconds(getTiming(FROM_FIRST_TS, now))  # TEMPORARY
			timings[home_id]["start_ts"] = initial_ts
			timings[home_id]["sensors"][sid] = initial_ts
		else:
			timings[home_id]["sensors"][sid] = default_timing
		

def getTimings(cassandra_session, tmpo_session, config, current_config_id, table_name, default_timing, now):
	""" 
	For each home, get the start timing for the query based on the missing data table
	(containing for each sensor the first timestamp with missing data from the previous query)
	if no timestamp available yet for this sensor, we get the first ever timestamp available
	for the Flukso sensor with tmpo API

	default_timing = last registered timestamp for a home : UTC tz
	'now' : no tz, but CET by default
	"""
	timings = {}
	ids = config.getIds()
	for home_id, sensors_ids in ids.items():
		# start_ts = earliest timestamp among all sensors of this home
		timings[home_id] = {"start_ts": now, "end_ts": None, "sensors": {}}
		
		for sid in sensors_ids:
			# TODO : use missing_data containing all rows of the table = only 1 select query
			where_clause = "sensor_id = '{}' and config_id = '{}.000000+0000'".format(sid, str(config.getConfigID()))
			sensor_missing_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, "*", where_clause, "ALLOW FILTERING", "")
			
			getSensorTimings(sensor_missing_df, timings, home_id, sid, config, current_config_id, now, default_timing)
			
		if timings[home_id]["start_ts"] == now:  # if no missing data for this home
			timings[home_id]["start_ts"] = default_timing  # UTC tz
		timings[home_id]["start_ts"] = timings[home_id]["start_ts"].tz_localize("CET").tz_convert("UTC")
		if timings[home_id]["end_ts"] is None or config.getConfigID() == current_config_id:
			timings[home_id]["end_ts"] = setInitSeconds(now).tz_localize("CET").tz_convert("UTC")

	return timings

# ====================================================================================


def generateHomes(tmpo_session, config, since, since_timing, to_timing, homes_missing, mode, now):
	""" 
	Given a configuration (sensors information, homes..), generate home objects
	1 home = combination of sensors (Sensor objects).
	For each home, query tmpo to get data given a predefined time interval, then
	compute power raw data, production, consumption, total  
	"""
	print("======================= HOMES =======================")
	homes = {}

	for hid, home_sensors in config.getSensorsConfig().groupby("home_id"):
		print("> {} | ".format(hid), end="")
		sensors = [] # list of Sensor objects
		if mode == "automatic":
			since_timing = homes_missing[hid]["start_ts"]
			# to_timing = now if homes_missing[hid]["end_ts"] is None else homes_missing[hid]["end_ts"]
			to_timing = homes_missing[hid]["end_ts"]
			print("{} > {} ({})".format(since_timing, to_timing, round(pd.Timedelta(to_timing - since_timing).seconds / 60.0, 2)), end=" | ")

		for sid, row in home_sensors.iterrows():
			sensors.append(Sensor(tmpo_session, row["flukso_id"], sid, since_timing, to_timing))
		home = Home(home_sensors, since, since_timing, to_timing, hid, sensors)
		homes[hid] = home

	return homes


def generateGroupedHomes(homes, config):
	""" 
	We receive a configuration object and create a set of grouped Home objects
	Each grouped home = sum of different homes  
	"""
	print("======================= GROUPS =======================")
	grouped_homes = {}

	for gid, group in config.getGroupsConfig().groupby("group_id"):
		print("> {} : ".format(gid))
		home_ids = list(group["homes"][group.index[0]])
		print("Home ids : ", home_ids)
		home = copy.copy(homes[home_ids[0]])
		for j in range(1, len(home_ids)):
			print(homes[home_ids[j]].getHomeID(), end=" ")
			home.appendFluksoData(homes[home_ids[j]].getRawDF(), homes[home_ids[j]].getHomeID())
			home.addConsProd(homes[home_ids[j]].getConsProdDF())
		home.setHomeID("group_" + gid)
		print()

		grouped_homes["group" + gid] = home

	return grouped_homes


def testSession(sensors_config, path=""):
	if not path:
		path = getProgDir()

	for sid, row in sensors_config.iterrows():
		try:
			print("{}, {}".format(sid, row["sensor_token"]))
			tmpo_session = tmpo.Session(path)
			tmpo_session.add(sid, row["sensor_token"])
			tmpo_session.sync()
			print("=> OK")
		except:
			print("=> NOT OK")
			continue


def getTmpoSession(config, path=""):
	""" 
	Get tmpo (via api) session with all the sensors in it
	"""
	if not path:
		path = getProgDir()
		# print(path)

	tmpo_session = tmpo.Session(path)
	for sid, row in config.getSensorsConfig().iterrows():
		# print(sid, row["sensor_token"])
		tmpo_session.add(sid, row["sensor_token"])

	tmpo_session.sync()
	print("> tmpo synchronization OK")

	return tmpo_session


def getFluksoData(sensor_file, path=""):
	"""
	get Flukso tmpo session (via API) + sensors info (IDs, ...)
	from a csv file containing the sensors configurations
	"""
	if not path:
		path = getProgDir()

	sensors = read_sensor_info(path, sensor_file)
	# print(sensors.head(5))
	tmpo_session = tmpo.Session(path)
	for hid, hn, sid, tk, n, c, p in sensors.values:
		tmpo_session.add(sid, tk)

	tmpo_session.sync()

	return sensors, tmpo_session


# ====================================================================================


def saveMissingData(cassandra_session, config, to_timing, homes, table_name):
	"""
	For each home, save the first timestamp with no data (nan values) for each sensors
	"""
	print("saving in Cassandra...   => table : {}".format(table_name))

	ptc.deleteRows(cassandra_session, CASSANDRA_KEYSPACE, table_name)  # truncate existing rows

	to_timing = convertTimezone(to_timing, "CET")
	config_id = str(config.getConfigID())[:19] + "Z"

	col_names = ["sensor_id", "config_id", "start_ts", "end_ts"]
	for hid, home in homes.items():
		print(hid, end=" ")
		inc_power_df = home.getIncompletePowerDF()
		if len(inc_power_df) > 0:
			sensors_ids = inc_power_df.columns
			for sid in sensors_ids:
				end_ts = str(inc_power_df.index[-1])[:19] + "Z"
				if inc_power_df[sid].isnull().values.any():  # if the column contains null
					for i, timestamp in enumerate(inc_power_df.index):
						# if valid timestamp
						if (to_timing - timestamp).days < LIMIT_TIMING_RAW: # X days from now max
							# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
							start_ts = str(timestamp)[:19] + "Z"
							if np.isnan(inc_power_df[sid][i]):
								
								values = [sid, config_id, start_ts, end_ts]
								# print("{} | start : {}, end = {}", config_id, start_ts, end_ts)
								ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, table_name, col_names, values)
								# as soon as we find the first ts with null value, we go to next sensor
								break

	print("Successfully Saved raw missing data in Cassandra : table {}".format(table_name))


def saveRawToCassandra(cassandra_session, homes, config, table_name):
	"""
	Save raw flukso flukso data to Cassandra table
	Save per sensor : 1 row = 1 sensor + 1 timestamp + 1 power value
		home_df : timestamp, sensor_id1, sensor_id2, sensor_id3 ... sensor_idN
	"""
	print("saving in Cassandra...   => table : {}".format(table_name))
	# TODO : insert a row only if the timestamp is after the start timing for each sensor

	insertion_time = str(pd.Timestamp.now())[:19] + "Z"
	config_id = str(config.getConfigID())[:19] + "Z"
	for hid, home in homes.items():
		print(hid)
		power_df = home.getRawDF()
		power_df['date'] = power_df.apply(lambda row: str(row.name.date()), axis=1) # add date column
		by_day_df = power_df.groupby("date")  # group by date

		col_names = ["sensor_id", "day", "ts", "insertion_time", "config_id", "power"]
		for date, date_rows in by_day_df:  # loop through each group (each date group)

			for sid in date_rows:  # loop through each column, 1 column = 1 sensor
				if sid == "date": continue
				insert_queries = ""
				for i, timestamp in enumerate(date_rows[sid].index):
					ts = str(timestamp)[:19] + "Z"
					power = date_rows[sid][i]
					values = [sid, date, ts, insertion_time, config_id, power]
					insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, table_name, col_names, values)

					if (i+1) % INSERTS_PER_BATCH == 0:
						ptc.batch_insert(cassandra_session, insert_queries)
						insert_queries = ""
				
				ptc.batch_insert(cassandra_session, insert_queries) 

	print("> Successfully Saved raw data in Cassandra : table {}".format(table_name))


# ====================================================================================


def saveFluksoDataToCsv(homes):
    print("saving flukso data in csv...")
    for home in homes:
        # filepath = "output/fluksoData/{}.csv".format(home.getHomeID())
        power_df = home.getRawDF()
        cons_prod_df = home.getConsProdDF()
        combined_df = power_df.join(cons_prod_df)

        outname = home.getHomeID() + '.csv'
        outdir = OUTPUT_FILE
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        filepath = os.path.join(outdir, outname)

        combined_df.to_csv(filepath)

    print("Successfully Saved flukso data in csv")


# ====================================================================================


def showProcessingTimes(configs, begin, setup_time, last_ts_time, config_timers):
	""" 
	Display processing time for each step of 1 query
	- TODO : replace all this by a Timer class
	"""
	print("================= Timings ===================")
	print("> Setup time : {}.".format(getTimeSpent(begin, setup_time)))
	print("> Last ts : {}.".format(getTimeSpent(setup_time, last_ts_time)))

	for i, config in enumerate(configs):
		config_id = config.getConfigID()
		print("> Config {} : {}".format(i+1, str(config_id)))
		t = config_timers[config_id]
		print("  > Timings : {}.".format(getTimeSpent(t[0], t[1])))
		print("  > Generate homes : {}.".format(getTimeSpent(t[1], t[2])))
		print("  > save raw : {}.".format(getTimeSpent(t[2], t[3])))
		print("  > Save missing : {}.".format(getTimeSpent(t[3], t[4])))
		print("  > Save power : {}.".format(getTimeSpent(t[4], t[5])))
		print("  > Save groups power : {}.".format(getTimeSpent(t[5], t[6])))
	
	print("> Total Processing time : {}.".format(getTimeSpent(begin, time.time())))


def processArguments():
	""" 
	# 29-10-2021 from Midnight to midnight :
	# --since s2021-10-29-00-00-00 --to s2021-10-30-00-00-00
	# 17-12-2021 from Midnight to midnight :
	# --since s2021-12-17-00-00-00 --to s2021-12-18-00-00-00
	"""
	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
	)
	argparser.add_argument("--mode", type=str, default="manual", 
							help="Manual : set --since --to parameters"
								"Automatic : no parameters to provide")

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

	return argparser


def main():
	begin = time.time()

	# > arguments
	argparser = processArguments()
	args = argparser.parse_args()
	mode = args.mode
	since = args.since
	to = args.to
	
	# > timings
	now = pd.Timestamp.now(tz="UTC").replace(microsecond=0) # remove microseconds for simplicity
	now_local = pd.Timestamp.now().replace(microsecond=0)   # default tz = CET, unaware timestamp
	start_timing = setInitSeconds(getTiming(since, now))  	# UTC
	to_timing = setInitSeconds(getTiming(to, now)) 			# UTC

	# =============================================================

	# > Configurations
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	current_config_id, all_sensors_configs = getCurrentSensorsConfigCassandra(cassandra_session, TBL_SENSORS_CONFIG)
	current_gconfig_id, all_groups_configs = getGroupsConfigsCassandra(cassandra_session, TBL_GROUPS_CONFIG)

	missing_data = getMissingRaw(cassandra_session, TBL_RAW_MISSING)
	configs = getNeededConfigs(all_sensors_configs, all_groups_configs, missing_data, current_config_id)
	
	print("Mode : {}".format(mode))
	print("start timing : {} (CET) => {} (UTC)".format(since, start_timing))
	print("to timing    : {} (CET) => {} (UTC)".format(to, to_timing))
	print("now (CET) : ", now_local)
	print("Number of configs : ", len(configs))

	setup_time = time.time()
	
	# =========================================================

	# STEP 1 : get last registered timestamp in raw table (using current config)
	default_timing = getDefaultTiming(mode, start_timing, cassandra_session, configs[-1])
	print("default timing : ", default_timing)
	last_ts_time = time.time()

	config_timers = {}

	for config in configs:
		config_id = config.getConfigID()
		print("           CONFIG {} : ".format(str(config_id)))
		config_timers[config_id] = [time.time()]

		print("Number of Homes : ", config.getNbHomes())
		print("Number of Fluksos : ", len(set(config.getSensorsConfig().flukso_id)))
		print("Number of Fluksos sensors : ", len(config.getSensorsConfig()))
		print("Number of groups : ", len(config.getGroupsConfig()))

		tmpo_session = getTmpoSession(config) 
		# testSession(sensors_config)

		# STEP 2 : get start and end timings for all homes for the query
		timings = getTimings(cassandra_session, tmpo_session, config, current_config_id, TBL_RAW_MISSING, default_timing, now_local)
		config_timers[config_id].append(time.time())

		# =========================================================
		
		# STEP 3 : generate homes and grouped homes
		print("==================================================")
		print("Generating homes data and getting Flukso data...")
		homes = generateHomes(tmpo_session, config, since, start_timing, to_timing, timings, mode, now)
		grouped_homes = generateGroupedHomes(homes, config)
		config_timers[config_id].append(time.time())
	
		# print("==================================================")
		# saveFluksoDataToCsv(homes.values())
		# saveFluksoDataToCsv(grouped_homes.values())

		# =========================================================
		
		# STEP 4 : save raw flukso data in cassandra
		print("==================================================")
		saveRawToCassandra(cassandra_session, homes, config, TBL_RAW)
		config_timers[config_id].append(time.time())

		# STEP 5 : save missing raw data in cassandra
		print("==================================================")
		saveMissingData(cassandra_session, config, now, homes, TBL_RAW_MISSING)
		config_timers[config_id].append(time.time())

		# STEP 6 : save power flukso data in cassandra
		print("==================================================")
		cp.savePowerDataToCassandra(cassandra_session, homes, config, TBL_POWER)
		config_timers[config_id].append(time.time())
		
		# STEP 7 : save groups of power flukso data in cassandra
		print("==================================================")
		cp.savePowerDataToCassandra(cassandra_session, grouped_homes, config, TBL_GROUPS_POWER)
		config_timers[config_id].append(time.time())

		print("---------------------------------------------------------------")
	
	# =========================================================
	
	showProcessingTimes(configs, begin, setup_time, last_ts_time, config_timers)
	


if __name__ == "__main__":
	main()
