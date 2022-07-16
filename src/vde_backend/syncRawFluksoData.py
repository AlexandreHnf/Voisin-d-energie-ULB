__title__ = "syncRawFluksoData"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


"""
Script to fetch Fluksometer data using the tmpo protocol and 
- format it into tables of time series
- save the raw data in Cassandra database
"""


# standard library
from datetime import timedelta
import os
import time

from threading import Thread
from turtle import home

# 3rd party packages
import pandas as pd
import numpy as np
import tmpo

import logging

from utils import (
	getLastRegisteredConfig,
	getProgDir,
	getTimeSpent,
	isEarlier,
	read_sensor_info,
	setupLogLevel
)


# Hide warnings :
import urllib3
import warnings

# security warning & Future warning
warnings.simplefilter('ignore', urllib3.exceptions.SecurityWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)


# local sources
from home import Home
from constants import (
	CASSANDRA_KEYSPACE, 
	FROM_FIRST_TS, 
	FROM_FIRST_TS_STATUS, 
	INSERTS_PER_BATCH, 
	LIMIT_TIMING_RAW,
	LOG_FILE, 
	LOG_VERBOSE, 
	OUTPUT_FILE, 
	TBL_RAW, 
	FREQ, 
	TBL_RAW_MISSING,
	TMPO_FILE
)


import pyToCassandra as ptc
from computePower import saveHomePowerDataToCassandra
from sensor import Sensor

# Create and configure logger
logging.getLogger("tmpo").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logging_handlers = [logging.FileHandler(LOG_FILE)]
if LOG_VERBOSE:
	logging_handlers.append(logging.StreamHandler())

logging.basicConfig(
	level = setupLogLevel(),
	format = "{asctime} {levelname:<8} {message}", style='{',
	handlers=logging_handlers
)

# ====================================================================================
# Cassandra table creation
# ====================================================================================


def createRawFluksoTable(cassandra_session, table_name):
	""" 
	create a cassandra table for the raw flukso data
	"""

	columns = [
		"sensor_id TEXT", 
		"day TEXT", 
		"ts TIMESTAMP", 
		"insertion_time TIMESTAMP", 
		"config_id TIMESTAMP",
		"power FLOAT"
	]

	ptc.createTable(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		columns, 
		["sensor_id, day"], 
		["ts"], 
		{"ts":"ASC"}
	)


def createPowerTable(cassandra_session, table_name):
	""" 
	create a cassandra table for the power data
	"""

	power_cols = [
		"home_id TEXT", 
		"day TEXT", 
		"ts TIMESTAMP", 
		"P_cons FLOAT", 
		"P_prod FLOAT", 
		"P_tot FLOAT", 
		"insertion_time TIMESTAMP",
		"config_id TIMESTAMP"
	]

	ptc.createTable(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		power_cols, 
		["home_id, day"], 
		["ts"], 
		{"ts":"ASC"}
	)


def createRawMissingTable(cassandra_session, table_name):
	""" 
	Raw missing table contains timestamps range where there is missing data
	from a specific query given a specific configuration of the sensors 
	"""

	cols = [
		"sensor_id TEXT", 
		"config_id TIMESTAMP",
		"start_ts TIMESTAMP",
		"end_ts TIMESTAMP"
	]

	ptc.createTable(
		cassandra_session, 
		CASSANDRA_KEYSPACE,
		table_name, 
		cols, 
		["sensor_id, config_id"], 
		["start_ts"], 
		{"start_ts":"ASC"}
	)


# ====================================================================================


def getLastRegisteredTimestamp(cassandra_session, table_name, sensor_id):
	""" 
	get the last registered timestamp of the raw table
	- None if no timestamp in the table

	We assume that if there is data in raw table, each sensor can have different last timestmaps
	registered. 
	Assume the 'raw' table is created

	technique : first get the last registered date for the sensor, then
	query the last timestamp of this day for this sensor.

	more robust but much slower
	"""
	# get last date available for this home
	where_clause = "sensor_id = '{}'".format(sensor_id)
	cols = ["sensor_id", "day"]
	dates_df = ptc.selectQuery(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		table_name, 
		cols, 
		where_clause, 
		"ALLOW FILTERING", 
		"", 
		"DISTINCT"
	)

	ts_df = None
	if len(dates_df) > 0:
		last_date = dates_df.iloc[len(dates_df) - 1]['day']

		where_clause = "sensor_id = '{}' AND day = '{}' ORDER BY ts DESC".format(
			sensor_id, last_date)
		ts_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name,
								["ts"], where_clause, "ALLOW FILTERING", "LIMIT 1")
		if len(ts_df) > 0:
			return ts_df

	return ts_df


def getDefaultTiming(cassandra_session, sensor_id):
	"""
	We find the last registered timestamp in raw table

	if last_timestamp comes from raw table, it is a tz-naive Timestamp with CET timezone

	return None if no last registered timestamp in raw table (it 
	will be defined later by 'getTimings' function based on the configuration)
	"""
	# get last registered timestamp in raw table
	last_timestamp = getLastRegisteredTimestamp(cassandra_session, TBL_RAW, sensor_id)

	if last_timestamp is not None:  # != None
		return last_timestamp.iloc[0]['ts']
	else:  # if no registered timestamp in raw table yet
		return None


# ====================================================================================

def getInitialTimestamp(tmpo_session, sid, now):
	""" 
	get the first ever registered timestamp for a sensor using tmpo Session
	if no such timestamp (None), return an arbitrary timing (ex: since 4min)

	return a timestamp in local timezone
	"""
	initial_ts = now if FROM_FIRST_TS is None else (now - pd.Timedelta(FROM_FIRST_TS))
	if FROM_FIRST_TS_STATUS == "server":
		initial_ts_tmpo = tmpo_session.first_timestamp(sid)

		if initial_ts_tmpo is not None:
			initial_ts = initial_ts_tmpo.tz_convert("CET")

	return initial_ts


def getSensorTimings(tmpo_session, cassandra_session, missing_data, sid, now):
	"""
	For each sensor, we get start timing, forming the interval of time we have to
	query to tmpo
	- The start timing is either based on the missing data table, or the initial timestamp 
	of the sensor if no missing data registered, or simply the default timing (the last
	registered timestamp in raw data table)

	return a starting timestamp with CET timezone 
		or None if no starting timestamp
	"""
	sensor_start_ts = None
	if missing_data.index.contains(sid):  # if there is missing data for this sensor
		# CET timezone (-8sec to avoid losing first ts)
		start_ts = missing_data.loc[sid]["start_ts"] - timedelta(seconds=FREQ[0])  
		sensor_start_ts = start_ts  # sensor start timing = missing data first timestamp
	else:  # if no missing data for this sensor
		default_timing = getDefaultTiming(cassandra_session, sid)  # None or tz-naive CET
		if default_timing is None:  # if no raw data registered for this sensor yet
			# we take all data from its first timestamp
			initial_ts = getInitialTimestamp(tmpo_session, sid, now)
			sensor_start_ts = initial_ts
		else:
			sensor_start_ts = default_timing

	return sensor_start_ts


def getTimings(tmpo_session, cassandra_session, config, missing_data, now):
	"""
	For each home, get the start timing for the query based on the missing data table
	(containing for each sensor the first timestamp with missing data from the previous query)
	if no timestamp available yet for this sensor, we get the first ever timestamp available
	for the Flukso sensor with tmpo API

	default_timing = last registered timestamp for a home : CET tz
	'now' : CET by default
	"""
	timings = {}
	try: 
		ids = config.getIds()
		for home_id, sensors_ids in ids.items():
			# start_ts = earliest timestamp among all sensors of this home
			timings[home_id] = {"start_ts": now, "end_ts": now, "sensors": {}}

			for sid in sensors_ids:  # for each sensor of this home
				sensor_start_ts = getSensorTimings(tmpo_session, cassandra_session, missing_data, sid, now)

				# if 'start_ts' is older (in the past) than the current start_ts
				if sensor_start_ts is not None and isEarlier(sensor_start_ts, timings[home_id]["start_ts"]):
					timings[home_id]["start_ts"] = sensor_start_ts

				if sensor_start_ts is not None and str(sensor_start_ts.tz) == "None":
					sensor_start_ts = sensor_start_ts.tz_localize("CET") # ensures a CET timezone
				timings[home_id]["sensors"][sid] = sensor_start_ts  # CET

			if timings[home_id]["start_ts"] is now:  # no data to recover from this home 
				timings[home_id]["start_ts"] = None

	except:
		logging.critical("Exception occured in 'getTimings' : ", exc_info=True)

	return timings


# ====================================================================================

def generateHome(tmpo_session, hid, home_sensors, since_timing, to_timing):
	""" 
	Given a start timing and an end timing, generate a Home object containing the
	result of the query between these 2 timings. 
	"""
	sensors = []  # list of Sensor objects

	duration_min = round((to_timing - since_timing).total_seconds() / 60.0, 2)
	logging.info("  -> {} > {} ({} min.)".format(since_timing, to_timing, duration_min))

	for sid, row in home_sensors.iterrows():
		sensors.append(Sensor(tmpo_session, row["flukso_id"], sid, since_timing, to_timing))

	home = Home(home_sensors, since_timing, to_timing, hid, sensors)
	home.showQueryInfo()

	return home


def testSession(sensors_config):
	""" 
	test each sensor and see if tmpo accepts or refuses the sensor when
	syncing
	"""
	path = TMPO_FILE
	if not path:
		path = getProgDir()

	for sid, row in sensors_config.getSensorsConfig().iterrows():
		try:
			logging.debug("{}, {}".format(sid, row["sensor_token"]))
			tmpo_session = tmpo.Session(path)
			tmpo_session.add(sid, row["sensor_token"])
			tmpo_session.sync()
			logging.debug("=> OK")
		except:
			logging.warning("=> NOT OK")
			continue


def getTmpoSession(config):
	"""
	Get tmpo (via api) session with all the sensors in it
	"""
	path = TMPO_FILE
	if not path:
		path = getProgDir()
	logging.info("tmpo path : " + path)

	tmpo_session = tmpo.Session(path)
	for sid, row in config.getSensorsConfig().iterrows():
		tmpo_session.add(sid, row["sensor_token"])

	logging.info("> tmpo synchronization...")
	try: 
		tmpo_session.sync()
	except Exception as e:
		logging.warning("Exception occured in tmpo sync: ", exc_info=True)
		logging.warning("> tmpo sql file needs to be reset, or some sensors are invalid.")
	logging.info("> tmpo synchronization : OK")

	return tmpo_session


def getFluksoData(sensor_file, path=""):
	"""
	get Flukso tmpo session (via API) + sensors info (IDs, ...)
	from a csv file containing the sensors configurations
	"""
	if not path:
		path = getProgDir()

	sensors = read_sensor_info(path, sensor_file)
	tmpo_session = tmpo.Session(path)
	for hid, hn, sid, tk, n, c, p in sensors.values:
		tmpo_session.add(sid, tk)

	tmpo_session.sync()

	return sensors, tmpo_session


# ====================================================================================


def saveHomeMissingData(cassandra_session, config, to_timing, home, saved_sensors):
	"""
	Save the first timestamp with no data (nan values) for each sensors of the home
	"""
	hid = home.getHomeID()
	logging.debug("- saving in Cassandra: {} ... ".format(TBL_RAW_MISSING))

	try:
		config_id = config.getConfigID().isoformat()

		col_names = ["sensor_id", "config_id", "start_ts", "end_ts"]
		
		inc_power_df = home.getIncompletePowerDF()
		if len(inc_power_df) > 0:
			sensors_ids = inc_power_df.columns
			for sid in sensors_ids:
				if saved_sensors.get(sid, None) is None:  # if no missing data saved for this sensor yet
					end_ts = to_timing.isoformat()
					if inc_power_df[sid].isnull().values.any():  # if the column contains null
						for i, timestamp in enumerate(inc_power_df.index):
							# if valid timestamp
							if (to_timing - timestamp).days < LIMIT_TIMING_RAW:  # X days from now max
								# save timestamp with CET local timezone
								start_ts = timestamp.isoformat()
								if np.isnan(inc_power_df[sid][i]):
									values = [sid, config_id, start_ts, end_ts]
									# logging.info("{} | start : {}, end = {}", config_id, start_ts, end_ts)
									ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, TBL_RAW_MISSING, col_names, values)
									saved_sensors[sid] = True  # mark that this sensor has missing data
									# as soon as we find the first ts with null value, we go to next sensor
									break

		logging.debug("   OK : missing raw data saved.")
	
	except:
		logging.critical("Exception occured in 'saveHomeMissingData' : {} ".format(hid), exc_info=True)


def saveHomeRawToCassandra(cassandra_session, home, config, timings):
	"""
	Save raw flukso flukso data to Cassandra table
	Save per sensor : 1 row = 1 sensor + 1 timestamp + 1 power value
		home_df : timestamp, sensor_id1, sensor_id2, sensor_id3 ... sensor_idN
	"""
	hid = home.getHomeID()
	logging.debug("- saving in Cassandra: {} ...".format(TBL_RAW))

	try: 
		insertion_time = pd.Timestamp.now(tz="CET").isoformat()
		config_id = config.getConfigID().isoformat()

		power_df = home.getRawDF()
		power_df['date'] = power_df.apply(lambda row: str(row.name.date()), axis=1)  # add date column
		by_day_df = power_df.groupby("date")  # group by date

		col_names = ["sensor_id", "day", "ts", "insertion_time", "config_id", "power"]
		for date, date_rows in by_day_df:  # loop through each group (each date group)

			for sid in date_rows:  # loop through each column, 1 column = 1 sensor
				if sid == "date" or timings[hid]["sensors"][sid] is None: continue
				insert_queries = ""
				for i, timestamp in enumerate(date_rows[sid].index):
					# if the timestamp > the sensor's defined start timing
					if isEarlier(timings[hid]["sensors"][sid], timestamp):
						ts = timestamp.isoformat()
						power = date_rows[sid][i]
						values = [sid, date, ts, insertion_time, config_id, power]
						insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, TBL_RAW, col_names, values)

						if (i + 1) % INSERTS_PER_BATCH == 0:
							ptc.batch_insert(cassandra_session, insert_queries)
							insert_queries = ""

				ptc.batch_insert(cassandra_session, insert_queries)

		logging.debug("   OK : raw data saved.")

	except:
		logging.critical("Exception occured in 'saveHomeRawToCassandra' : ", exc_info=True)


# ====================================================================================

def displayHomeInfo(home_id, start_ts, end_ts):
	""" 
	Display some info during the execution of a query for logging. Can be activated by
	turning the logging mode to 'INFO' 
	> home id | start timestamp > end timestamp (nb days > nb minutes)
		-> date 1 start timestamp > date 1 end timestamp (nb minutes)
			- nb raw data, nb NaN data, total nb NaN data
		-> date 2 start timestamp > date 2 end timestamp (nb minutes)
			- ...
		-> ...
	"""

	nb_days = (end_ts - start_ts).days
	duration_min = 0
	if start_ts is not None and end_ts is not None:
		duration_min = round((end_ts - start_ts).total_seconds() / 60.0, 2)
	if duration_min > 0:
		logging.info("> {} | {} > {} ({} days > {} min.)".format(home_id, 
					start_ts, end_ts, nb_days, duration_min))
	else:
		logging.info("> {} | no data to recover".format(home_id))


def getIntermediateTimings(start_ts, end_ts):
	""" 
	Given 2 timestamps, generate the intermediate timings
	- interval duration = 1 day
	"""
	intermediate_timings = list(pd.date_range(
		start_ts,
		end_ts,
		freq="1D"
	))

	if len(intermediate_timings) == 1 and end_ts != start_ts:
		intermediate_timings.append(end_ts)

	return intermediate_timings


def processHomes(cassandra_session, tmpo_session, config, timings, now):
	# for each home
	for hid, home_sensors in config.getSensorsConfig().groupby("home_id"):
		if hid in ['CDB001', 'CDB043', 'CDBA01', 'ECHASC', 'ECHBUA', 'ECHL01']:
			saved_sensors = {}  # for missing data, to check if sensors missing data already saved
			# if home has a start timestamp and a end timestamp
			if timings[hid]["start_ts"] is not None and timings[hid]["end_ts"] is not None:
				intermediate_timings = getIntermediateTimings(timings[hid]["start_ts"], timings[hid]["end_ts"])
				displayHomeInfo(hid, timings[hid]["start_ts"], timings[hid]["end_ts"])

				for i in range(len(intermediate_timings)-1):  # query day by day
					start_timing = intermediate_timings[i]
					to_timing = intermediate_timings[i+1]

					# generate home
					home = generateHome(tmpo_session, hid, home_sensors, start_timing, to_timing)

					threads = []
					# save raw flukso data in cassandra
					t1 = Thread(target = saveHomeRawToCassandra, args=(cassandra_session, home, config, timings))
					threads.append(t1)
					t1.start()

					# save missing raw data in cassandra
					t2 = Thread(target = saveHomeMissingData, args = (cassandra_session, config, now, home, saved_sensors))
					threads.append(t2)
					t2.start()

					# save power flukso data in cassandra
					t3 = Thread(target = saveHomePowerDataToCassandra, args = (cassandra_session, home, config))
					threads.append(t3)
					t3.start()

					# wait for the threads to complete
					for t in threads:
						t.join()

			else:
				logging.info("{} : No data to save".format(hid))
		
			logging.info("---------------------------------------------------------------")

# ====================================================================================


def saveFluksoDataToCsv(homes):
	""" 
	may be obsolete/broken 
	"""
	logging.info("saving flukso data in csv...")
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

	logging.info("Successfully Saved flukso data in csv")


# ====================================================================================


def showProcessingTimes(configs, begin, setup_time, config_timers):
	"""
	Display processing time for each step of 1 query
	"""
	logging.info("--------------------- Timings --------------------")
	logging.info("> Setup time : {}.".format(getTimeSpent(begin, setup_time)))

	for i, config in enumerate(configs):
		config_id = config.getConfigID()
		logging.info("> Config : {}".format(str(config_id)))
		t = config_timers[config_id]
		logging.info("  > Timings : {}.".format(getTimeSpent(t["start"], t["timing"])))
		logging.info("  > Generate homes + saving in db : {}.".format(getTimeSpent(t["timing"], t["homes"])))

	logging.info("> Total Processing time : {}.".format(getTimeSpent(begin, time.time())))


def createTables(cassandra_session):
	"""
	create the necessary tables for the flukso data synchronization
	""" 
	createRawFluksoTable(cassandra_session, "raw")
	createRawMissingTable(cassandra_session, "raw_missing")
	createPowerTable(cassandra_session, "power")


def sync(cassandra_session):
	logging.info("====================== Sync ======================")
	begin = time.time()
	
	# > timings
	now = pd.Timestamp.now(tz="CET").replace(microsecond=0)  # remove microseconds for simplicity

	# =============================================================

	# > Configuration
	config = getLastRegisteredConfig(cassandra_session)
	missing_data = ptc.selectQuery(
		cassandra_session,
		CASSANDRA_KEYSPACE,
		TBL_RAW_MISSING,
		"*",
		"",
		"",
		""
	)
	logging.info("- Running time (Now - CET) :  " + str(now))
	setup_time = time.time()

	# =========================================================

	ptc.deleteRows(cassandra_session, CASSANDRA_KEYSPACE, TBL_RAW_MISSING)  # truncate existing rows

	# Timer
	config_timers = {}
	config_id = config.getConfigID()
	logging.info("- Config :                    " + str(config_id))
	config_timers[config_id] = {"start": time.time()}

	# Config information
	logging.info("- Number of Homes :           " + str(config.getNbHomes()))
	logging.info("- Number of Fluksos :         " + str(len(set(config.getSensorsConfig().flukso_id))))
	logging.info("- Number of Fluksos sensors : " + str(len(config.getSensorsConfig())))
	logging.info("---------------------- Tmpo -----------------------")

	# TMPO synchronization
	tmpo_session = getTmpoSession(config)

	# STEP 1 : get start and end timings for all homes for the query
	timings = getTimings(
		tmpo_session,
		cassandra_session,
		config,
		missing_data,
		now
	)
	config_timers[config_id]["timing"] = time.time()

	# =========================================================

	logging.info("---------------------- Homes ---------------------")
	logging.info("Generating homes data, getting Flukso data and save in Cassandra...")

	# STEP 2 : process all homes data, and save in database
	processHomes(cassandra_session, tmpo_session, config, timings, now)

	config_timers[config_id]["homes"] = time.time()

	# =========================================================

	showProcessingTimes([config], begin, setup_time, config_timers) 


def main():

	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	# first, create tables if needed : 
	createTables(cassandra_session)

	# then, sync new data in Cassandra
	sync(cassandra_session)


if __name__ == "__main__":
	main()
