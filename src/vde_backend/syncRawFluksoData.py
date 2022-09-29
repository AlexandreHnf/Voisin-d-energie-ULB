__title__ = "syncRawFluksoData"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, and Guillaume Levasseur"
__license__ = "MIT"


"""
Script to fetch Fluksometer data using the tmpo protocol and 
- format it into tables of time series
- save the raw data in Cassandra database
"""


# standard library
from datetime import timedelta
import os
import time
import argparse
import sys

from threading import Thread

# 3rd party packages
import pandas as pd
import numpy as np
import tmpo

from utils import (
	logging,
	getLastRegisteredConfig,
	getProgDir,
	getTimeSpent,
	isEarlier,
	setInitSeconds,
	read_sensor_info,
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
	PROD,
	CASSANDRA_KEYSPACE, 
	FROM_FIRST_TS, 
	INSERTS_PER_BATCH, 
	LIMIT_TIMING_RAW,
	TBL_RAW, 
	FREQ, 
	TBL_RAW_MISSING,
	TMPO_FILE,
	TBL_POWER
)


import pyToCassandra as ptc
from computePower import saveHomePowerDataToCassandra
from sensor import Sensor

# ====================================================================================
# Cassandra table creation
# ====================================================================================


def createRawFluksoTable(table_name):
	""" 
	create a cassandra table for the raw flukso data
	"""

	columns = [
		"sensor_id TEXT", 
		"day TEXT", 				# CET timezone
		"ts TIMESTAMP", 			# UTC timezone (automatically converted)
		"insertion_time TIMESTAMP", 
		"config_id TIMESTAMP",
		"power FLOAT"
	]

	ptc.createTable(
		CASSANDRA_KEYSPACE, 
		table_name, 
		columns, 
		["sensor_id"],
		["day", "ts"],
		{"day": "ASC", "ts":"ASC"}
	)


def createPowerTable(table_name):
	""" 
	create a cassandra table for the power data
	"""

	power_cols = [
		"home_id TEXT", 
		"day TEXT", 			# CET timezone
		"ts TIMESTAMP", 		# UTC timezone (automatically converted)
		"P_cons FLOAT", 
		"P_prod FLOAT", 
		"P_tot FLOAT", 
		"insertion_time TIMESTAMP",
		"config_id TIMESTAMP"
	]

	ptc.createTable(
		CASSANDRA_KEYSPACE, 
		table_name, 
		power_cols, 
		["home_id"],
		["day", "ts"],
		{"day": "ASC", "ts":"ASC"}
	)


def createRawMissingTable(table_name):
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
		CASSANDRA_KEYSPACE,
		table_name, 
		cols, 
		["sensor_id", "config_id"],
		["start_ts"], 
		{"start_ts":"ASC"}
	)


# ====================================================================================


def getLastRegisteredTimestamp(table_name, sensor_id):
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
	where_clause = "sensor_id = '{}' ORDER BY day DESC".format(sensor_id)
	dates_df = ptc.selectQuery(
		CASSANDRA_KEYSPACE, 
		table_name, 
		["day"],
		where_clause, 
		limit=1,
	)

	if len(dates_df) > 0:
		last_date = dates_df.iat[0,0]
		ts_df = ptc.selectQuery(
			CASSANDRA_KEYSPACE,
			table_name,
			["ts"],
			"sensor_id = '{}' AND day = '{}'".format(sensor_id, last_date),
		)
		if len(ts_df) > 0:
			return ts_df.max().max()

	return None


def getInitialTimestamp(tmpo_session, sid, now):
	""" 
	get the first ever registered timestamp for a sensor using tmpo Session
	if no such timestamp (None), return an arbitrary timing (ex: since 4min)

	return a timestamp in local timezone (CET)
	"""
	initial_ts = now if FROM_FIRST_TS is None else (now - pd.Timedelta(FROM_FIRST_TS))
	if PROD:
		initial_ts_tmpo = tmpo_session.first_timestamp(sid)

		if initial_ts_tmpo is not None:
			initial_ts = initial_ts_tmpo.tz_convert("CET")

	return initial_ts


def getSensorTimings(tmpo_session, missing_data, sid, now):
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
	if sid in missing_data['sensor_id']:  # if there is missing data for this sensor
		# CET timezone (minus a certain offset to avoid losing first ts)
		sensor_start_ts = (
			missing_data
			.groupby('sensor_id')
			.get_group(sid)["start_ts"]
			- timedelta(seconds=FREQ[0])
		) # sensor start timing = missing data first timestamp
	else:  # if no missing data for this sensor
		default_timing = getLastRegisteredTimestamp(TBL_RAW, sid)  # None or tz-naive CET
		if default_timing is None:  # if no raw data registered for this sensor yet
			# we take its first tmpo timestamp
			initial_ts = getInitialTimestamp(tmpo_session, sid, now)
			sensor_start_ts = initial_ts
		else:
			sensor_start_ts = default_timing

	return sensor_start_ts


def getTimings(tmpo_session, config, missing_data, now):
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
				sensor_start_ts = getSensorTimings(tmpo_session, missing_data, sid, now)
				if type(sensor_start_ts) is not pd.Series:
					sensor_start_ts = [sensor_start_ts]
				# if 'start_ts' is older (in the past) than the current start_ts
				for ts in sensor_start_ts:
					logging.debug(f"sensors start ts : {ts}")
					if ts is not None and isEarlier(ts, timings[home_id]["start_ts"]):
							timings[home_id]["start_ts"] = ts

					if ts is not None and str(ts.tz) == "None":
							ts = ts.tz_localize("CET") # ensures a CET timezone
					timings[home_id]["sensors"][sid] = setInitSeconds(ts)  # CET

			if timings[home_id]["start_ts"] is now:  # no data to recover from this home 
				timings[home_id]["start_ts"] = None
		#truncate existing rows in Raw missing table
		ptc.deleteRows(CASSANDRA_KEYSPACE, TBL_RAW_MISSING)
		logging.debug("Missing raw table deleted")

	except:
		logging.critical("Exception occured in 'getTimings' : ", exc_info=True)

	return timings



def setCustomTimings(config, timings, custom_timings):
	""" 
	Set custom start timing and custom end timing for each home, and each sensor
	- Same custom timings for each home.
	"""
	try:
		for home_id, sensors_ids in config.getIds().items():
			timings[home_id] = {
				"start_ts": custom_timings["start_ts"],
				"end_ts": custom_timings["end_ts"], 
				"sensors": {}
			}
			for sid in sensors_ids:
				timings[home_id]["sensors"][sid] = setInitSeconds(custom_timings["start_ts"])  # CET

	except:
		logging.critical("Exception occured in 'setCustomTimings' : ", exc_info=True)



def processTimings(tmpo_session, config, missing_data, now, custom_timings):
	""" 
	Get the timings for each home 
	Timings are either custom or determined by the current database state.
	"""
	timings = {}
	if (len(custom_timings) > 0):
		setCustomTimings(config, timings, custom_timings)
	else:
		timings = getTimings(
			tmpo_session, 
			config,
			missing_data,
			now
		)

	return timings



# ====================================================================================

def generateHome(tmpo_session, hid, home_sensors, since_timing, to_timing):
	""" 
	Given a start timing and an end timing, generate a Home object containing the
	result of the query between these 2 timings. 
	"""
	sensors = []  # list of Sensor objects

	duration_min = round((to_timing - since_timing).total_seconds() / 60.0, 2)
	logging.info("  -> {} > {} ({} min.)".format(
		since_timing, 
		to_timing, 
		duration_min
	))

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


def saveHomeMissingData(config, to_timing, home, saved_sensors):
	"""
	Save the first timestamp with no data (nan values) for each sensors of the home
	"""
	hid = home.getHomeID()

	try:
		config_id = config.getConfigID().isoformat()

		col_names = ["sensor_id", "config_id", "start_ts", "end_ts"]
		
		inc_power_df = home.getIncompletePowerDF()
		if len(inc_power_df) > 0:
			sensors_ids = inc_power_df.columns
			for sid in sensors_ids:
				if saved_sensors.get(sid, None) is None:  # if no missing data saved for this sensor yet
					if inc_power_df[sid].isnull().values.any():  # if the column contains null
						for i, timestamp in enumerate(inc_power_df.index):
							# if valid timestamp
							if (to_timing - timestamp).days < LIMIT_TIMING_RAW:  # X days from now max
								if np.isnan(inc_power_df[sid][i]):
									values = [sid, config_id, timestamp, to_timing]
									ptc.insert(CASSANDRA_KEYSPACE, TBL_RAW_MISSING, col_names, values)
									saved_sensors[sid] = True  # mark that this sensor has missing data
									# as soon as we find the first ts with null value, we go to next sensor
									break

	
	except:
		logging.critical("Exception occured in 'saveHomeMissingData' : {} ".format(hid), exc_info=True)


def saveHomeRawToCassandra(home, config, timings):
	"""
	Save raw flukso flukso data to Cassandra table
	Save per sensor : 1 row = 1 sensor + 1 timestamp + 1 power value
		home_df : timestamp, sensor_id1, sensor_id2, sensor_id3 ... sensor_idN
	"""
	hid = home.getHomeID()

	try: 
		insertion_time = pd.Timestamp.now(tz="CET")
		config_id = config.getConfigID()

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
						power = date_rows[sid][i]
						values = [sid, date, timestamp, insertion_time, config_id, power]
						insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, TBL_RAW, col_names, values)

						if (i + 1) % INSERTS_PER_BATCH == 0:
							ptc.batch_insert(insert_queries)
							insert_queries = ""

				ptc.batch_insert(insert_queries)


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
		logging.info("> {} | {} > {} ({} days > {} min.)".format(
			home_id, 
			start_ts, 
			end_ts, 
			nb_days, 
			duration_min
		))
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


def saveDataThreads(home, config, timings, now, saved_sensors, custom):
	""" 
	Threads to save data to different Cassandra tables
	-> raw data in raw table
	-> raw missing data in raw_missing table
	-> power data in power table
	"""

	threads = []
	# save raw flukso data in cassandra
	t1 = Thread(
		target = saveHomeRawToCassandra, 
		args=(home, config, timings)
	)
	threads.append(t1)
	t1.start()

	if not custom:  # in custom mode, no need to save missing data (in the past)
		# save missing raw data in cassandra
		t2 = Thread(
			target = saveHomeMissingData, 
			args = (config, now, home, saved_sensors)
		)
		threads.append(t2)
		t2.start()

	# save power flukso data in cassandra
	t3 = Thread(
		target = saveHomePowerDataToCassandra, 
		args = (home, config)
	)
	threads.append(t3)
	t3.start()

	# wait for the threads to complete
	for t in threads:
		t.join()


def processHomes(tmpo_session, config, timings, now, custom):
	""" 
	For each home, we first create the home object containing
	all the tmpo queries and series computation
	Then, we save computed data in Cassandra tables. 
	"""

	# for each home
	for hid, home_sensors in config.getSensorsConfig().groupby("home_id"):
		saved_sensors = {}  # for missing data, to check if sensors missing data already saved
		# if home has a start timestamp and a end timestamp
		if timings[hid]["start_ts"] is not None and timings[hid]["end_ts"] is not None:
			# set init seconds (for tmpo query), might set timings earlier than planned (not a problem)
			start_timing = setInitSeconds(timings[hid]["start_ts"])
			end_timing = setInitSeconds(timings[hid]["end_ts"])
			intermediate_timings = getIntermediateTimings(start_timing, end_timing)
			displayHomeInfo(hid, start_timing, end_timing)

			for i in range(len(intermediate_timings)-1):  # query day by day
				start_ts = intermediate_timings[i]
				to_ts = intermediate_timings[i+1]

				# generate home
				home = generateHome(
					tmpo_session, 
					hid, 
					home_sensors, 
					start_ts, 
					to_ts
				)

				saveDataThreads(
					home, 
					config, 
					timings, 
					now, 
					saved_sensors,
					custom
				)

		else:
			logging.info("{} : No data to save".format(hid))


# ====================================================================================


def showConfigInfo(config):
	""" 
	Display Configuration stats/information
	"""

	logging.info("- Number of Homes :           {}".format(
		str(config.getNbHomes())
	))
	logging.info("- Number of Fluksos :         {}".format(
		str(len(set(config.getSensorsConfig().flukso_id)))
	))
	logging.info("- Number of Fluksos sensors : {}".format(
		str(len(config.getSensorsConfig()))
	))


def showProcessingTimes(begin, setup_time, t):
	"""
	Display processing time for each step of 1 query
	t = timer (dictionary with running timings)
	"""

	logging.info("--------------------- Timings --------------------")
	logging.info("> Setup time :                     {}.".format(
		getTimeSpent(begin, setup_time)
	))
	logging.info("> Timings computation :            {}.".format(
		getTimeSpent(t["start"], t["timing"])
	))
	logging.info("> Generate homes + saving in db :  {}.".format(
		getTimeSpent(t["timing"], t["homes"])
	))

	logging.info("> Total Processing time :          {}.".format(
		getTimeSpent(begin, time.time())
	))


def createTables():
	"""
	create the necessary tables for the flukso data synchronization
	""" 
	createRawFluksoTable(TBL_RAW)
	createRawMissingTable(TBL_RAW_MISSING)
	createPowerTable(TBL_POWER)


def sync(custom_timings):
	logging.info("====================== Sync ======================")

	# custom mode (custom start and end timings)
	custom = "start_ts" in custom_timings  # custom mode
	logging.info("- Custom mode :               " + str(custom))
	begin = time.time()

	# =============================================================

	now = pd.Timestamp.now(tz="CET").replace(microsecond=0)  # remove microseconds for simplicity

	# > Configuration
	config = getLastRegisteredConfig()
	missing_data = ptc.selectQuery(
		CASSANDRA_KEYSPACE,
		TBL_RAW_MISSING,
		["*"],
		where_clause="",
		limit=None,
		allow_filtering=False
	)

	logging.info("- Running time (Now - CET) :  " + str(now))
	setup_time = time.time()

	# =========================================================

	# Timer
	config_id = config.getConfigID()
	logging.info("- Config :                    " + str(config_id))
	timer = {"start": time.time()}

	# Config information
	showConfigInfo(config)

	logging.info("---------------------- Tmpo -----------------------")

	# TMPO synchronization
	tmpo_session = getTmpoSession(config)

	# STEP 1 : get start and end timings for all homes for the query
	timings = processTimings(
		tmpo_session,
		config,
		missing_data,
		now,
		custom_timings
	)
	timer["timing"] = time.time()

	# =========================================================

	logging.info("---------------------- Homes ---------------------")
	logging.info("Generating homes data, getting Flukso data and save in Cassandra...")

	# STEP 2 : process all homes data, and save in database
	processHomes(tmpo_session, config, timings, now, custom)

	timer["homes"] = time.time()

	# =========================================================

	showProcessingTimes(begin, setup_time, timer) 


def processCustomTimings(start, end):
	""" 
	Given the custom mode, check if the two provided arguments
	are valid. Namely, if the timestamp format is ok, and 
	start ts < end ts
	"""
	custom_timings = {}
	if start:
		try:
			custom_timings["start_ts"] = pd.Timestamp(start, tz="CET")
			custom_timings["end_ts"] = pd.Timestamp(end, tz="CET")
		except:
			logging.critical("Wrong argument format - custom timings : ", exc_info=True)
			sys.exit(1)

		if isEarlier(custom_timings["end_ts"], custom_timings["start_ts"]):
			logging.critical("Wrong arguments (custom timings) : first timing must be earlier than second timing")
			sys.exit(1)

	return custom_timings


def getArguments():
	""" 
	process arguments
	argument : custom mode : choose a interval between 2 specific timings
	"""
	argparser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter
	)

	argparser.add_argument(
		"--start", 
		type=str,
		help="Start timing. Format : YYYY-mm-ddTHH:MM:SS"
	)

	argparser.add_argument(
		"--end", 
		type=str,
		default=str(pd.Timestamp.now()),
		help="End timing. Format : YYYY-mm-ddTHH:MM:SS"
	)

	return argparser.parse_args()


def main():
	args = getArguments()

	# Custom timings argument
	custom_timings = processCustomTimings(args.start, args.end)

	# first, create tables if needed : 
	createTables()

	# then, sync new data in Cassandra
	sync(custom_timings)


if __name__ == "__main__":
	main()
