""" 
Author : Alexandre Heneffe

Script to fetch Fluksometer data using the tmpo protocol and 
- format it into tables of time series
- save the raw data in Cassandra database
"""

from home import *
from constants import *
import pyToCassandra as ptc
from sensorConfig import Configuration
from utils import *
import computePower as cp
from sensor import *

import argparse
import os

import pandas as pd
import numpy as np
import tmpo
import time

# Hide warnings :
import urllib3
import warnings

# security warning & Future warning
warnings.simplefilter('ignore', urllib3.exceptions.SecurityWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)

import logging

logging.getLogger("tmpo").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logging_handlers = [logging.FileHandler(LOG_FILE)]
if LOG_VERBOSE:
	logging_handlers.append(logging.StreamHandler())

# Create and configure logger
logging.basicConfig(level = setupLogLevel(),
					format = "{asctime} {levelname:<8} {message}", style='{',
					handlers=logging_handlers
					)

# ====================================================================================


def getLastRegisteredTimestamp(cassandra_session, table_name, sensor_id):
	"""
	get the last registered timestamp of the raw table
	- None if no timestamp in the table

	We assume that if there is data in raw table, each sensor can have different last timestmaps
	registered. 
	Assume the raw table is created
	"""

	dates = ["'" + d + "'" for d in getLastXDates()]
	ts_df = None
	for date in dates:
		where_clause = "sensor_id = '{}' AND day = {} ORDER BY ts DESC".format(
			sensor_id, date)
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
	if not last_timestamp.empty:  # != None
		return last_timestamp.iloc[0]['ts']
	else:  # if no registered timestamp in raw table yet
		return None


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


def getNeededConfigs(all_sensors_configs, missing_data, cur_sconfig):
	"""
	From the missing data table, deduce the configurations to use
	Return a list of Configuration objects
	all_sensors_configs : a groupby object : 
		keys = config ids (insertion dates), values = dataframe

	return : list of Configuration objects, each object = 1 sensors config
	"""
	configs = []
	needed_configs = list(missing_data.groups.keys())
	if cur_sconfig not in needed_configs:
		needed_configs.append(cur_sconfig)

	for config_id in needed_configs:
		config = Configuration(config_id, all_sensors_configs.get_group(config_id).set_index("sensor_id"))
		configs.append(config)

	return configs


def getInitialTimestamp(tmpo_session, sid, now):
	""" 
	get the first ever registered timestamp for a sensor using tmpo Session
	if no such timestamp (None), return an arbitrary timing (ex: since 4min)

	return a timestamp with UTC timezone
	"""
	initial_ts = setInitSeconds(getTiming(FROM_FIRST_TS, now))

	if FROM_FIRST_TS_STATUS == "server":
		initial_ts_tmpo = tmpo_session.first_timestamp(sid)

		if initial_ts_tmpo is not None:
			initial_ts = initial_ts_tmpo.tz_localize(None)

	# logging.debug("{} first ts = {} : {}".format(sid, initial_ts, initial_ts.tz))

	return initial_ts


def getSensorTimings(tmpo_session, cassandra_session, missing_data, timings, home_id, sid, config, current_config_id, now):
	"""
	For each sensor, we get start timing and the end timing, forming the interval of time
	we have to query to tmpo
	- The start timing is either based on the missing data table, or the initial timestamp 
	of the sensor if no missing data registered, or simply the default timing (the last
	registered timestamp in raw data table)

	if no missing data, and not the current config : 
		we simply do not put timings for the sensor = None

	return a starting timestamp with CET timezone 
		or None if no starting timestamp and end timestamp
	"""
	sensor_start_ts = None
	if missing_data.index.contains(sid):  # if there is missing data for this sensor
		start_ts = missing_data.loc[sid]["start_ts"]  # CET timezone
		end_ts = missing_data.loc[sid]["end_ts"]  # CET timezone

		sensor_start_ts = start_ts  # sensor start timing = missing data first timestamp

		if timings[home_id]["end_ts"] is None:
			# end timing is the same for each sensor of this home, so take the first one
			timings[home_id]["end_ts"] = end_ts.tz_localize("CET").tz_convert("UTC")

	else:  # if no missing data for this sensor
		default_timing = getDefaultTiming(cassandra_session, sid)  # None or tz-naive CET
		if config.getConfigID() == current_config_id:  # if current config
			if default_timing is None:  # if no raw data registered for this sensor yet
				# we take all data from its first timestamp
				initial_ts = getInitialTimestamp(tmpo_session, sid, now)
				sensor_start_ts = initial_ts
			else:
				sensor_start_ts = default_timing
		# if not the current config, then no data to recover from this sensor : no timings

	return sensor_start_ts


def getTimings(tmpo_session, cassandra_session, config, current_config_id, missing_data, table_name, now):
	"""
	For each home, get the start timing for the query based on the missing data table
	(containing for each sensor the first timestamp with missing data from the previous query)
	if no timestamp available yet for this sensor, we get the first ever timestamp available
	for the Flukso sensor with tmpo API

	default_timing = last registered timestamp for a home : UTC tz
	'now' : no tz, but CET by default
	"""
	timings = {}
	try: 
		ids = config.getIds()
		for home_id, sensors_ids in ids.items():
			# start_ts = earliest timestamp among all sensors of this home
			timings[home_id] = {"start_ts": now, "end_ts": None, "sensors": {}}

			for sid in sensors_ids:  # for each sensor of this home
				sensor_start_ts = getSensorTimings(tmpo_session, cassandra_session, missing_data, timings, 
													home_id, sid, config, current_config_id, now)

				# if 'start_ts' is older (in the past) than the current start_ts
				if sensor_start_ts is not None and isEarlier(sensor_start_ts, timings[home_id]["start_ts"]):
					timings[home_id]["start_ts"] = sensor_start_ts

				if sensor_start_ts is not None and str(sensor_start_ts.tz) == "None":
					sensor_start_ts = sensor_start_ts.tz_localize("CET") # ensures a CET timezone
				timings[home_id]["sensors"][sid] = sensor_start_ts  # CET

			# convert to UTC timezone for the tmpo query
			timings[home_id]["start_ts"] = timings[home_id]["start_ts"].tz_localize("CET").tz_convert("UTC")

			if timings[home_id]["start_ts"] is now:  # no data to recover from this home 
				timings[home_id]["start_ts"] = None
			
			if config.getConfigID() == current_config_id:
				timings[home_id]["end_ts"] = setInitSeconds(now).tz_localize("CET").tz_convert("UTC")
	except:
		logging.critial("Exception occured in 'getTimings' : ", exc_info=True)

	return timings


# ====================================================================================

def generateHome(tmpo_session, hid, home_sensors, since, since_timing, to_timing, timings, mode):
	home = None
	if timings[hid]["start_ts"] is not None:  # if home has a start timestamp
		sensors = []  # list of Sensor objects
		if mode == "automatic":
			since_timing = timings[hid]["start_ts"]
			to_timing = timings[hid]["end_ts"]

		logging.info("> {} | {} > {} ({} min.)".format(hid, since_timing, to_timing,
									round(pd.Timedelta(to_timing - since_timing).seconds / 60.0, 2)))

		for sid, row in home_sensors.iterrows():
			sensors.append(Sensor(tmpo_session, row["flukso_id"], sid, since_timing, to_timing))
		home = Home(home_sensors, since, since_timing, to_timing, hid, sensors)
		home.showQueryInfo()
	else:
		logging.info("None (no data to recover)")

	return home


def generateHomes(tmpo_session, config, since, since_timing, to_timing, timings, mode, now):
	"""
	Given a configuration (sensors information, homes..), generate home objects
	1 home = combination of sensors (Sensor objects).
	For each home, query tmpo to get data given a predefined time interval, then
	compute power raw data, production, consumption, total  
	"""
	logging.info("======================= HOMES =======================")
	homes = {}

	try:
		for hid, home_sensors in config.getSensorsConfig().groupby("home_id"):
			if timings[hid]["start_ts"] is not None:  # if home has a start timestamp
				sensors = []  # list of Sensor objects
				if mode == "automatic":
					since_timing = timings[hid]["start_ts"]
					to_timing = timings[hid]["end_ts"]

				logging.info("> {} | {} > {} ({} min.)".format(hid, since_timing, to_timing,
											round(pd.Timedelta(to_timing - since_timing).seconds / 60.0, 2)))

				for sid, row in home_sensors.iterrows():
					sensors.append(Sensor(tmpo_session, row["flukso_id"], sid, since_timing, to_timing))
				home = Home(home_sensors, since, since_timing, to_timing, hid, sensors)
				home.showQueryInfo()
				homes[hid] = home
			else:
				logging.info("None (no data to recover)")
	except:
		logging.critial("Exception occured in 'generateHomes' : ", exc_info=True)

	return homes



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
		# logging.debug("{}, {}".format(sid, row["sensor_token"]))
		tmpo_session.add(sid, row["sensor_token"])

	logging.info("> tmpo synchronization...")
	try: 
		tmpo_session.sync()
	except Exception as e:
		logging.warning("Exception occured in tmpo sync: ", exc_info=True)
		logging.warning("> tmpo sql file needs to be reset, or some sensors are invalid.")
		# testSession(config)
	logging.debug("> tmpo synchronization OK")

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


def saveHomeMissingData(cassandra_session, config, to_timing, home, table_name):
	"""
	Save the first timestamp with no data (nan values) for each sensors of the home
	"""
	hid = home.getHomeID()
	logging.info("- saving in Cassandra: {} ... ".format(hid, table_name))

	try: 
		to_timing = convertTimezone(to_timing, "CET")
		config_id = str(config.getConfigID())[:19] + "Z"

		col_names = ["sensor_id", "config_id", "start_ts", "end_ts"]
		
		inc_power_df = home.getIncompletePowerDF()
		if len(inc_power_df) > 0:
			sensors_ids = inc_power_df.columns
			for sid in sensors_ids:
				end_ts = str(inc_power_df.index[-1])[:19] + "Z"
				if inc_power_df[sid].isnull().values.any():  # if the column contains null
					for i, timestamp in enumerate(inc_power_df.index):
						# if valid timestamp
						if (to_timing - timestamp).days < LIMIT_TIMING_RAW:  # X days from now max
							# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
							start_ts = str(timestamp)[:19] + "Z"
							if np.isnan(inc_power_df[sid][i]):
								values = [sid, config_id, start_ts, end_ts]
								# print("{} | start : {}, end = {}", config_id, start_ts, end_ts)
								ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, table_name, col_names, values)
								# as soon as we find the first ts with null value, we go to next sensor
								break

		logging.info("   OK : missing raw data saved.")
	
	except:
		logging.critial("Exception occured in 'saveHomeMissingData' : {} ".format(hid), exc_info=True)


def saveMissingData(cassandra_session, config, to_timing, homes, table_name):
	"""
	For each home, save the first timestamp with no data (nan values) for each sensors
	"""
	logging.info("saving in Cassandra...   => table : {}".format(table_name))

	try: 
		ptc.deleteRows(cassandra_session, CASSANDRA_KEYSPACE, table_name)  # truncate existing rows

		to_timing = convertTimezone(to_timing, "CET")
		config_id = str(config.getConfigID())[:19] + "Z"

		col_names = ["sensor_id", "config_id", "start_ts", "end_ts"]
		for hid, home in homes.items():
			# logging.info(hid)
			inc_power_df = home.getIncompletePowerDF()
			if len(inc_power_df) > 0:
				sensors_ids = inc_power_df.columns
				for sid in sensors_ids:
					end_ts = str(inc_power_df.index[-1])[:19] + "Z"
					if inc_power_df[sid].isnull().values.any():  # if the column contains null
						for i, timestamp in enumerate(inc_power_df.index):
							# if valid timestamp
							if (to_timing - timestamp).days < LIMIT_TIMING_RAW:  # X days from now max
								# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
								start_ts = str(timestamp)[:19] + "Z"
								if np.isnan(inc_power_df[sid][i]):
									values = [sid, config_id, start_ts, end_ts]
									# print("{} | start : {}, end = {}", config_id, start_ts, end_ts)
									ptc.insert(cassandra_session, CASSANDRA_KEYSPACE, table_name, col_names, values)
									# as soon as we find the first ts with null value, we go to next sensor
									break

		logging.info("Successfully Saved raw missing data in Cassandra : table {}".format(table_name))
	
	except:
		logging.critial("Exception occured in 'saveMissingData' : ", exc_info=True)


def saveHomeRawToCassandra(cassandra_session, home, config, table_name, timings):
	"""
	Save raw flukso flukso data to Cassandra table
	Save per sensor : 1 row = 1 sensor + 1 timestamp + 1 power value
		home_df : timestamp, sensor_id1, sensor_id2, sensor_id3 ... sensor_idN
	"""
	hid = home.getHomeID()
	logging.info("- saving in Cassandra: {} ...".format(hid, table_name))

	try: 
		insertion_time = str(pd.Timestamp.now())[:19] + "Z"
		config_id = str(config.getConfigID())[:19] + "Z"

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
						ts = str(timestamp)[:19] + "Z"
						power = date_rows[sid][i]
						values = [sid, date, ts, insertion_time, config_id, power]
						insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, table_name, col_names, values)

						if (i + 1) % INSERTS_PER_BATCH == 0:
							ptc.batch_insert(cassandra_session, insert_queries)
							insert_queries = ""

				ptc.batch_insert(cassandra_session, insert_queries)

		logging.info("   OK : raw data saved.")

	except:
		logging.critial("Exception occured in 'saveHomeRawToCassandra' : ", exc_info=True)


def saveRawToCassandra(cassandra_session, homes, config, table_name, timings):
	"""
	Save raw flukso flukso data to Cassandra table
	Save per sensor : 1 row = 1 sensor + 1 timestamp + 1 power value
		home_df : timestamp, sensor_id1, sensor_id2, sensor_id3 ... sensor_idN
	"""
	logging.info("saving in Cassandra...   => table : {}".format(table_name))

	try: 
		insertion_time = str(pd.Timestamp.now())[:19] + "Z"
		config_id = str(config.getConfigID())[:19] + "Z"
		for hid, home in homes.items():
			# logging.info(hid)
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
							ts = str(timestamp)[:19] + "Z"
							power = date_rows[sid][i]
							values = [sid, date, ts, insertion_time, config_id, power]
							insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, table_name, col_names, values)

							if (i + 1) % INSERTS_PER_BATCH == 0:
								ptc.batch_insert(cassandra_session, insert_queries)
								insert_queries = ""

					ptc.batch_insert(cassandra_session, insert_queries)

		logging.info("> Successfully Saved raw data in Cassandra : table {}".format(table_name))

	except:
		logging.critial("Exception occured in 'saveRawToCassandra' : ", exc_info=True)


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
	logging.info("================= Timings ===================")
	logging.info("> Setup time : {}.".format(getTimeSpent(begin, setup_time)))

	for i, config in enumerate(configs):
		config_id = config.getConfigID()
		logging.info("> Config {} : {}".format(i + 1, str(config_id)))
		t = config_timers[config_id]
		logging.info("  > Timings : {}.".format(getTimeSpent(t["start"], t["timing"])))
		logging.info("  > Generate homes + saving in db : {}.".format(getTimeSpent(t["timing"], t["homes"])))

	logging.info("> Total Processing time : {}.".format(getTimeSpent(begin, time.time())))


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


def sync(mode, since, to):
	logging.info("======================================================================")
	logging.info("======================================================================")
	begin = time.time()
	
	# > timings
	now = pd.Timestamp.now(tz="UTC").replace(microsecond=0)  # remove microseconds for simplicity
	now_local = pd.Timestamp.now().replace(microsecond=0)  # default tz = CET, unaware timestamp
	start_timing = setInitSeconds(getTiming(since, now))  # UTC
	to_timing = setInitSeconds(getTiming(to, now))  # UTC

	# =============================================================

	# > Configurations
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	current_config_id, all_sensors_configs = getCurrentSensorsConfigCassandra(cassandra_session, TBL_SENSORS_CONFIG)

	missing_data = getMissingRaw(cassandra_session, TBL_RAW_MISSING)
	configs = getNeededConfigs(all_sensors_configs, missing_data, current_config_id)

	logging.info("Mode : {}".format(mode))
	logging.info("start timing : {} (CET) => {} (UTC)".format(since, start_timing))
	logging.info("to timing    : {} (CET) => {} (UTC)".format(to, to_timing))
	logging.info("now (CET) : " + str(now_local))
	logging.info("Number of configs : " + str(len(configs)))

	setup_time = time.time()

	# =========================================================

	config_timers = {}

	for config in configs:
		config_id = config.getConfigID()
		logging.info("                [CONFIG {}] : ".format(str(config_id)))
		config_timers[config_id] = {"start": time.time()}

		logging.info("Number of Homes : " + str(config.getNbHomes()))
		logging.info("Number of Fluksos : " + str(len(set(config.getSensorsConfig().flukso_id))))
		logging.info("Number of Fluksos sensors : " + str(len(config.getSensorsConfig())))

		tmpo_session = getTmpoSession(config)
		# testSession(sensors_config)

		# STEP 1 : get start and end timings for all homes for the query
		missing = pd.DataFrame([])
		if config_id in missing_data.groups.keys():  # if missing table contains this config id
			missing = missing_data.get_group(config_id).set_index("sensor_id")
		timings = getTimings(tmpo_session, cassandra_session, config, current_config_id, missing, 
							TBL_RAW_MISSING, now_local)
		# logging.info(timings)
		config_timers[config_id]["timing"] = time.time()

		# =========================================================

		logging.info("==================================================")
		logging.info("Generating homes data, getting Flukso data and save in Cassandra...")
		logging.info("==================================================")

		ptc.deleteRows(cassandra_session, CASSANDRA_KEYSPACE, TBL_RAW_MISSING)  # truncate existing rows

		# for each home
		for hid, home_sensors in config.getSensorsConfig().groupby("home_id"):

			# STEP 2 : generate home
			home = generateHome(tmpo_session, hid, home_sensors, since, start_timing, to_timing, timings, mode)

			# STEP 3 : save raw flukso data in cassandra
			saveHomeRawToCassandra(cassandra_session, home, config, TBL_RAW, timings)

			# STEP 4 : save missing raw data in cassandra
			saveHomeMissingData(cassandra_session, config, now, home, TBL_RAW_MISSING)

			# STEP 5 : save power flukso data in cassandra
			cp.saveHomePowerDataToCassandra(cassandra_session, home, config, TBL_POWER)

			logging.info("---------------------------------------------------------------")

		config_timers[config_id]["homes"] = time.time()

	# =========================================================

	showProcessingTimes(configs, begin, setup_time, config_timers) 


def main():

	# > arguments
	argparser = processArguments()
	args = argparser.parse_args()
	mode = args.mode
	since = args.since
	to = args.to

	sync(mode, since, to)


if __name__ == "__main__":
	main()
