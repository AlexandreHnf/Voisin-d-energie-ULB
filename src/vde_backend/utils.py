__title__ = "utils"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, and Guillaume Levasseur"
__license__ = "MIT"


# standard library
import json
import os
import sys
import os.path
import math
from datetime import timedelta

# 3rd party packages
import pandas as pd
import numpy as np
import logging
import logging.handlers

# local sources
from constants import (
	PROD,
	CASSANDRA_KEYSPACE, 
	FREQ, 
	LOG_LEVEL,
	LOG_FILE,
	LOG_HANDLER,
	TBL_POWER,
	TBL_SENSORS_CONFIG
)

from sensorConfig import Configuration
import pyToCassandra as ptc


def setupLogLevel():
	""" 
	set logging level based on a constant
	levels : 
	- CRITICAL
	- ERROR
	- WARNING
	- INFO
	- DEBUG
	"""
	if LOG_LEVEL == "CRITICAL":
		return logging.CRITICAL
	elif LOG_LEVEL == "ERROR":
		return logging.ERROR
	elif LOG_LEVEL == "WARNING":
		return logging.WARNING
	elif LOG_LEVEL == "INFO":
		return logging.INFO
	elif LOG_LEVEL == "DEBUG":
		return logging.DEBUG


def getLogHandler():
	""" 
	If prod : rotating logfile handler
	If dev : only stdout
	"""
	if LOG_HANDLER == "logfile":
		handler = logging.handlers.TimedRotatingFileHandler(
			LOG_FILE,
			when='midnight',
			backupCount=7,
		)
	else:  # stdout
		handler = logging.StreamHandler(stream=sys.stdout)
	
	return handler


# Create and configure logger
logging.getLogger("tmpo").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

logging.basicConfig(
	level = setupLogLevel(),
	format = "{asctime} {levelname:<8} {filename:<16} {message}",
    style='{',
	handlers=[getLogHandler()]
)


def getProgDir():
	import __main__
	main_path = os.path.abspath(__main__.__file__)
	main_path = os.path.dirname(main_path) + os.sep
	return main_path


def read_sensor_info(path, sensor_file):
	"""
	read csv file of sensors data
	"""
	path += sensor_file
	sensors = pd.read_csv(
		path, 
		header=0, 
		index_col=1
	)
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


def getLastRegisteredConfig(cassandra_session):
	"""
	Get the last registered config based on insertion time
	"""
	latest_configs = ptc.groupbyQuery(
		cassandra_session,
		CASSANDRA_KEYSPACE,
		TBL_SENSORS_CONFIG,
		column='insertion_time',
		groupby_operator='max',
		groupby_cols=['home_id', 'sensor_id'],
		allow_filtering=False,
		tz="UTC"
	)
	last_config_id = latest_configs.max().max().tz_localize('UTC')
	config_df = ptc.selectQuery(
		cassandra_session,
		CASSANDRA_KEYSPACE,
		TBL_SENSORS_CONFIG,
		["*"],
		"insertion_time = '{}'".format(last_config_id),
	)
	config = Configuration(last_config_id, config_df.set_index("sensor_id"))
	return config


def getAllRegisteredConfigs(cassandra_session):
	""" 
	Get all configs present in the system
	returns a list of config ids

	POTENTIAL ISSUE : if the whole config table does not fit in memory
		-> possible solution : select distinct insertion_time, home_id, sensor_id to reduce the nb of queried lines
	"""
	all_configs_df = ptc.selectQuery(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		TBL_SENSORS_CONFIG,
		["*"], 
		where_clause="", 
		limit=None, 
		allow_filtering=False
	)

	configs = []
	if len(all_configs_df) > 0:
		for config_id, config in all_configs_df.groupby("insertion_time"):

			configs.append(Configuration(config_id, config.set_index("sensor_id")))

	return configs


def getHomePowerDataFromCassandra(cassandra_session, home_id, date, ts_clause=""):
	""" 
	Get power data from Power table in Cassandra
	> for 1 specific home
	> specific day
	"""

	where_clause = "home_id = '{}' and day = '{}' {}".format(home_id, date, ts_clause)
	cols = [
		"home_id", 
		"day", 
		"ts", 
		"p_cons", 
		"p_prod", 
		"p_tot"
	]

	home_df = ptc.selectQuery(
		cassandra_session, 
		CASSANDRA_KEYSPACE, 
		TBL_POWER, 
		cols, 
		where_clause,
	)

	return home_df


def getDatesBetween(start_date, end_date):
	""" 
	get the list of dates between 2 given dates
	"""
	d = pd.date_range(
		start_date.date(), 
		end_date.date()-timedelta(days=1), 
		freq='d'
	)

	dates = []
	for ts in d:
		dates.append(str(ts.date()))
	dates.append(str(end_date.date()))

	return dates


def toEpochs(time):
	return int(math.floor(time.value / 1e9))


def isEarlier(ts1, ts2):
	""" 
	check if timestamp 'ts1' is earlier/older than timestamp 'ts2'
	"""
	return (ts1 - ts2) / np.timedelta64(1, 's') < 0


def time_range(since, until):
	return pd.date_range(
		since,
		until,
		freq=str(FREQ[0]) + FREQ[1],
		closed='left',
	)


def resample_extend(df, since_timing, to_timing):
	"""
	pandas' df.resample only returns a DataFrame whose index matches the input index.
	On the opposite, this function guarrantees that the index of the returned,
	resampled DataFrame start at since_timing and end at to_timing.
	"""
	filled_df = (df
		# Resampling with origin is needed for later reindexing. If indexes are
		# unaligned, the data is lost, replaced by NaNs.
		.resample(str(FREQ[0]) + FREQ[1], origin=since_timing)
		.mean()
		.reindex(time_range(since_timing, to_timing))
	)
	return filled_df


def energy2power(energy_df):
	"""
	from cumulative energy to power (Watt)
	"""
	power_df = energy_df.diff() * 1000
	power_df.fillna(0, inplace=True)
	
	power_df = power_df.resample(str(FREQ[0]) + FREQ[1]).mean()

	return power_df


def getTimeSpent(time_begin, time_end):
	""" 
	Get the time spent in seconds between 2 timings (1 timing = time.time())
	"""
	return timedelta(seconds=time_end - time_begin)


def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	config = getLastRegisteredConfig(cassandra_session)
	print(config.getSensorsConfig())


if __name__ == '__main__':
	main()
