import pandas as pd
import numpy as np
import os
import math
from constants import *
from sensorConfig import Configuration
from datetime import date, timedelta, datetime
import logging

import pyToCassandra as ptc

import logging



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
	sensors = pd.read_csv(path, header=0, index_col=1)
	return sensors
	

def getCurrentSensorsConfigCassandra(cassandra_session, table_name):
	""" 
	Get the last registered sensors configuration
	sensors configurations columns : home ids, sensors ids, tokens, indices for each phase (net, pro, con)
	method : get all rows of the table, groupby config id (sorted by dates) and pick the last one
	
	POTENTIAL ISSUE : if the whole config table does not fit in memory
		-> possible solution : select distinct insertion_time, home_id, sensor_id to reduce the nb of queried lines
	"""

	all_configs_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, table_name, ["*"], 
				"", "allow filtering", "")
	
	by_date_df = all_configs_df.groupby("insertion_time")
	dates = list(by_date_df.groups.keys())
	current_config_id = dates[-1]   # last config registered

	return current_config_id, by_date_df	


def getLastRegisteredConfig(cassandra_session):
	""" 
	Get the last registered config based on insertion time

	POTENTIAL ISSUE : if the whole config table does not fit in memory
		-> possible solution : select distinct insertion_time, home_id, sensor_id to reduce the nb of queried lines
	"""
	all_configs_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, TBL_SENSORS_CONFIG,
									["*"], "", "", "").groupby("insertion_time")
	
	config = None
	if len(all_configs_df) > 0:
		config_ids = list(all_configs_df.groups.keys())  # keys sorted by default
		# print(config_ids)

		last_config_id = config_ids[-1]
		config = Configuration(last_config_id, all_configs_df.get_group(last_config_id).set_index("sensor_id"))

	return config


def getAllRegisteredConfigs(cassandra_session):
	""" 
	Get all configs present in the system
	returns a list of config ids

	POTENTIAL ISSUE : if the whole config table does not fit in memory
		-> possible solution : select distinct insertion_time, home_id, sensor_id to reduce the nb of queried lines
	"""
	all_configs_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, TBL_SENSORS_CONFIG,
									["insertion_time, home_id, sensor_id"], "", "", "")

	configs = []
	if len(all_configs_df) > 0:
		for config_id, config in all_configs_df.groupby("insertion_time"):
			print("config ", config_id)
			print(config)

			configs.append(Configuration(config_id, config.set_index("sensor_id")))

	return configs


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
	since = "Xdays" or "Xhours" or "Xmin" or a specific date : "sYYYY-MM-DD-H-M-S"
	return format : YY-MM-DD H-M-S UTC
	return a timestamp with UTC timezone
	"""
	timing = 0
	if t:
		if t[0] == "s":
			e = t[1:].split("-")
			timing = pd.Timestamp(year=int(e[0]), month=int(e[1]), day=int(e[2]),
								  hour=int(e[3]), minute=int(e[4]), second=int(e[5]), 
								  tz="CET").tz_convert("UTC")
		else:  # since x min, x hours, x days...
			# logging.info("time delta : " + pd.Timedelta(t))
			timing = now - pd.Timedelta(t)
	else:
		timing = now  # now

	# logging.info("timing : " + timing)
	return timing


def convertTimezone(ts, timezone):
	""" 
	Convert a timestamp timezone to another 
	ex:  CET > UTC or UTC > CET
	We assume the timestamp is already aware
	"""
	return ts.tz_convert(timezone)


def getDatesBetween(start_date, end_date):
	""" 
	get the list of dates between 2 given dates
	"""
	d = pd.date_range(start_date.date(), end_date.date()-timedelta(days=1), freq='d')
	dates = []
	for ts in d:
		dates.append(str(ts.date()))
	dates.append(str(end_date.date()))

	return dates


def getLastXDates():
	""" 
	get the last x days from now
	ex : before_yesterday, yesterday, now
	"""
	dates = []
	day = datetime.now()
	dates.append(day.strftime('%Y-%m-%d'))
	for _ in range(LAST_TS_DAYS):
		day = (day - timedelta(1))  # the day before
		dates.append(day.strftime('%Y-%m-%d'))

	return dates


def getLocalTimestampsIndex(df):
    """
    set timestamps to local timezone
    """

    # NAIVE
    if df.index.tzinfo is None or df.index.tzinfo.utcoffset(df.index) is None:
        # first convert to aware timestamp, then local
        return df.index.tz_localize("CET", ambiguous='NaT').tz_convert("CET")
    else: # if already aware timestamp
        return df.index.tz_convert("CET")


def toEpochs(time):
	return int(math.floor(time.value / 1e9))


def isEarlier(ts1, ts2):
	""" 
	check if timestamp 'ts1' is earlier/older than timestamp 'ts2'
	"""
	return (ts1 - ts2) / np.timedelta64(1, 's') < 0


def getSpecificSerie(value, since_timing, to_timing):

	period = (to_timing - since_timing).total_seconds() / FREQ[0]
	# logging.info("s : {}, t : {}, to : {}, period : {}".format(self.since_timing, self.to_timing, to, period))
	values = pd.date_range(since_timing, periods=period, freq=str(FREQ[0]) + FREQ[1])
	# logging.info("datetime range : " + values)
	values_series = pd.Series(int(period) * [value], values)
	# logging.info(since_timing, values_series.index[0])

	return values_series


def energy2power(energy_df):
	"""
	from cumulative energy to power (Watt)
	"""
	# logging.info(energy_df.head(10))
	power_df = energy_df.diff() * 1000
	# power_df.drop(first_ts, inplace=True)  # drop first row because no data after conversion
	# replace all negative values by 0, power can't be negative
	# power_df[power_df < 0] = 0  
	power_df.fillna(0, inplace=True)
	
	power_df = power_df.resample(str(FREQ[0]) + FREQ[1]).mean()

	return power_df


def getTimeSpent(time_begin, time_end):
	""" 
	Get the time spent in seconds between 2 timings (1 timing = time.time())
	"""
	return timedelta(seconds=time_end - time_begin)


def getIntermediateTimings(start_ts, end_ts):
	""" 
	Given 2 timestamps, generate the intermediate timings
	- interval duration = 1 day
	"""
	intermediate_timings = [start_ts]
	nb_days = (end_ts - start_ts).days
	current_ts = start_ts
	for _ in range(nb_days):
		current_ts += timedelta(days = 1)
		intermediate_timings.append(current_ts)
	if end_ts != current_ts:
		intermediate_timings.append(end_ts)

	return intermediate_timings


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


def main():
	# start_ts = pd.Timestamp('2022-05-01 08:12:00')
	start_ts = pd.Timestamp('2022-05-07 08:16:00')
	end_ts = pd.Timestamp('2022-05-08 08:16:00')
	it = getIntermediateTimings(start_ts, end_ts)
	print(it)

if __name__ == '__main__':
	main()