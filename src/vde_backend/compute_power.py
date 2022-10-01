__title__ = "compute_power"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, and Guillaume Levasseur"
__license__ = "MIT"



# standard library

# 3rd party packages
import logging
import pandas as pd

# local source
from constants import (
	CASSANDRA_KEYSPACE, 
	INSERTS_PER_BATCH, 
	TBL_POWER, 
	TBL_RAW
)
import py_to_cassandra as ptc
from utils import (
	get_dates_between, 
	get_last_registered_config
)


# ====================================================================================
# called by syncRawFluksoData.py just after writing raw data to cassandra
# ====================================================================================


def save_home_power_data_to_cassandra(home, config):
	""" 
	save power flukso data to cassandra : P_cons, P_prod, P_tot
	- home : Home object 
		=> contains cons_prod_df : timestamp, P_cons, P_prod, P_tot
	"""
	hid = home.get_home_id()

	try: 
		insertion_time = pd.Timestamp.now(tz="CET")
		config_id = config.get_config_id()

		cons_prod_df = home.get_cons_prod_df()
		# add date column
		cons_prod_df['date'] = cons_prod_df.apply(lambda row: str(row.name.date()), axis=1) 
		by_day_df = cons_prod_df.groupby("date")  # group by date

		col_names = [
			"home_id", 
			"day", 
			"ts", 
			"p_cons", 
			"p_prod", 
			"p_tot", 
			"insertion_time", 
			"config_id"
		]
		for date, date_rows in by_day_df:  # loop through each group (each date group)

			insert_queries = ""
			nb_inserts = 0
			for timestamp, row in date_rows.iterrows():
				# [:-1] to avoid date column
				values = [hid, date, timestamp] + list(row)[:-1] + [insertion_time, config_id]  
				insert_queries += ptc.get_insert_query(
					CASSANDRA_KEYSPACE, 
					TBL_POWER, 
					col_names, 
					values
				)

				if (nb_inserts+1) % INSERTS_PER_BATCH == 0:
					ptc.batch_insert(insert_queries)
					insert_queries = ""

				nb_inserts+=1
		
			ptc.batch_insert(insert_queries)
		
	except:
		logging.critical("Exception occured in 'save_home_power_data_to_cassandra' : {}".format(hid), exc_info=True)


# =====================================================================================
# called whenever we want to modify the power table with time interval
# =====================================================================================


def get_home_raw_data(sensors_df, day):
	""" 
	get raw flukso data from cassandra given a certain day
	return dictionary of the format : 
		{home_id: {sensor_id1: df, sensor_id2: df, ...}, ...}
	"""

	home_rawdata = {}

	for sid in sensors_df.index:
		where_clause = "sensor_id = '{}' and day = '{}'".format(sid, day)
		raw_data_df = ptc.select_query(
			CASSANDRA_KEYSPACE, 
			TBL_RAW,
			["sensor_id, day, ts, power"], 
			where_clause
		)

		home_rawdata[sid] = raw_data_df

	return home_rawdata


def get_home_consumption_production_df(home_rawdata, home_id, sensors_df):
	""" 
	compute power data from raw data (coming from cassandra 'raw' table) : 
	P_cons = P_tot - P_prod
	P_net = P_prod + P_cons
	"""

	first_sid = list(home_rawdata.keys())[0]
	cons_prod_df = home_rawdata[first_sid][["sensor_id","day", "ts"]].copy()
	cons_prod_df = cons_prod_df.rename(columns={"sensor_id": "home_id"})
	cons_prod_df["home_id"] = home_id  # replace 1st sensor_id by home_id
	cons_prod_df["P_cons"] = 0
	cons_prod_df["P_prod"] = 0
	cons_prod_df["P_tot"] = 0

	for sid in sensors_df.index:
		power_df = home_rawdata[sid]
		p = sensors_df.loc[sid]["pro"]
		n = sensors_df.loc[sid]["net"]

		cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * power_df["power"]
		cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * power_df["power"]

	cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

	cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals

	return cons_prod_df


def save_recomputed_powers_to_cassandra(new_config_id, cons_prod_df):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the raw data
	of some period of time in Cassandra for 1 specific home
	- cons_prod_df : home_id, day, ts, p_cons, p_prod, p_tot

	We assume that the data is of 1 specific date. 
	"""
	insertion_time = pd.Timestamp.now(tz="CET")
	col_names = [
		"home_id", 
		"day", 
		"ts", 
		"p_cons", 
		"p_prod", 
		"p_tot", 
		"config_id", 
		"insertion_time"
	]
	insert_queries = ""
	nb_inserts = 0
	print(cons_prod_df)
	for _, row in cons_prod_df.iterrows():
		values = list(row)
		values.append(new_config_id)
		values.append(insertion_time)
		insert_queries += ptc.get_insert_query(
			CASSANDRA_KEYSPACE, 
			TBL_POWER, 
			col_names, 
			values
		)
		if (nb_inserts+1) % INSERTS_PER_BATCH == 0:
			ptc.batch_insert(insert_queries)
			insert_queries = ""

		nb_inserts+=1

	ptc.batch_insert(insert_queries)


def get_data_dates_from_home(sensors_df):
	""" 
	From a home, query the first date in the raw data.
	then, return all dates between the first date and now.
	"""
	now = pd.Timestamp.now()
	first_date = now
	for sensor_id in list(sensors_df.index):  # sensor ids
		first_date_df = ptc.select_query(
			CASSANDRA_KEYSPACE,
			TBL_RAW,
			["day"],
			"sensor_id = '{}'".format(sensor_id),
			limit=1
		)
		if len(first_date_df) > 0:
			if pd.Timestamp(first_date_df.iat[0,0]) < first_date:
				first_date = pd.Timestamp(first_date_df.iat[0,0])

	all_dates = get_dates_between(first_date, now)
	return all_dates


def exist_home_power_data(home_id, date):
	""" 
	From a home, query the first date in the raw data.
	return True if there is at least 1 row of data for this home, 
	given the specified date.
	"""
	first_date_df = ptc.select_query(
		CASSANDRA_KEYSPACE,
		TBL_POWER,
		["p_cons"],
		"home_id = '{}' and day = '{}'".format(home_id, date),
		limit=1
	)
	return len(first_date_df) > 0


def recompute_power_data(new_config, homes):
	""" 
	Given a configuration, recompute all power data for all select homes
	based on the existing raw data stored in Cassandra.
	"""
	config_by_home = new_config.get_sensors_config().groupby("home_id")  # group by home
	new_config_id = new_config.get_config_id()
	if len(homes) == 0:
		homes = list(config_by_home.groups.keys())

	for hid in homes:
		sensors_df = config_by_home.get_group(hid) # new config
		# first select all dates registered for this home
		all_dates = get_data_dates_from_home(sensors_df)

		if len(all_dates) > 0 and exist_home_power_data(hid, all_dates[0]):
			# then, for each day, recompute data and store it in the database (overwrite existing data)
			for date in all_dates:
				# get raw data from previous config
				home_rawdata = get_home_raw_data(sensors_df, date)
				# Check if raw data is not empty
				if len(home_rawdata) > 0:
					# recompute power data with new config info : consumption, production, total
					home_powers = get_home_consumption_production_df(home_rawdata, hid, sensors_df)

					# save (overwrite) to cassandra table
					if len(home_powers) > 0:
						save_recomputed_powers_to_cassandra(new_config_id, home_powers)


# ====================================================================================


def main():
	last_config = get_last_registered_config()
	recompute_power_data(last_config, [])


if __name__ == "__main__":
	main()
