__title__ = "computePower"
__version__ = "0.0.1"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"

from constants import *
import pyToCassandra as ptc
from utils import *
from sensorConfig import Configuration
import logging

import copy
import pandas as pd

# ====================================================================================
# called by syncRawFluksoData.py just after writing raw data to cassandra
# ====================================================================================


def saveHomePowerDataToCassandra(cassandra_session, home, config):
	""" 
	save power flukso data to cassandra : P_cons, P_prod, P_tot
	- home : Home object 
		=> contains cons_prod_df : timestamp, P_cons, P_prod, P_tot
	"""
	hid = home.getHomeID()
	logging.debug("- saving in Cassandra: {} ...".format(TBL_POWER))

	try: 
		insertion_time = str(pd.Timestamp.now())[:19] + "Z"
		config_id = str(config.getConfigID())[:19] + "Z"

		cons_prod_df = home.getConsProdDF()
		cons_prod_df['date'] = cons_prod_df.apply(lambda row: str(row.name.date()), axis=1) # add date column
		by_day_df = cons_prod_df.groupby("date")  # group by date

		col_names = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot", "insertion_time", "config_id"]
		for date, date_rows in by_day_df:  # loop through each group (each date group)

			insert_queries = ""
			nb_inserts = 0
			for timestamp, row in date_rows.iterrows():
				# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
				ts = str(timestamp)[:19] + "Z"
				values = [hid, date, ts] + list(row)[:-1] + [insertion_time, config_id]  # [:-1] to avoid date column
				insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, TBL_POWER, col_names, values)

				if (nb_inserts+1) % INSERTS_PER_BATCH == 0:
					ptc.batch_insert(cassandra_session, insert_queries)
					insert_queries = ""

				nb_inserts+=1
		
			ptc.batch_insert(cassandra_session, insert_queries)
		
		logging.debug("   OK : power data saved.")
	except:
		logging.critial("Exception occured in 'saveHomePowerDataToCassandra' : {}".format(hid), exc_info=True)


def savePowerDataToCassandra(cassandra_session, homes, config, table_name):
	""" 
	save power flukso data to cassandra : P_cons, P_prod, P_tot
	- homes : Home objects 
		=> contains cons_prod_df : timestamp, P_cons, P_prod, P_tot
	"""
	logging.info("saving in Cassandra...   => table : {}".format(table_name))

	insertion_time = str(pd.Timestamp.now())[:19] + "Z"
	config_id = str(config.getConfigID())[:19] + "Z"
	for hid, home in homes.items():
		# logging.debug(hid)
		cons_prod_df = home.getConsProdDF()
		cons_prod_df['date'] = cons_prod_df.apply(lambda row: str(row.name.date()), axis=1) # add date column
		by_day_df = cons_prod_df.groupby("date")  # group by date

		col_names = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot", "insertion_time", "config_id"]
		for date, date_rows in by_day_df:  # loop through each group (each date group)

			insert_queries = ""
			nb_inserts = 0
			for timestamp, row in date_rows.iterrows():
				# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
				ts = str(timestamp)[:19] + "Z"
				values = [hid, date, ts] + list(row)[:-1] + [insertion_time, config_id]  # [:-1] to avoid date column
				insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, table_name, col_names, values)

				if (nb_inserts+1) % INSERTS_PER_BATCH == 0:
					ptc.batch_insert(cassandra_session, insert_queries)
					insert_queries = ""

				nb_inserts+=1
		
			ptc.batch_insert(cassandra_session, insert_queries)
	
	logging.info("Successfully saved power data in cassandra : table {}".format(table_name))


# =====================================================================================
# called whenever we want to modify the power table with given config and time interval
# =====================================================================================


def getHomeRawData(cassandra_session, sensors_df, day, config_id):
	""" 
	get raw flukso data from cassandra given a certain day and a certain configuration
	return dictionary of the format : 
		{home_id: {sensor_id1: df, sensor_id2: df, ...}, ...}
	"""

	home_rawdata = {}

	for sid in sensors_df.index:
		where_clause = "sensor_id = '{}' and day = '{}' and config_id = '{}.000000+0000'".format(sid, day, config_id)
		raw_data_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, TBL_RAW,
								["*"], where_clause, "ALLOW FILTERING", "")

		home_rawdata[sid] = raw_data_df

	return home_rawdata


def getHomeConsumptionProductionDf(home_rawdata, home_id, sensors_df):
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


def concentrateConsProdDf(cons_prod_df):
	""" 
	transform : index=i, cols = [home_id day ts P_cons P_prod P_tot]
	into : index = [day, ts], cols = [P_cons P_prod P_tot] 
	"""
	df = cons_prod_df.set_index(['day', 'ts'])
	df = df.drop(['home_id'], axis=1)
	return df


def saveRecomputedPowersToCassandra(cassandra_session, prev_config_id, cons_prod_df, table_name):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the raw data
	of some period of time in Cassandra for 1 specific home
	- cons_prod_df : home_id, day, ts, p_cons, p_prod, p_tot

	We assume that the data is of 1 specific date. 
	"""

	config_id = str(prev_config_id)[:19] + "Z"
	insertion_time = str(pd.Timestamp.now())[:19] + "Z"
	col_names = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot", "config_id", "insertion_time"]

	insert_queries = ""
	nb_inserts = 0
	for _, row in cons_prod_df.iterrows():
		values = list(row)
		values[2] = str(values[2]) + "Z"  # timestamp (ts)
		values.append(config_id)
		values.append(insertion_time)
		insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, table_name, col_names, values)

		if (nb_inserts+1) % INSERTS_PER_BATCH == 0:
			ptc.batch_insert(cassandra_session, insert_queries)
			insert_queries = ""

		nb_inserts+=1

	ptc.batch_insert(cassandra_session, insert_queries)


def getDataDatesFromConfig(cassandra_session, home_id, config_id, now):
	""" 
	From a config, get all registered dates in power table for a home
	"""
	where_clause = "home_id = '{}' and config_id = '{}.000000+0000'".format(home_id, config_id)
	first_date_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, TBL_POWER,
									["day"], where_clause, "ALLOW FILTERING", "LIMIT 1")

	all_dates = []
	if len(first_date_df) > 0:
		# print(first_date_df)

		first_date = pd.Timestamp(first_date_df.iloc[0]["day"])

		all_dates = getDatesBetween(first_date, now)
		# print(all_dates)

	return all_dates


def recomputePowerData(cassandra_session, prev_config_id, new_config, homes, now):
	""" 
	Given a configuration, recompute all power data for all select homes
	based on the existing raw data stored in Cassandra.
	"""
	
	config_by_home = new_config.getSensorsConfig().groupby("home_id")  # group by home
	if len(homes) == 0:
		homes = list(config_by_home.groups.keys())

	for hid in homes:
		print(hid)
		sensors_df = config_by_home.get_group(hid)  # new config

		# first select all dates registered with this config for this home
		all_dates = getDataDatesFromConfig(cassandra_session, hid, prev_config_id, now)
		# then, for each day, recompute data and store it in the database (overwrite existing data)
		for date in all_dates:

			print(date)
			# get raw data from previous config
			home_rawdata = getHomeRawData(cassandra_session, sensors_df, date, prev_config_id)

			# recompute power data with new config info : consumption, production, total
			home_powers = getHomeConsumptionProductionDf(home_rawdata, hid, sensors_df)

			# save (overwrite) to cassandra table  # TODO : change to 'power'
			if len(home_powers) > 0:
				saveRecomputedPowersToCassandra(cassandra_session, prev_config_id, home_powers, "power")


# ====================================================================================


def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	last_config = getLastRegisteredConfig(cassandra_session)
	
	recomputePowerData(cassandra_session, last_config, [])
	
	

if __name__ == "__main__":
	main()