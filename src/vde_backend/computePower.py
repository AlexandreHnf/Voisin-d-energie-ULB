__title__ = "computePower"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"



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
import pyToCassandra as ptc
from utils import (
	getDatesBetween, 
	getLastRegisteredConfig
)


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
		insertion_time = pd.Timestamp.now(tz="CET").isoformat()
		config_id = config.getConfigID().isoformat()

		cons_prod_df = home.getConsProdDF()
		cons_prod_df['date'] = cons_prod_df.apply(lambda row: str(row.name.date()), axis=1) # add date column
		by_day_df = cons_prod_df.groupby("date")  # group by date

		col_names = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot", "insertion_time", "config_id"]
		for date, date_rows in by_day_df:  # loop through each group (each date group)

			insert_queries = ""
			nb_inserts = 0
			for timestamp, row in date_rows.iterrows():
				# save timestamp with CET local timezone, ISO format.
				ts = timestamp.isoformat()
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


# =====================================================================================
# called whenever we want to modify the power table with given config and time interval
# =====================================================================================


def getHomeRawData(cassandra_session, sensors_df, day):
	""" 
	get raw flukso data from cassandra given a certain day and a certain configuration
	return dictionary of the format : 
		{home_id: {sensor_id1: df, sensor_id2: df, ...}, ...}
	"""

	home_rawdata = {}

	for sid in sensors_df.index:
		where_clause = "sensor_id = '{}' and day = '{}'".format(sid, day)
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


def saveRecomputedPowersToCassandra(cassandra_session, prev_config_id, cons_prod_df):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the raw data
	of some period of time in Cassandra for 1 specific home
	- cons_prod_df : home_id, day, ts, p_cons, p_prod, p_tot

	We assume that the data is of 1 specific date. 
	"""
	config_id = prev_config_id.isoformat()
	insertion_time = pd.Timestamp.now(tz="CET").isoformat()
	col_names = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot", "config_id", "insertion_time"]
	insert_queries = ""
	nb_inserts = 0
	for _, row in cons_prod_df.iterrows():
		values = list(row)
		values[2] = values[2].isoformat() # timestamp (ts)
		values.append(config_id)
		values.append(insertion_time)
		insert_queries += ptc.getInsertQuery(CASSANDRA_KEYSPACE, TBL_POWER, col_names, values)
		if (nb_inserts+1) % INSERTS_PER_BATCH == 0:
			ptc.batch_insert(cassandra_session, insert_queries)
			insert_queries = ""

		nb_inserts+=1

	ptc.batch_insert(cassandra_session, insert_queries)


def getDataDatesForHome(cassandra_session, sensors_df):
	""" 
	From a home, query the first date in the raw data.
	"""
	now = pd.Timestamp.now(tz="CET")
	first_date = now
	for sensor_id in sensors_df["sensor_id"]:
		first_date_df = ptc.selectQuery(
			cassandra_session,
			CASSANDRA_KEYSPACE,
			TBL_RAW,
			["day"],
			"sensor_id = '{}'".format(sensor_id),
			"ALLOW FILTERING",
			"LIMIT 1"
		)
		if len(first_date_df) > 0:
			if pd.Timestamp(first_date_df.iat[0,0]) < first_date:
				first_date = pd.Timestamp(first_date_df.iat[0,0])

	all_dates = getDatesBetween(first_date, now)
	return all_dates


def recomputePowerData(cassandra_session, new_config, homes):
	""" 
	Given a configuration, recompute all power data for all select homes
	based on the existing raw data stored in Cassandra.
	"""
	config_by_home = new_config.getSensorsConfig().groupby("home_id")  # group by home
	new_id = new_config.getConfigID().isoformat()
	if len(homes) == 0:
		homes = list(config_by_home.groups.keys())

	for hid in homes:
		sensors_df = config_by_home.get_group(hid)  # new config
		# first select all dates registered for this home
		all_dates = getDataDatesForHome(cassandra_session, sensors_df)
		# then, for each day, recompute data and store it in the database (overwrite existing data)
		for date in all_dates:
			# get raw data from previous config
			home_rawdata = getHomeRawData(cassandra_session, sensors_df, date)
			# recompute power data with new config info : consumption, production, total
			home_powers = getHomeConsumptionProductionDf(home_rawdata, hid, sensors_df)

			# save (overwrite) to cassandra table
			if len(home_powers) > 0:
				saveRecomputedPowersToCassandra(cassandra_session, new_id, home_powers)


# ====================================================================================


def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	last_config = getLastRegisteredConfig(cassandra_session)
	recomputePowerData(cassandra_session, last_config, [])


if __name__ == "__main__":
	main()
