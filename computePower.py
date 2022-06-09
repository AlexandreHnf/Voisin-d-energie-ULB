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


def getRawData(cassandra_session, config, day):
	""" 
	get raw flukso data from cassandra given a certain day and a certain configuration
	return dictionary of the format : 
		{hid: {sid1: df, sid2: df, ...}, ...}
	"""

	config_id = config.getConfigID()
	homes_rawdata = {}
	for hid, sensors_df in config.getSensorsConfig().groupby("home_id"):
		homes_rawdata[hid] = {}

		for sid in sensors_df.index:
			where_clause = "sensor_id = '{}' and day = '{}' and config_id = '{}.000000+0000'".format(sid, day, config_id)
			raw_data_df = ptc.selectQuery(cassandra_session, CASSANDRA_KEYSPACE, TBL_RAW,
									["*"], where_clause, "ALLOW FILTERING", "")
			# print(where_clause)
			# print(raw_data_df.head(3))
			homes_rawdata[hid][sid] = raw_data_df

	return homes_rawdata


def getConsumptionProductionDf(config, homes_rawdata):
	""" 
	compute power data from raw data (coming from cassandra 'raw' table) : 
	P_cons = P_tot - P_prod
	P_net = P_prod + P_cons
	"""

	homes_power = {}
	for hid, sensors_df in config.getSensorsConfig().groupby("home_id"):
		first_sid = list(homes_rawdata[hid].keys())[0]
		cons_prod_df = homes_rawdata[hid][first_sid][["sensor_id","day", "ts"]].copy()
		cons_prod_df = cons_prod_df.rename(columns={"sensor_id": "home_id"})
		cons_prod_df["home_id"] = hid  # replace 1st sensor_id by home_id
		cons_prod_df["P_cons"] = 0
		cons_prod_df["P_prod"] = 0
		cons_prod_df["P_tot"] = 0

		for sid in sensors_df.index:
			power_df = homes_rawdata[hid][sid]
			p = sensors_df.loc[sid]["pro"]
			n = sensors_df.loc[sid]["net"]

			cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * power_df["power"]
			cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * power_df["power"]

		cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

		cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals
		# print(cons_prod_df.head(3))
		homes_power[hid] = cons_prod_df

	return homes_power


def concentrateConsProdDf(cons_prod_df):
	""" 
	transform : index=i, cols = [home_id day ts P_cons P_prod P_tot]
	into : index = [day, ts], cols = [P_cons P_prod P_tot] 
	"""
	df = cons_prod_df.set_index(['day', 'ts'])
	df = df.drop(['home_id'], axis=1)
	return df


def saveRecomputedPowersToCassandra(cassandra_session, config, homes_powers, table_name):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the raw data
	of some period of time in Cassandra
	- homes_powers : {hid: cons_prod_df}
	- cons_prod_df : home_id, day, ts, p_cons, p_prod, p_tot

	We assume that the data is of 1 specific date. 
	"""
	logging.info("saving in Cassandra : flukso.{} table...".format(table_name))

	config_id = str(config.getConfigID())[:19] + "Z"
	insertion_time = str(pd.Timestamp.now())[:19] + "Z"
	col_names =  ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot", "config_id", "insertion_time"]

	for hid, cons_prod_df in homes_powers.items():
		print(hid)

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

	logging.info("Successfully saved powers in Cassandra")


# ====================================================================================

def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	last_config = getLastRegisteredConfig(cassandra_session)
	homes_rawdata = getRawData(cassandra_session, last_config, "2022-05-29") 
	
	logging.info("==============================================")

	# powers computations (p_cons, p_prod, p_tot)
	homes_powers = getConsumptionProductionDf(last_config, homes_rawdata)

	saveRecomputedPowersToCassandra(cassandra_session, last_config, homes_powers, "power2")
	
	

if __name__ == "__main__":
	main()