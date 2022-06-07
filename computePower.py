from constants import *
import pyToCassandra as ptc
from utils import *
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


# ====================================================================================
# called manually whenever we want to modify the power table with given time interval
# ====================================================================================


def getRawData(session, since, ids, table_name):
	""" 
	get raw flukso data from cassandra since a certain amount of time
	return dictionary of the format : 
		{hid: {sid1: df, sid2: df, ...}, ...}
	"""
	now = pd.Timestamp.now(tz="CET")
	timing = setInitSeconds(getTiming(since, now))

	logging.info("timing date : " + str(timing.date()))
	logging.info("now date : " + str(now.date()))
	dates = ["'" + d + "'" for d in getDatesBetween(timing, now)]
	logging.info(dates)
	dates = ",".join(dates)
	timing_format = "'" + str(timing)[:19] + ".000000+0000" + "'"

	homes_rawdata = {}
	for home_id, sensors_ids in ids.items():
		homes_rawdata[home_id] = {}

		for sid in sensors_ids:
			where_clause = "sensor_id = {} and day IN ({}) AND ts > {}".format("'"+sid+"'", dates, timing_format)
			sensor_df = ptc.selectQuery(session, CASSANDRA_KEYSPACE, table_name, "*", where_clause, "ALLOW FILTERING", "")
		
			homes_rawdata[home_id][sid] = sensor_df

	return homes_rawdata


def getConsumptionProductionDF(sensors_config, homes_rawdata, ids):
	""" 
	compute power data from raw data (coming from cassandra 'raw' table) : 
	P_cons = P_tot - P_prod
	P_net = P_prod + P_cons
	"""
	homes_stats = {}
	for hid, home_sensors in sensors_config.groupby("home_id"):
		first_sid = list(homes_rawdata[hid].keys())[0]
		cons_prod_df = homes_rawdata[hid][first_sid][["sensor_id","day", "ts"]].copy()
		cons_prod_df = cons_prod_df.rename(columns={"sensor_id": "home_id"})
		cons_prod_df["home_id"] = hid  # replace 1st sensor_id by home_id
		cons_prod_df["P_cons"] = 0
		cons_prod_df["P_prod"] = 0
		cons_prod_df["P_tot"] = 0

		for sid in home_sensors.index:
			sensor_df = homes_rawdata[hid][sid]
			p = home_sensors.loc[sid]["pro"]
			n = home_sensors.loc[sid]["net"]
			# logging.info("{} : p: {}, n: {}".format(sid, p, n))

			cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * sensor_df["power"]
			cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * sensor_df["power"]

		cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

		cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals
		# logging.info(cons_prod_df.head(5))
		homes_stats[hid] = cons_prod_df

	return homes_stats


def concentrateConsProdDf(cons_prod_df):
	""" 
	transform : index=i, cols = [home_id day ts P_cons P_prod P_tot]
	into : index = [day, ts], cols = [P_cons P_prod P_tot] 
	"""
	df = cons_prod_df.set_index(['day', 'ts'])
	df = df.drop(['home_id'], axis=1)
	return df


def getGroupsPowers(home_powers, groups):
	"""
	home_stats format : {home_id : cons_prod_df}
	groups format : [[home_ID1, home_ID2], [home_ID3, home_ID4], ...]
	"""
	groups_powers = {}
	# logging.info(groups)
	for i, group in enumerate(groups):
		# logging.info(home_stats[group[0]].head(2))
		cons_prod_df = concentrateConsProdDf(copy.copy(home_powers[group[0]]))
		for j in range(1, len(group)):
			# logging.info(home_stats[group[j]].head(2))

			cons_prod_df = cons_prod_df.add(concentrateConsProdDf(home_powers[group[j]]), fill_value=0)
		
		groups_powers["group" + str(i + 1)] = cons_prod_df

		# logging.info(cons_prod_df.head(10))

	return groups_powers


def saveStatsToCassandra(session, homes_powers, table_name):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the raw data
	of some period of time in Cassandra
	- homes_powers : {hid: cons_prod_df}
	- cons_prod_df : home_id, day, ts, p_cons, p_prod, p_tot
	"""
	logging.info("saving in Cassandra : flukso.{} table...".format(table_name))

	insertion_time = str(pd.Timestamp.now())[:19] + "Z"
	for hid, cons_prod_df in homes_powers.items():
		logging.info(hid)
		
		col_names = list(cons_prod_df.columns)
		for _, row in cons_prod_df.iterrows():
			values = list(row)
			values[2] = str(values[2]) + "Z"  # timestamp (ts)
			values.append(insertion_time)
			logging.info(values)
			ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	logging.info("Successfully saved powers in Cassandra")


def saveGroupsStatsToCassandra(session, groups_powers, table_name):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the groups 
	of some period of time in Cassandra
	- groups_powers : {groupI: cons_prod_df}
	- cons_prod_df : index = [day, ts], cols = [P_cons P_prod P_tot]
	"""
	logging.info("saving in Cassandra : flukso.{} table...".format(table_name))

	insertion_time = str(pd.Timestamp.now())[:19] + "Z"
	for gid, cons_prod_df in groups_powers.items():
		logging.info(gid)
		
		col_names = ["home_id", "day", "ts", "P_cons", "P_prod", "P_tot"]
		for date, row in cons_prod_df.iterrows():
			values = [gid] + list(date) + list(row)  # date : ("date", "ts")
			values[2] = str(values[2]) + "Z"  # timestamp (ts)
			values.append(insertion_time)
			logging.info(values)
			ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	logging.info("Successfully saved groups powers in Cassandra")

# ====================================================================================

def main():
	cassandra_session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	sensors_config = getSensorsConfigCassandra(cassandra_session, TBL_SENSORS_CONFIG)
	home_ids = sensors_config.getIds()
	ids = getSensorsIds(sensors_config)

	# test raw data retrieval
	since = "s2022-03-02-16-18-08"
	# since = "3min"
	# TODO : refactor getRawData + getConsumptionProductionDF + getGroupsConfigCassandra with configs
	homes_rawdata = getRawData(cassandra_session, since, ids, TBL_RAW) 

	
	logging.info("==============================================")

	# powers computations (p_cons, p_prod, p_tot)
	homes_powers = getConsumptionProductionDF(sensors_config, homes_rawdata, ids)

	groups_config = getGroupsConfigCassandra(cassandra_session, TBL_GROUPS_CONFIG)
	# groups powers computations
	groups_powers = getGroupsPowers(homes_powers, groups_config)

	saveStatsToCassandra(cassandra_session, homes_powers, "power2")
	saveGroupsStatsToCassandra(cassandra_session, groups_powers, "groups_power2")
	


if __name__ == "__main__":
	main()