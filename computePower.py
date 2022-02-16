from tokenize import group
from turtle import home
from constants import *
import pyToCassandra as ptc
from utils import *

import copy
import os
import sys
import pandas as pd
from datetime import date, timedelta


# ====================================================================================
# called by syncRawFluksoData.py just after writing raw data to cassandra
# ====================================================================================

def savePowerDataToCassandra(homes, table_name):
	""" 
	save power flukso data to cassandra : P_cons, P_prod, P_tot
	- homes : Home objects 
		=> contains cons_prod_df : timestamp, P_cons, P_prod, P_tot
	"""
	print("saving power data to Cassandra...")
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)

	for hid, home in homes.items():
		print(hid)
		cons_prod_df = home.getConsProdDF()

		col_names = ["home_id", "day", "ts", "p_cons", "p_prod", "p_tot"]
		for timestamp, row in cons_prod_df.iterrows():
			day = str(timestamp.date())
			# save timestamp with CET local timezone, format : YY-MM-DD H:M:SZ
			ts = str(timestamp)[:19] + "Z"
			values = [hid, day, ts] + list(row)
			ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)
	
	print("Successfully saved power data in cassandra : table {}".format(table_name))


# ====================================================================================
# called manually whenever we want to modify the power table with given time interval
# ====================================================================================


def getRawData(session, since, ids, table_name):
	""" 
	get raw flukso data from cassandra since a certain amount of time
	TODO : liste des dates en fonction du date_range 
	-> pandas.date_range(sdate,edate-timedelta(days=1),freq='d')
	"""
	now = pd.Timestamp.now(tz="CET")
	timing = setInitSeconds(getTiming(since, now))

	print("timing date : ", str(timing.date()))
	print("now date : ", str(now.date()))
	dates = ["'" + d + "'" for d in getDatesBetween(timing, now)]
	print(dates)
	dates = ",".join(dates)
	timing_format = "'" + str(timing)[:19] + ".000000+0000" + "'"

	homes_rawdata = {}
	for home_id, sensors_ids in ids.items():
		homes_rawdata[home_id] = {}

		for sid in sensors_ids:
			where_clause = "sensor_id = {} and day IN ({}) AND ts > {}".format("'"+sid+"'", dates, timing_format)
			sensor_df = ptc.selectQuery(session, CASSANDRA_KEYSPACE, table_name, "*", where_clause, "")
		
			homes_rawdata[home_id][sid] = sensor_df

	return homes_rawdata


def getConsumptionProductionDF(sensors, homes_rawdata, ids):
	""" 
	compute power data from raw data (coming from cassandra 'raw' table) : 
	P_cons = P_tot - P_prod
	P_net = P_prod + P_cons
	"""
	homes_stats = {}
	for home_id in ids:
		# print("================> ", home_id)
		home_sensors = sensors.loc[sensors["home_ID"] == home_id]
		first_sid = list(homes_rawdata[home_id].keys())[0]
		cons_prod_df = homes_rawdata[home_id][first_sid][["sensor_id","day", "ts"]].copy()
		cons_prod_df = cons_prod_df.rename(columns={"sensor_id": "home_id"})
		cons_prod_df["home_id"] = home_id  # replace 1st sensor_id by home_id
		cons_prod_df["P_cons"] = 0
		cons_prod_df["P_prod"] = 0
		cons_prod_df["P_tot"] = 0

		for sid in ids[home_id]:
			sensor_df = homes_rawdata[home_id][sid]
			p = home_sensors.loc[home_sensors['sensor_id'] == sid, 'pro'].iloc[0]
			n = home_sensors.loc[home_sensors['sensor_id'] == sid, 'net'].iloc[0]
			# print("{} : p: {}, n: {}".format(sid, p, n))

			cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * sensor_df["power"]
			cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * sensor_df["power"]

		cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

		cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals
		# print(cons_prod_df.head(5))
		homes_stats[home_id] = cons_prod_df

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
	print(groups)
	for i, group in enumerate(groups):
		# print(home_stats[group[0]].head(2))
		cons_prod_df = concentrateConsProdDf(copy.copy(home_powers[group[0]]))
		for j in range(1, len(group)):
			# print(home_stats[group[j]].head(2))

			cons_prod_df = cons_prod_df.add(concentrateConsProdDf(home_powers[group[j]]), fill_value=0)
		
		groups_powers["group" + str(i + 1)] = cons_prod_df

		# print(cons_prod_df.head(10))

	return groups_powers


def saveStatsToCassandra(session, homes_powers, table_name):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the raw data
	of some period of time in Cassandra
	- homes_powers : {hid: cons_prod_df}
	- cons_prod_df : home_id, day, ts, p_cons, p_prod, p_tot
	"""
	print("saving in Cassandra : flukso.{} table...".format(table_name))

	for hid, cons_prod_df in homes_powers.items():
		print(hid)
		
		col_names = list(cons_prod_df.columns)
		for _, row in cons_prod_df.iterrows():
			values = list(row)
			values[2] = str(values[2]) + "Z"  # timestamp (ts)
			print(values)
			ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	print("Successfully saved powers in Cassandra")


def saveGroupsStatsToCassandra(session, groups_powers, table_name):
	""" 
	Save the powers (P_cons, P_prod, P_tot) of the groups 
	of some period of time in Cassandra
	- groups_powers : {groupI: cons_prod_df}
	- cons_prod_df : index = [day, ts], cols = [P_cons P_prod P_tot]
	"""
	print("saving in Cassandra : flukso.{} table...".format(table_name))

	for gid, cons_prod_df in groups_powers.items():
		print(gid)
		
		col_names = ["home_id", "day", "ts", "P_cons", "P_prod", "P_tot"]
		for date, row in cons_prod_df.iterrows():
			values = [gid] + list(date) + list(row)  # date : ("date", "ts")
			values[2] = str(values[2]) + "Z"  # timestamp (ts)
			print(values)
			ptc.insert(session, CASSANDRA_KEYSPACE, table_name, col_names, values)

	print("Successfully saved groups powers in Cassandra")

# ====================================================================================

def main():
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	sensors = read_sensor_info("", UPDATED_SENSORS_FILE)
	home_ids = set(sensors.home_ID)
	ids = getSensorsIds(sensors)

	# test raw data retrieval
	since = "s2022-02-14-16-10-00"
	# since = "3min"
	homes_rawdata = getRawData(session, since, ids, "raw") 

	
	print("==============================================")

	# powers computations (p_cons, p_prod, p_tot)
	homes_powers = getConsumptionProductionDF(sensors, homes_rawdata, ids)

	groups = getFLuksoGroups()
	# groups powers computations
	groups_powers = getGroupsPowers(homes_powers, groups)

	"""
	saveStatsToCassandra(session, homes_powers, "stats")
	saveGroupsStatsToCassandra(session, groups_powers, "groups_stats")
	"""	


if __name__ == "__main__":
	main()