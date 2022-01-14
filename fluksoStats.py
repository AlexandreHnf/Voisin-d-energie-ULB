from turtle import home
from constants import *
import pyToCassandra as ptc

import os
import sys
import pandas as pd

# ====================================================================================


def read_sensor_info(sensor_file):
	"""
	read csv file of sensors data
	"""
	sensor_file
	sensors = pd.read_csv(sensor_file, header=0, index_col=1)
	return sensors


def getTiming(t, now):
	"""
	get the timestamp of the "since"
	ex : the timestamp 20 min ago
	"""
	print("since {}".format(t))
	timing = 0
	if t:
		if t[0] == "s":
			e = t[1:].split("-")
			timing = pd.Timestamp(year=int(e[0]), month=int(e[1]), day=int(e[2]),
								  hour=int(e[3]), minute=int(e[4]), tz="CET")
		else:
			print("time delta : ", pd.Timedelta(t))
			print("now : ", now)
			timing = now - pd.Timedelta(t)

	print("timing : ", timing)

	return timing


def getRawData(session, since, home_ids):
	""" 
	get raw flukso data from cassandra since a certain amount of time
	"""
	now = pd.Timestamp.now(tz="CET")
	timing = getTiming(since, now)

	print("timing date : ", str(timing.date()))
	print("now date : ", str(now.date()))
	dates = list(set(["'"+str(timing.date())+"'", "'"+str(now.date())+"'"]))
	print(dates)
	dates = ",".join(dates)
	timing_format = "'" + str(timing)[:19] + ".000000+0000" + "'"

	homes_rawdata = {}
	for home_id in home_ids:
		
		where_clause = "home_id = {} and day IN ({}) AND ts > {}".format("'"+home_id+"'", dates, timing_format)
		data = ptc.selectQuery(session, CASSANDRA_KEYSPACE, "raw_data", "*", where_clause, "LIMIT 10")

		# print(home_id)
		# print(data)

		homes_rawdata[home_id] = data

	return homes_rawdata
		

def getConsumptionProductionDF(sensors, homes_rawdata, home_ids):
		""" 
		P_cons = P_tot - P_prod
		P_net = P_prod + P_cons
		"""
		homes_stats = {}
		for home_id in home_ids:
			power_df = homes_rawdata[home_id]
			home_sensors = sensors.loc[sensors["home_ID"] == home_id]
			cons_prod_df = power_df[["home_id","day", "ts"]].copy()
			cons_prod_df["P_cons"] = 0
			cons_prod_df["P_prod"] = 0
			cons_prod_df["P_tot"] = 0

			for i, phase in enumerate(home_sensors.index):
				phase_i = "phase" + str(i+1)
				p = home_sensors.loc[phase]["pro"]
				n = home_sensors.loc[phase]["net"]

				cons_prod_df["P_prod"] = cons_prod_df["P_prod"] + p * power_df[phase_i]
				cons_prod_df["P_tot"] = cons_prod_df["P_tot"] + n * power_df[phase_i]

			cons_prod_df["P_cons"] = cons_prod_df["P_tot"] - cons_prod_df["P_prod"]

			cons_prod_df = cons_prod_df.round(1)  # round all column values with 2 decimals
			homes_stats[home_id] = cons_prod_df

			# print(home_id)
			# print(cons_prod_df)

		return homes_stats


def saveStatsToCassandra(session, homes_stats):
	""" 
	Save the stats (P_cons, P_prod, P_tot) of the raw data
	of some period of time in Cassandra
	"""
	print("saving in Cassandra : flukso.stats table...")

	for hid, cons_prod_df in homes_stats.items():
		print(hid)
		
		col_names = list(cons_prod_df.columns)
		for _, row in cons_prod_df.iterrows():
			values = list(row)
			values[2] = str(values[2]) + "Z"  # timestamp (ts)
			print(values)
			ptc.insert(session, CASSANDRA_KEYSPACE, "stats", col_names, values)

	print("Successfully saved stats in Cassandra")


def main():
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	sensors = read_sensor_info(UPDATED_SENSORS_FILE)
	home_ids = set(sensors.home_ID)

	# test raw data retrieval
	since = "s2022-01-14-15-20-00"
	# since = "3min"
	homes_rawdata = getRawData(session, since, home_ids) 

	print("==============================================")

	# test stats computations
	homes_stats = getConsumptionProductionDF(sensors, homes_rawdata, home_ids)

	saveStatsToCassandra(session, homes_stats)


if __name__ == "__main__":
	main()