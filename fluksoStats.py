from tokenize import group
from turtle import home
from constants import *
import pyToCassandra as ptc

import copy
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
		data = ptc.selectQuery(session, CASSANDRA_KEYSPACE, "raw_data", "*", where_clause, "LIMIT 20")

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


def concentrateConsProdDf(cons_prod_df):
	""" 
	transform : index=i, cols = [home_id day ts P_cons P_prod P_tot]
	into : index = [day, ts], cols = [P_cons P_prod P_tot] 
	"""
	df = cons_prod_df.set_index(['day', 'ts'])
	df = df.drop(['home_id'], axis=1)
	return df


def getGroupsStats(home_stats, groups):
	"""
	home_stats format : {home_id : cons_prod_df}
	groups format : [[home_ID1, home_ID2], [home_ID3, home_ID4], ...]
	"""
	groups_stats = {}
	print(groups)
	for i, group in enumerate(groups):
		# print(home_stats[group[0]].head(2))
		cons_prod_df = concentrateConsProdDf(copy.copy(home_stats[group[0]]))
		for j in range(1, len(group)):
			# print(home_stats[group[j]].head(2))

			cons_prod_df = cons_prod_df.add(concentrateConsProdDf(home_stats[group[j]]), fill_value=0)
		
		groups_stats["group" + str(i + 1)] = cons_prod_df

		# print(cons_prod_df.head(10))

	return groups_stats


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

def saveGroupsStatsToCassandra(session, groups_stats):
	""" 
	Save the stats (P_cons, P_prod, P_tot) of the groups 
	of some period of time in Cassandra
	"""
	print("saving in Cassandra : flukso.groups_stats table...")

	for gid, cons_prod_df in groups_stats.items():
		print(gid)
		
		col_names = ["home_id", "day", "ts", "P_cons", "P_prod", "P_tot"]
		for date, row in cons_prod_df.iterrows():
			values = [gid] + list(date) + list(row)  # date : ("date", "ts")
			values[2] = str(values[2]) + "Z"  # timestamp (ts)
			print(values)
			ptc.insert(session, CASSANDRA_KEYSPACE, "groups_stats", col_names, values)

	print("Successfully saved groups stats in Cassandra")


def main():
	session = ptc.connectToCluster(CASSANDRA_KEYSPACE)
	sensors = read_sensor_info(UPDATED_SENSORS_FILE)
	home_ids = set(sensors.home_ID)

	# test raw data retrieval
	since = "s2022-01-28-10-01-00"
	# since = "3min"
	homes_rawdata = getRawData(session, since, home_ids) 

	print("==============================================")

	# stats computations
	homes_stats = getConsumptionProductionDF(sensors, homes_rawdata, home_ids)

	groups = getFLuksoGroups()
	# groups stats computations
	groups_stats = getGroupsStats(homes_stats, groups)

	saveStatsToCassandra(session, homes_stats)
	saveGroupsStatsToCassandra(session, groups_stats)
	


if __name__ == "__main__":
	main()