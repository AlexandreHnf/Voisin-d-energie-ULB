__title__ = "pyToCassandra"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# standard library
import json

# 3rd party packages
import pandas as pd
import logging

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy

# local source
from constants import (
	CASSANDRA_AUTH_MODE, 
	CASSANDRA_CREDENTIALS_FILE, 
	SERVER_BACKEND_IP
)


def getRightFormat(values):
	""" 
	Get the right string format given a list of values 
	used by 'insert'
	"""
	res = []
	for v in values:
		if type(v) == str:
			res.append("'" + v + "'")
		elif type(v) == list:
			# => ['v1', 'v2', 'v3', ... ]
			l = "["
			for vv in v:
				l += "'" + vv + "',"
			res.append(l[:-1] + "]")
		else:
			res.append(str(v))
	
	return res


def createKeyspace(session, keyspace_name, replication_class, replication_factor):
	""" 
	Create a new keyspace in the Cassandra database

	command : CREATE KEYSPACE <keyspace> WITH REPLICATION = 
				{'class': <replication_class>, 'replication_factor': <replication_factor>}
	"""
	# to create a new keyspace :
	keyspace_query = "CREATE KEYSPACE {} ".format(keyspace_name)
	keyspace_query += "WITH REPLICATION = "
	keyspace_query += "{'class' : {}, ".format(replication_class)
	keyspace_query += "'replication_factor': {}};".format(replication_factor)	

	session.execute(keyspace_query)

	# use the keyspace
	# session.execute("USE {}".format(keyspace_name))


def deleteRows(session, keyspace, table_name):
	""" 
	delete all rows of a table
	"""
	query = "TRUNCATE {}.{}".format(keyspace, table_name)
	session.execute(query)


def insert(session, keyspace, table, columns, values):
	""" 
	Insert a new row in the table

	command : INSERT INTO <keyspace>.<table> (<columns>) VALUES (<values>);
	"""
	
	query = "INSERT INTO {}".format(keyspace)
	query += ".{} ".format(table)
	query += "({}) ".format(",".join(columns))
	query += "VALUES ({});".format(",".join(getRightFormat(values)))

	# logging.info("===> insert query :" + query)
	session.execute(query)


def getInsertQuery(keyspace, table, columns, values):
	""" 
	Get the prepared statement for an insert query

	command : INSERT INTO <keyspace>.<table> (<columns>) VALUES (<values>);
	"""

	query = "INSERT INTO {}".format(keyspace)
	query += ".{} ".format(table)
	query += "({}) ".format(",".join(columns))
	query += "VALUES ({});".format(",".join(getRightFormat(values)))

	return query



def batch_insert(session, inserts):
	""" 
	Gets a string containing a series of Insert queries
	and use batch to execute them all at once
	condition : same partition keys for each insert query for higher performance
	"""
	query = "BEGIN BATCH " 
	query += inserts
	query += "APPLY BATCH;"

	session.execute(query)


def getCompoundKeyStr(primary_keys):
	""" 
	right string format for compound key
	"""
	return "(" + "".join(primary_keys) + ")"


def getClusteringKeyStr(clustering_keys):
	""" 
	right string format for clustering key
	"""
	if len(clustering_keys) == 0:
		return ""
	else:
		return "," + ",".join(clustering_keys)


def getOrdering(ordering):
	""" 
	ordering format : {"column_name": "ASC", "column_name2": "DESC"}
	"""
	res = ""
	if len(ordering) > 0:
		res += "WITH CLUSTERING ORDER BY ("
		for col_name, ordering_type in ordering.items():
			res += col_name + " " + ordering_type + ","
		res = res[:-1] + ")"  # replace last "," by ")"

	return res


def createTable(session, keyspace, table_name, columns, primary_keys, clustering_keys, ordering):
	""" 
	Create a new table in the database 
	columns = [column name type, ...]

	command : CREATE TABLE IF NOT EXISTS <keyspace>.<table_name> 
				(<columns>, PRIMARY KEY (<primary keys><clustering keys>)) <ordering>;
	"""
	# create a new table : 

	query = "CREATE TABLE IF NOT EXISTS {}".format(keyspace)
	query += ".{} ".format(table_name)
	query += "({}, ".format(",".join(columns))
	query += "PRIMARY KEY ({}".format(getCompoundKeyStr(primary_keys))
	query += "{})) ".format(getClusteringKeyStr(clustering_keys))
	query += "{};".format(getOrdering(ordering))

	# logging.info("===>  create table query : " + query)
	session.execute(query) 
	# logging.info("successfully created table " + table_name)


def pandas_factory(colnames, rows):
	""" 
	used by 'selectResToDf'
	"""
	return pd.DataFrame(rows, columns=colnames)


def convertColumnsTimezones(df):
	""" 
	Given a queried dataframe from Cassandra, convert
	the timezone of columns containing timestamps

	We assume the timestamps in Cassandra tables are stored with
	UTC timezone
	""" 

	ts_columns = [ 
		'ts',
		'config_id', 
		'insertion_time', 
		'start_ts', 
		'end_ts'
	]
	for col_name in ts_columns:
		if col_name in df.columns:
			# first convert to UTC beause UTC saved timestamps in Cassandra comes up
			# with no timezone. Ex: stored ts : 'YYYY-MM-DD HH:MM:SS.MMM000+0000'
			# becomes 'YYYY-MM-DD HH:MM:SS.MMM' when queried.
			df[col_name] = df[col_name].dt.tz_localize("UTC").dt.tz_convert('UTC')
			df[col_name] = df[col_name].dt.tz_convert('CET')


def selectResToDf(session, query):
	""" 
	process a select query and returns a pandas DataFrame
	with the result of the query 
	"""

	session.row_factory = pandas_factory
	session.default_fetch_size = None

	rslt = session.execute(query, timeout=None)
	df = rslt._current_rows

	return df


def selectQuery(
		session, 
		keyspace, 
		table_name, 
		columns, 
		where_clause, 
		limit				= None,
		allow_filtering 	= True,
		distinct 			= False,
		tz 					= 'CET'
	):
	""" 
	columns = * or a list of columns

	command : SELECT <distinct> <columns> FROM <keyspace>.<table_name> 
				WHERE <where_clause> <LIMIT> <ALLOW FILTERING>;
	"""

	where = ""
	if len(where_clause) > 0:
		where = "WHERE"
	distinct = "DISTINCT" if distinct else ""
	limit = "LIMIT {}".format(limit) if limit is not None else ""
	allow_filtering = "ALLOW FILTERING" if allow_filtering else ""
	
	query = "SELECT {} ".format(distinct)
	query += "{} ".format(",".join(columns))
	query += "FROM {}".format(keyspace)
	query += ".{} ".format(table_name)
	query += "{} ".format(where)
	query += "{} ".format(where_clause)
	query += "{} ".format(limit)
	query += "{};".format(allow_filtering)

	# logging.info("===> select query : " + query)
	res_df = selectResToDf(session, query)
	if tz == "CET" and len(res_df) > 0:
		convertColumnsTimezones(res_df)

	return res_df
	

# ==========================================================================


def connectToCluster(keyspace):
	""" 
	connect to Cassandra Cluster
	- either locally : simple, ip = 127.0.0.1:9042, by default
	- or with username and password : using AuthProvider
	"""
	try:
		if CASSANDRA_AUTH_MODE == "local":
			# create the cluster : connects to localhost (127.0.0.1:9042) by default
			cluster = Cluster(
				load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1'),
				protocol_version=3
			)
		
		elif CASSANDRA_AUTH_MODE == "server":
			with open(CASSANDRA_CREDENTIALS_FILE) as json_file:
				cred = json.load(json_file)
				auth_provider = PlainTextAuthProvider(
					username=cred["username"], 
					password=cred["password"]
				)
				cluster = Cluster(
					[SERVER_BACKEND_IP], 
					port=9042, 
					auth_provider=auth_provider
				)

		# connect to the keyspace or create one if it doesn't exist
		session = cluster.connect(keyspace)
	except:
		logging.critical("Exception occured in 'connectToCluster' cassandra: ", exc_info=True)
	
	return session


def main():
	session = connectToCluster("test")


if __name__ == "__main__":
	main()