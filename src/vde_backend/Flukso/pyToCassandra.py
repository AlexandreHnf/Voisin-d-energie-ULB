__title__ = "pyToCassandra"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
from constants import * 
import json

import logging


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
	"""
	# to create a new keyspace : 
	keyspace_query = "CREATE KEYSPACE {} WITH REPLICATION = \
					{'class' : {}, 'replication_factor': {}};"\
					.format(keyspace_name, replication_class, replication_factor)

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
	"""
	query = "INSERT INTO {}.{} ({}) VALUES ({});" \
					.format(keyspace, table, ",".join(columns), ",".join(getRightFormat(values)))
	# logging.info("===> insert query :" + query)
	session.execute(query)


def getInsertQuery(keyspace, table, columns, values):
	""" 
	Get the prepared statement for an insert query
	"""
	return "INSERT INTO {}.{} ({}) VALUES ({});" \
					.format(keyspace, table, ",".join(columns), ",".join(getRightFormat(values)))


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
	"""
	# create a new table : 
	query = "CREATE TABLE IF NOT EXISTS {}.{} \
					({}, PRIMARY KEY ({}{})) {};" \
					.format(keyspace, table_name, ",".join(columns), 
					getCompoundKeyStr(primary_keys),
					getClusteringKeyStr(clustering_keys),
					getOrdering(ordering))

	logging.info("===>  create table query : " + query)
	session.execute(query) 
	logging.info("successfully created table " + table_name)


def pandas_factory(colnames, rows):
	""" 
	used by 'selectResToDf'
	"""
	return pd.DataFrame(rows, columns=colnames)

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


def selectQuery(session, keyspace, table, columns, where_clause, allow_filtering, limit, distinct=""):
	""" 
	columns = * or a list of columns
	SELECT <> FROM <> WHERE <> ... ALLOW FILTERING
	"""

	where = ""
	if len(where_clause) > 0:
		where = "WHERE"
	
	query = "SELECT {} {} FROM {}.{} {} {} {} {};".format(
		distinct, ",".join(columns), keyspace, table, where, where_clause, limit, allow_filtering
	)

	# logging.info("===> select query : " + query)
	rows = selectResToDf(session, query)

	return rows
	

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
			cluster = Cluster()
		
		elif CASSANDRA_AUTH_MODE == "server":
			with open(CASSANDRA_CREDENTIALS_FILE) as json_file:
				cred = json.load(json_file)
				auth_provider = PlainTextAuthProvider(
					username=cred["username"], 
					password=cred["password"]
				)
				cluster = Cluster([SERVER_BACKEND_IP], port=9042, auth_provider=auth_provider)

		# connect to the keyspace or create one if it doesn't exist
		session = cluster.connect(keyspace)
	except:
		logging.critical("Exception occured in 'connectToCluster' cassandra: ", exc_info=True)
	
	return session


def main():
	session = connectToCluster("test")

	# createKeyspace(session, "test", "SimpleStrategy", "1")

	logging.info("Insertion done.")


if __name__ == "__main__":
	main()