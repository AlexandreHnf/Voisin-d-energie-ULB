__title__ = "py_to_cassandra"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, Guillaume Levasseur, and Brice Petit"
__license__ = "MIT"


# 3rd party packages
import cassandra
import cassandra.auth
import cassandra.cluster
import cassandra.policies
import json
import logging
import os.path
import pandas as pd

# local source
from constants import (
    CASSANDRA_CREDENTIALS_FILE,
    SERVER_BACKEND_IP,
    CASSANDRA_REPLICATION_STRATEGY,
    CASSANDRA_REPLICATION_FACTOR,
    CASSANDRA_KEYSPACE
)


def load_json_credentials(path: str):
    cred = {}
    if os.path.exists(path):
        with open(path) as json_file:
            cred = json.load(json_file)

    return cred


# ==========================================================================


def create_keyspace(session, keyspace_name):
    """
    Create a new keyspace in the Cassandra database

    command : CREATE KEYSPACE <keyspace> WITH REPLICATION =
                {'class': <replication_class>, 'replication_factor': <replication_factor>}
    """
    # to create a new keyspace :
    keyspace_query = (
        "CREATE KEYSPACE {} WITH REPLICATION = {{'class' : '{}', 'replication_factor': {}}};"
    )
    keyspace_query = keyspace_query.format(
        keyspace_name,
        CASSANDRA_REPLICATION_STRATEGY,
        CASSANDRA_REPLICATION_FACTOR,
    )
    logging.debug(keyspace_query)
    session.execute(keyspace_query)


def connect_to_cluster(keyspace):
    """
    connect to Cassandra Cluster
    - either locally : simple, ip = 127.0.0.1:9042, by default
    - or with username and password using AuthProvider, if the credentials file
      exits
    """
    lbp = cassandra.policies.DCAwareRoundRobinPolicy(local_dc='datacenter1')
    auth_provider = None
    try:
        cred = load_json_credentials(CASSANDRA_CREDENTIALS_FILE)
        if len(cred):
            auth_provider = cassandra.auth.PlainTextAuthProvider(
                username=cred["username"],
                password=cred["password"]
            )

        cluster = cassandra.cluster.Cluster(
            contact_points=[SERVER_BACKEND_IP, '127.0.0.1', ],
            port=9042,
            load_balancing_policy=lbp,
            protocol_version=4,
            auth_provider=auth_provider,
        )

        # connect to the keyspace
        session = cluster.connect()
        session.set_keyspace(keyspace)
    except cassandra.InvalidRequest:
        # Create the keyspace if it does not exist.
        create_keyspace(session, keyspace)
        session.set_keyspace(keyspace)
    except Exception:
        logging.critical("Exception occured in 'connect_to_cluster' cassandra: ", exc_info=True)
        exit(57)

    return session


SESSION = connect_to_cluster(CASSANDRA_KEYSPACE)


def get_right_format(values):
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
            _list = "["
            for vv in v:
                _list += "'" + vv + "',"
            res.append(_list[:-1] + "]")
        elif "isoformat" in dir(v):
            res.append("'" + v.isoformat() + "'")
        else:
            res.append(str(v))

    return res


def delete_rows(keyspace, table_name):
    """
    delete all rows of a table
    """
    query = "TRUNCATE {}.{}".format(keyspace, table_name)
    SESSION.execute(query)


def insert(keyspace, table, columns, values):
    """
    Insert a new row in the table

    command : INSERT INTO <keyspace>.<table> (<columns>) VALUES (<values>);
    """

    query = "INSERT INTO {}".format(keyspace)
    query += ".{} ".format(table)
    query += "({}) ".format(",".join(columns))
    query += "VALUES ({});".format(",".join(get_right_format(values)))
    logging.debug("===> insert query :" + query)
    SESSION.execute(query)


def get_insert_query(keyspace, table, columns, values):
    """
    Get the prepared statement for an insert query

    command : INSERT INTO <keyspace>.<table> (<columns>) VALUES (<values>);
    """

    query = "INSERT INTO {}".format(keyspace)
    query += ".{} ".format(table)
    query += "({}) ".format(",".join(columns))
    query += "VALUES ({});".format(",".join(get_right_format(values)))

    return query


def batch_insert(inserts):
    """
    Gets a string containing a series of Insert queries
    and use batch to execute them all at once
    condition : same partition keys for each insert query for higher performance
    """
    query = "BEGIN BATCH "
    query += inserts
    query += "APPLY BATCH;"

    SESSION.execute(query)


def get_ordering(ordering):
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


def create_table(keyspace, table_name, columns, primary_keys, clustering_keys, ordering):
    """
    Create a new table in the database
    columns = [column name type, ...]

    command : CREATE TABLE IF NOT EXISTS <keyspace>.<table_name>
                (<columns>, PRIMARY KEY (<primary keys><clustering keys>)) <ordering>;
    """

    query = "CREATE TABLE IF NOT EXISTS {}".format(keyspace)
    query += ".{} ".format(table_name)
    query += "({}, ".format(",".join(columns))
    query += "PRIMARY KEY (({})".format(','.join(primary_keys))
    query += "{})) ".format(',' + ','.join(clustering_keys) if len(clustering_keys) else '')
    query += "{};".format(get_ordering(ordering))

    SESSION.execute(query)
    logging.debug("===> create table query : " + query)


def pandas_factory(colnames, rows):
    """
    used by 'select_res_to_df'
    """
    return pd.DataFrame(rows, columns=colnames)


def convert_columns_timezones(df, tz):
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
            df[col_name] = df[col_name].dt.tz_localize("UTC").dt.tz_convert(tz)


def select_res_to_df(query):
    """
    process a select query and returns a pandas DataFrame
    with the result of the query
    """

    SESSION.row_factory = pandas_factory
    SESSION.default_fetch_size = None

    rslt = SESSION.execute(query, timeout=None)
    df = rslt._current_rows

    return df


def select_query(
        keyspace,
        table_name,
        columns,
        where_clause,
        limit=None,
        allow_filtering=True,
        distinct=False,
        tz='CET'
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

    logging.debug("===> select query : " + query)
    res_df = select_res_to_df(query)
    if len(res_df) > 0:
        # remark: the date column in tables is in CET timezone
        convert_columns_timezones(res_df, tz)

    return res_df


def groupby_query(
        keyspace,
        table_name,
        column,
        groupby_operator,
        groupby_cols,
        limit=None,
        allow_filtering=True,
        tz='CET'
):
    """
    column = a single column to apply the operator on.
    groupby_cols = list of columns by which data is grouped.
    command : SELECT <groupby_operator> <columns> FROM <keyspace>.<table_name>
                GROUP BY <groupby_cols> <LIMIT> <ALLOW FILTERING>;
    """

    if '*' in column or ',' in column:
        raise ValueError("Group by only supports one column to compute the operator.")

    limit = "LIMIT {}".format(limit) if limit is not None else ""
    allow_filtering = "ALLOW FILTERING" if allow_filtering else ""
    query = "SELECT {}({}) FROM {}.{} GROUP BY {} {} {};".format(
        groupby_operator,
        column,
        keyspace,
        table_name,
        ','.join(groupby_cols),
        limit,
        allow_filtering,
    )
    logging.debug("===> groupby query : " + query)
    res_df = select_res_to_df(query)
    if len(res_df) > 0:
        # remark: the date column in tables is in CET timezone
        convert_columns_timezones(res_df, tz)

    return res_df


def exist_table(keyspace, table_name):
    """
    Check if a table exists in the cluster given a certain keyspace
    """
    query = "SELECT table_name from system_schema.tables "
    query += "where keyspace_name = '{}' ".format(keyspace)
    query += "and table_name = '{}' ".format(table_name)
    query += "ALLOW FILTERING;"

    res_df = select_res_to_df(query)

    return len(res_df) > 0
