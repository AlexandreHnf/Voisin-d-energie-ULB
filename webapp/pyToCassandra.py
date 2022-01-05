from cassandra.cluster import Cluster


def getRightFormat(values):
    res = []
    for v in values:
        if type(v) == str:
            res.append("'" + v + "'")
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


def insert(session, keyspace, table, columns, values):
    """ 
    Insert a new row in the table
    """
    query = "INSERT INTO {}.{} ({}) VALUES ({});" \
                    .format(keyspace, table, ",".join(columns), ",".join(getRightFormat(values)))
    # print("===> insert query :", query)
    session.execute(query)


def getCompoundKeyStr(primary_keys):
    return "(" + "".join(primary_keys) + ")"


def getClusteringKeyStr(clustering_keys):
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


def createTable(session, keyspace, table, columns, primary_keys, clustering_keys, ordering):
    """ 
    Create a new table in the database 
    columns = [column name type, ...]
    """
    # create a new table : 
    query = "CREATE TABLE IF NOT EXISTS {}.{} \
                    ({}, PRIMARY KEY ({}{})) {};" \
                    .format(keyspace, table, ",".join(columns), 
                    getCompoundKeyStr(primary_keys),
                    getClusteringKeyStr(clustering_keys),
                    getOrdering(ordering))

    print("===>  create table query : ", query)
    session.execute(query) 


# ==========================================================================


def connectToCluster(keyspace):
    # create the cluster : connects to localhost (127.0.0.1:9042) by default
    cluster = Cluster()

    # connect to the keyspace or create one if it doesn't exist
    session = cluster.connect(keyspace)

    return session


def testFruitStock(session):
    # create a new table : 
    createTable(session, "test", "fruit_stock", 
                ["item_id TEXT", "name TEXT", "price_p_item DECIMAl"], 
                ["item_id"], ["name"],
                {"name": "DESC"})
    # session.execute("CREATE TABLE IF NOT EXISTS test.fruit_stock \
    #                 (item_id TEXT, name TEXT, price_p_item DECIMAL, PRIMARY KEY (item_id));")

    # insert new row
    insert(session, "test", "fruit_stock", ["item_id", "name", "price_p_item"], ["a0", "apples", 0.50])
    insert(session, "test", "fruit_stock", ["item_id", "name", "price_p_item"], ["b1", "bananas", 0.40])
    insert(session, "test", "fruit_stock", ["item_id", "name", "price_p_item"], ["c3", "oranges", 0.35])
    insert(session, "test", "fruit_stock", ["item_id", "name", "price_p_item"], ["d4", "pineapples", 2.5])
    insert(session, "test", "fruit_stock", ["item_id", "name", "price_p_item"], ["e5", "grapes", 0.44])
    # session.execute("INSERT INTO test.fruit_stock (item_id, name, price_p_item) \
    #                 VALUES ('a0','apples',0.50);")


def main():
    session = connectToCluster("test")

    # createKeyspace(session, "test", "SimpleStrategy", "1")

    testFruitStock(session)

    print("Insertion done.")


if __name__ == "__main__":
    main()