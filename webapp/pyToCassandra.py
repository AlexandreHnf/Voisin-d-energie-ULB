from cassandra.cluster import Cluster


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
                    .format(keyspace, table, ",".join(columns), ",".join(values))
    print("===> insert query :", query)
    session.execute(query)


def createTable(session, keyspace, table, columns, primary_key):
    """ 
    Create a new table in the database 
    columns = [column name type, ...]
    """
    # create a new table : 
    query = "CREATE TABLE IF NOT EXISTS {}.{} \
                    ({}, PRIMARY KEY ({}));" \
                    .format(keyspace, table, ",".join(columns), primary_key)

    print("===>  create table query : ", query)
    session.execute(query) 


# ==========================================================================


def main():
    # create the cluster : connects to localhost (127.0.0.1:9042) by default
    cluster = Cluster()

    # connect to the keyspace or create one if it doesn't exist
    session = cluster.connect('test')

    # createKeyspace(session, "test", "SimpleStrategy", "1")

    # create a new table : 
    createTable(session, "test", "fruit_stock", ["item_id TEXT", "name TEXT", "price_p_item DECIMAl"], "item_id")
    # session.execute("CREATE TABLE IF NOT EXISTS test.fruit_stock \
    #                 (item_id TEXT, name TEXT, price_p_item DECIMAL, PRIMARY KEY (item_id));")

    # insert new row
    insert(session, "test", "fruit_stock", ["item_id", "name", "price_p_item"], ["'a0'", "'apples'", '0.50'])
    # session.execute("INSERT INTO test.fruit_stock (item_id, name, price_p_item) \
    #                 VALUES ('a0','apples',0.50);")

    print("Insertion done.")


if __name__ == "__main__":
    main()