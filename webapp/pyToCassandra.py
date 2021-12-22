from cassandra.cluster import Cluster

# create the cluster : connects to localhost (127.0.0.1:9042) by default
cluster = Cluster()

# connect to the keyspace or create one if it doesn't exist
session = cluster.connect('test_keyspace')

# to create a new keyspace : 
# keyspace_query = "CREATE KEYSPACE test_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};"
# session.execute(keyspace_query)

# create a new table : 
session.execute("CREATE TABLE IF NOT EXISTS test_keyspace.fruit_stock \
                (item_id TEXT, name TEXT, price_p_item DECIMAL, PRIMARY KEY (item_id));")

# insert new row
session.execute("INSERT INTO test_keyspace.fruit_stock (item_id, name, price_p_item) \
                VALUES ('a0','apples',0.50);")

print("Insertion done.")


