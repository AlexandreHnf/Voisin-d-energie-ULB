from cassandra.cluster import cluster

cluster = cluster()

session = cluster.connect('test_keyspace')

session.execute("INSERT INTO table_name (id ...) VALUES (uuid(), ...)")

print("Insertion done.")


