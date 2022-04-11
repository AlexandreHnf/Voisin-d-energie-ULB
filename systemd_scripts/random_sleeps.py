import time 
import random 
import pandas as pd
from cassandra.cluster import Cluster



print("before sleep.. - ", pd.Timestamp.now())

# t = random.randint(1, 6)
t = 2
print("sleep({})".format(t))

# time.sleep(t)

print("after sleep.. - ", pd.Timestamp.now())
