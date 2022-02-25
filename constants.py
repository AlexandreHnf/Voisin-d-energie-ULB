""" 
Constants
"""

# files paths
FLUKSO_TECHNICAL_FILE =                 "sensors/FluksoTechnical.xlsx"
UPDATED_FLUKSO_TECHNICAL_FILE =         "sensors/updated_sensors.csv"
COMPACT_SENSOR_FILE =                   "sensors/sensors_technical.csv"
PHASE_TO_MODIF_FILE =                   "sensors/phases_to_modify.txt"
SENSOR_FILE =                           "sensors/sensors.csv"
UPDATED_SENSORS_FILE =                  "sensors/updated_sensors.csv"

OUTPUT_FILE =                           'output/fluksoData/'

GROUPS_FILE =                           "sensors/grouped_homes_sensors.txt"

IDS_FILE =                              "sensors/ids.json"
GIDS_FILE =                             "sensors/grp_ids.json"



# sample frequency
FREQ = [8, "S"]  # 8 sec.

# cassandra keyspaces
CASSANDRA_KEYSPACE = "flukso" 

# missing raw data time limit
LIMIT_TIMING_RAW = 2  # days

# nb of days limit when getting the last timestamp of raw table
LAST_TS_DAYS = 2  # days
