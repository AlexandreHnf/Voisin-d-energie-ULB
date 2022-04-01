""" 
Constants
"""

# files paths
FLUKSO_TECHNICAL_FILE =                 "sensors/FluksoConfigurations/FluksoTechnical.xlsx"
# FLUKSO_TECHNICAL_FILE =                 "sensors/FluksoConfigurations/FluksoTechnical_config1.xlsx"
UPDATED_FLUKSO_TECHNICAL_FILE =         "sensors/updated_sensors.csv"
COMPACT_SENSOR_FILE =                   "sensors/sensors_technical.csv"
PHASE_TO_MODIF_FILE =                   "sensors/phases_to_modify.txt"
# SENSOR_FILE =                           "sensors/sensors.csv"
UPDATED_SENSORS_FILE =                  "sensors/updated_sensors.csv"

OUTPUT_FILE =                           'output/fluksoData/'

GROUPS_FILE =                           "sensors/grouped_homes_sensors.txt"

IDS_FILE =                              "sensors/ids.json"
GIDS_FILE =                             "sensors/grp_ids.json"



# sample frequency
FREQ =                                  [8, "S"]  # 8 sec.

# missing raw data time limit = keep data from max X time back from now
LIMIT_TIMING_RAW =                      2  # days

# nb of days limit when getting the last timestamp of raw table
LAST_TS_DAYS =                          2  # days

# "since" default value for the earliest timestamp of a flukso
# normally since = 0, but for testing, we put since = 5min 
SINCE_INIT = "5min"


SERVER_FRONTEND_IP = 					'iridia-vde-frontend.hpda.ulb.ac.be'
SERVER_BACKEND_IP = 					'iridia-vde-db.hpda.ulb.ac.be'

# cassandra authentication
CASSANDRA_SERV_USERNAME = 				'cassandra'
CASSANDRA_SERV_PASSWORD = 				'5H54G5Sb/Pe0kT54uoj+cfiYDgLiLOGZ'

# cassandra keyspaces
CASSANDRA_KEYSPACE =                    "flukso" 

# cassandra tables names
TBL_SENSORS_CONFIG =                    "sensors_config"
TBL_GROUPS_CONFIG =                     "groups_config"
TBL_RAW =                               "raw"
TBL_RAW_MISSING =                       "raw_missing"
TBL_POWER =                             "power"
TBL_GROUPS_POWER =                      "groups_power"


INSERTS_PER_BATCH =                     11000

FROM_FIRST_TS =                         "4min"  # TEMPORARY