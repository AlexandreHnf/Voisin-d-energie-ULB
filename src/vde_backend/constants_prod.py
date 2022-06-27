__title__ = "Constants"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# =========================== FILE PATHS ====================================
# configuration path
FLUKSO_CONFIG_FILE =                 	"/opt/vde/flukso_config/Configuration.xlsx"

# local
OUTPUT_FILE =                           'local_scripts/output/fluksoData/'
GROUPS_FILE =                           "local_scripts/sensors/grouped_homes_sensors.txt"

TMPO_FILE = 							"/opt/vde/"

LOG_FILE = 								"/var/log/vde/logs.log"

# credentials
CASSANDRA_CREDENTIALS_FILE = 			'/opt/vde/cassandra_serv_credentials.json'
SFTP_CREDENTIALS_FILE = 				'/opt/vde/sftp_credentials.json'


# ========================== COMPUTATIONS =====================================
# sample frequency
FREQ =                                  [8, "S"]  # 8 sec.

# missing raw data time limit = keep data from max X time back from now
LIMIT_TIMING_RAW =                      2  # days

# nb of days limit when getting the last timestamp of raw table
LAST_TS_DAYS =                          2  # days

# "since" default value for the earliest timestamp of a flukso
FROM_FIRST_TS_STATUS = 					"server"
# default first timestamp origin : chosen arbitrarily. Ex : since 4min or since 1 days, ...
FROM_FIRST_TS =                         "4min"

# nb lines to insert per batch insert when inserting in cassandra table
INSERTS_PER_BATCH =                     11000


# =========================== CASSANDRA =======================================
# cassandra authentication
CASSANDRA_AUTH_MODE = 					'server'

# cassandra keyspaces
CASSANDRA_KEYSPACE =                    "flukso" 

# cassandra tables names
TBL_ACCESS =                            "access"
TBL_SENSORS_CONFIG =                    "sensors_config"
TBL_RAW =                               "raw"
TBL_RAW_MISSING =                       "raw_missing"
TBL_POWER =                             "power"
TBL_GROUP =                             "group"


# ============================= SERVER ======================================
SERVER_FRONTEND_IP = 					'iridia-vde-frontend.hpda.ulb.ac.be'
SERVER_BACKEND_IP = 					'iridia-vde-db.hpda.ulb.ac.be'

LOG_LEVEL =								"INFO"
LOG_VERBOSE = 							True