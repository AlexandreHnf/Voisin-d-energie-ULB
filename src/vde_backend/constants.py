__title__ = "Constants"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# =========================== FILE PATHS ====================================
# files paths
FLUKSO_TECHNICAL_FILE =                 "../../../flukso_config/Configuration.xlsx"

UPDATED_FLUKSO_TECHNICAL_FILE =         "../../../sensors/updated_sensors.csv"
COMPACT_SENSOR_FILE =                   "../../../sensors/sensors_technical.csv"
PHASE_TO_MODIF_FILE =                   "../../../sensors/phases_to_modify.txt"
# SENSOR_FILE =                           "../../../sensors/sensors.csv"
UPDATED_SENSORS_FILE =                  "../../../sensors/updated_sensors.csv"

OUTPUT_FILE =                           '../../../output/fluksoData/'

GROUPS_FILE =                           "../../../sensors/grouped_homes_sensors.txt"

IDS_FILE =                              "../../../sensors/ids.json"
GIDS_FILE =                             "../../../sensors/grp_ids.json"

TMPO_FILE = 							""

LOG_FILE = 								"../../../logs/backend_logs.log"

CASSANDRA_CREDENTIALS_FILE = 			'cassandra_serv_credentials.json'
SFTP_CREDENTIALS_FILE = 				'sftp_credentials.json'


# ========================== COMPUTATIONS =====================================
# sample frequency
FREQ =                                  [8, "S"]  # 8 sec.

# missing raw data time limit = keep data from max X time back from now
LIMIT_TIMING_RAW =                      2  # days

# nb of days limit when getting the last timestamp of raw table
LAST_TS_DAYS =                          2  # days

# "since" default value for the earliest timestamp of a flukso
# normally since = 0, but for testing, we put since = 4min
FROM_FIRST_TS_STATUS = 					"local"
FROM_FIRST_TS =                         "1days" 

# nb lines to insert per batch insert when inserting in cassandra table
INSERTS_PER_BATCH =                     11000


# =========================== CASSANDRA =======================================
# cassandra authentication
CASSANDRA_AUTH_MODE = 					'local'

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