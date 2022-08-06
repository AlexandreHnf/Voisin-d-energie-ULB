__title__ = "Constants"
__version__ = "1.0.0"
__author__ = "Alexandre Heneffe"
__license__ = "MIT"
__copyright__ = "Copyright 2022 Alexandre Heneffe"


# =========================== PRODUCTION MODE ===============================
# False by default for development and testing purposes.
# Switch to True for production.
PROD = False

# =========================== FILE PATHS ====================================

# Excel tabs to read when loading sensor configuration.
CONFIG_SENSORS_TAB =                    "Export_InstallationSensors"
CONFIG_ACCESS_TAB =                     "Export_Access"
CONFIG_CAPTIONS_TAB =                   "InstallationCaptions"

# Path to local databases.
TMPO_FILE = 		"/opt/vde/" if PROD else ""
SFTP_LOCAL_PATH = 	"/opt/vde/sftp_data/" if PROD else "../../output/sftp_data/"

# Log files.
LOG_FILE = 			"/var/log/vde/prod.log" if PROD else "/var/log/vde/test.log"

# credentials
CASSANDRA_CREDENTIALS_FILE = 			'/opt/vde/cassandra_serv_credentials.json'
SFTP_CREDENTIALS_FILE = 				'/opt/vde/sftp_credentials.json'


# ========================== COMPUTATIONS =====================================
# sample frequency
FREQ =                                  [8, "S"]  # 8 sec.

# missing raw data time limit = keep data from max X time back from now
LIMIT_TIMING_RAW =                      2  # days

# Period back in time to fetch Flukso data. Set None to disable.
FROM_FIRST_TS =                         None if PROD else "10min"

# nb lines to insert per batch insert when inserting in cassandra table
INSERTS_PER_BATCH =                     11000


# =========================== CASSANDRA =======================================
# cassandra keyspaces
CASSANDRA_KEYSPACE =                    "flukso" if PROD else "test"

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

LOG_LEVEL =								"INFO" if PROD else "DEBUG"
