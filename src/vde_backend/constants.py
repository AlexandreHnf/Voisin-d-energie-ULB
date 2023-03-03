__title__ = "constants"
__version__ = "2.0.0"
__author__ = "Alexandre Heneffe, Guillaume Levasseur, and Brice Petit"
__license__ = "MIT"


# =========================== PRODUCTION MODE ===============================
# False by default for development and testing purposes.
# Switch to True for production.
PROD = False

# =========================== FILE PATHS ====================================

# Excel tabs to read when loading sensor configuration.
CONFIG_SENSORS_TAB = "Export_InstallationSensors"
CONFIG_ACCESS_TAB = "Export_Access"
CONFIG_CAPTIONS_TAB = "InstallationCaptions"

# Path to local databases.
TMPO_FILE = "/opt/vde/" if PROD else ""
SFTP_LOCAL_PATH = "/opt/vde/sftp_data/" if PROD else "../../output/sftp_data/"

# Log files.
LOG_FILE = "/var/log/vde/prod.log" if PROD else "/var/log/vde/test.log"
LOG_LEVEL = "INFO" if PROD else "DEBUG"
LOG_HANDLER = "logfile" if PROD else "stdout"

# credentials
CASSANDRA_CREDENTIALS_FILE = '/opt/vde/cassandra_serv_credentials.json'
SFTP_CREDENTIALS_FILE = '/opt/vde/sftp_credentials.json'
RTU_CREDENTIALS_FILE = '/opt/vde/rtu_credentials.json'


# ========================== COMPUTATIONS =====================================
# sample frequency
FREQ = [8, "S"]  # 8 sec.

# missing raw data time limit = keep data from max X time back from now
LIMIT_TIMING_RAW = 2  # days

# Period back in time to fetch Flukso data. Will be ignored in production mode.
# Set None to disable.
FROM_FIRST_TS = "5770min"

# nb lines to insert per batch insert when inserting in cassandra table
INSERTS_PER_BATCH = 11000

# Threshold of holes
GAP_THRESHOLD = '4h'

# =========================== CASSANDRA =======================================
# cassandra keyspaces
CASSANDRA_KEYSPACE = "flukso" if PROD else "test"
# Use NetworkTopologyStrategy if more than one datacenter.
CASSANDRA_REPLICATION_STRATEGY = 'SimpleStrategy'
# The replication factor must not exceed the number of nodes in the cluster.
CASSANDRA_REPLICATION_FACTOR = 1

# cassandra tables names
TBL_ACCESS = "access"
TBL_SENSORS_CONFIG = "sensors_config"
TBL_RAW = "raw"
TBL_RAW_MISSING = "raw_missing"
TBL_POWER = "power"
TBL_GROUP = "group"
TBL_RTU_DATA = "rtu"


# ============================= SERVER ======================================
SERVER_FRONTEND_IP = 'iridia-vde-frontend.hpda.ulb.ac.be'
SERVER_BACKEND_IP = 'iridia-vde-db.hpda.ulb.ac.be'
RTU_IP_ADDR = "192.168.0.4"
