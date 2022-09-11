import pandas as pd
import pyToCassandra as ptc

from constants import (
    CASSANDRA_KEYSPACE,
    RTU_CREDENTIALS_FILE,
    RTU_IP_ADDR,
    TBL_RTU_DATA,
)
from rtu_comm import RTUConnector
from utils import load_json_credentials


def main():
    rtu_creds = load_json_credentials(RTU_CREDENTIALS_FILE)


if __name__ == '__main__':
    main()

