import pandas as pd
import pyToCassandra as ptc

from constants import (
    CASSANDRA_KEYSPACE,
    RTU_CREDENTIALS_FILE,
    RTU_IP_ADDR,
    TBL_RTU_DATA,
)
from rtu_comm import RTUConnector

COLS = {
    'ip': 'TEXT',
    'day': 'TEXT',
    'ts': 'TIMESTAMP',
    'cos_phi': 'FLOAT',
    'active': 'FLOAT',
    'apparent': 'FLOAT',
    'reactive': 'FLOAT',
    'tension1_2': 'FLOAT',
    'tension2_3': 'FLOAT',
    'tension3_1': 'FLOAT',
}


def create_rtu_table():
    ptc.createTable(
        CASSANDRA_KEYSPACE,
        TBL_RTU_DATA,
        [ ' '.join(kv) for kv in COLS.items() ],
        ['ip'],
        ['day', 'ts'],
        {'day': 'ASC', 'ts': 'ASC'}
    )


def prepare_rtu_row(vals: pd.Series, ip: str):
    vals = vals.rename(index={
        'COS PHI': 'cos_phi',
        'PUISSANCE ACTIVE': 'active',
        'PUISSANCE APPARENTE': 'apparent',
        'PUISSANCE REACTIVE': 'reactive',
        'TENSION PHASE 1-2': 'tension1_2',
        'TENSION PHASE 2-3': 'tension2_3',
        'TENSION PHASE 3-1': 'tension3_1',
    })
    vals['ip'] = ip
    vals['day'] = vals['ts'].date().isoformat()
    return vals[COLS.keys()] # Ensure all columns are present.


def main():
    create_rtu_table()
    creds = ptc.load_json_credentials(RTU_CREDENTIALS_FILE)
    rtu = RTUConnector(RTU_IP_ADDR, creds['user'], creds['pwd'])
    rtudat = rtu.read_values()
    rtu_row = prepare_rtu_row(rtudat, rtu.addr)
    ptc.insert(
        CASSANDRA_KEYSPACE,
        TBL_RTU_DATA,
        rtu_row.index,
        rtu_row.values
    )


if __name__ == '__main__':
    main()

