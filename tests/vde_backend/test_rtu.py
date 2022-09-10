import sys
sys.path.insert(1, 'src/vde_backend')
# Call tests from the top-level folder, like:
# python3 tests/vde_backend/test_rtu.py

import constants
import mock_rtu_pages
import os.path
import pandas as pd
import requests
import unittest
import unittest.mock

logdir = os.path.dirname(constants.LOG_FILE)
if os.path.exists(logdir):
    import rtu_comm
else:
    raise FileNotFoundError('Please create {} before running the tests.'.format(logdir))


def mocked_requests_get(*args, **kwargs):
    url = args[0]
    auth = kwargs['auth']
    resp = requests.Response()
    resp.encoding = 'UTF-8'
    resp.url = url
    if auth.username == 'Test1' and auth.password == 'Test2':
        if url == 'http://192.168.0.1/rtui/index.html':
                resp.status_code = 200
                resp.reason = 'OK'
                resp._content = mock_rtu_pages.LOGIN_PAGE.encode(resp.encoding)
        elif url == 'http://192.168.0.1/ABBRTU560/PrioI_HwTree':
                resp.status_code = 200
                resp.reason = 'OK'
                resp._content = mock_rtu_pages.HARDWARE_PAGE.encode(resp.encoding)
        elif url == 'http://192.168.0.1/ABBRTU560/hwTree_pdInfoMon?IDNR=0&REF=549&MODE=1':
                resp.status_code = 200
                resp.reason = 'OK'
                resp._content = mock_rtu_pages.DATA_PAGE.encode(resp.encoding)
        else:
            resp.status_code = 404
            resp.reason = 'Not Found'
    else:
        resp.status_code = 401
        resp.reason = 'Unauthorized'

    return resp

class TestRTUDriver(unittest.TestCase):
    @unittest.mock.patch('requests.Session.get', side_effect=mocked_requests_get)
    def test_rtu_init_wrong_creds(self, mock_get):
        self.assertRaises(
            requests.HTTPError,
            rtu_comm.RTUConnector,
            '192.168.0.1',
            'test',
            'blah',
            n_retries=2
        )

    @unittest.mock.patch('requests.Session.get', side_effect=mocked_requests_get)
    def test_rtu_init_right_creds(self, mock_get):
        rtu = rtu_comm.RTUConnector('192.168.0.1', 'Test1', 'Test2')
        self.assertEqual(
            'http://192.168.0.1/ABBRTU560/hwTree_pdInfoMon?IDNR=0&REF=549&MODE=1',
            rtu.url_hwinfo,
        )

    @unittest.mock.patch('requests.Session.get', side_effect=mocked_requests_get)
    def test_rtu_read_values_tiv(self, mock_get):
        rtu = rtu_comm.RTUConnector('192.168.0.1', 'Test1', 'Test2')
        test_vals = rtu.read_values().to_dict()
        self.assertEqual('CET', test_vals['ts'].tz.zone)
        num_vals = { k: v for k, v in test_vals.items() if k != 'ts' }
        self.assertDictEqual(
            {
                'COS PHI': -0.906700,
                'PUISSANCE ACTIVE': 30720.000000,
                'PUISSANCE APPARENTE': 34920.000000,
                'PUISSANCE REACTIVE': -6120.000000,
                'TENSION PHASE 1-2': 237.500000,
                'TENSION PHASE 2-3': 237.229996,
                'TENSION PHASE 3-1': 237.319992,
            },
            num_vals,
        )


if __name__ == '__main__':
    unittest.main()

