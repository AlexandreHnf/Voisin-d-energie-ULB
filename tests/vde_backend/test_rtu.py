import sys
sys.path.insert(1, 'src/vde_backend')

import constants
import mock_rtu_pages
import os.path
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


#['COS PHI', '-0.719300', datetime.datetime(2022, 8, 9, 15, 9, 13, 814978)]
#['PUISSANCE ACTIVE', '17880.000000', datetime.datetime(2022, 8, 9, 15, 9, 13, 815029)]
#['PUISSANCE APPARENTE', '25160.000000', datetime.datetime(2022, 8, 9, 15, 9, 13, 815057)]
#['PUISSANCE REACTIVE', '-10400.000000', datetime.datetime(2022, 8, 9, 15, 9, 13, 815083)]
#['TENSION PHASE 1-2', '237.860001', datetime.datetime(2022, 8, 9, 15, 9, 13, 815119)]
#['TENSION PHASE 2-3', '237.589996', datetime.datetime(2022, 8, 9, 15, 9, 13, 815148)]
#['TENSION PHASE 3-1', '237.909988', datetime.datetime(2022, 8, 9, 15, 9, 13, 815175)]

if __name__ == '__main__':
    unittest.main()

