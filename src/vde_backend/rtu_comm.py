import pandas as pd
import requests
import time

from bs4 import BeautifulSoup
from utils import logging


class RTUConnector():
    """
    HTTP driver for ABB RTU 560 series.
    """
    datefmt = "%Y-%m-%d, %H:%M:%S"

    def __init__(self, addr: str, user: str, pwd: str, n_retries=10):
        """
        Initialize the RTU HTTP driver.

        Args:
            addr: IP address of the RTU
            user: login
            pwd:  password

        Notes:
            UI index: http://192.168.0.1/rtui/index.html
            To get the hardware tree: http://192.168.0.1/ABBRTU560/PrioI_HwTree
            To get the measures: http://192.168.0.1/ABBRTU560/hwTree_pdInfoMon?IDNR=0&REF=549&MODE=1
        """
        self.addr = str(addr)
        self.n_retries = int(n_retries)
        self.auth = requests.auth.HTTPDigestAuth(user, pwd)
        self.sess = requests.Session()
        self.url_index = "http://{}/rtui/index.html".format(self.addr)
        self.url_hwtree = "http://{}/ABBRTU560/PrioI_HwTree".format(self.addr)
        self.connect()
        self.url_hwinfo = self.get_hw_addr()

    def connect(self):
        resp = requests.Response()
        for _ in range(self.n_retries):
            resp = self.sess.get(self.url_index, auth=self.auth)
            if resp.ok:
                break

            time.sleep(1)

        logging.debug('RTU connect: GET {}'.format(resp.status_code))
        logging.debug('RTU connect: Cookies {}'.format(self.sess.cookies.get_dict()))
        resp.raise_for_status()

    def get_hw_addr(self, prefix="hwTree_pdInfoMon"):
        resp = self.sess.get(self.url_hwtree, auth=self.auth)
        htmltree = BeautifulSoup(resp.text, "html.parser")
        links = []
        for item in htmltree.li.find_all("a"):
            link = item.get("href")
            links.append(link)
            if prefix in link:
                url_hwinfo = "http://{}".format(self.addr) + link
                return url_hwinfo

        raise ValueError(
            "RTUConnector.get_hw_addr: no matching URL found for '{}' among {}"
            .format(prefix, ", ".join(links))
        )

    def read_values(self):
        resp = self.sess.get(self.url_hwinfo, auth=self.auth)
        logging.debug('RTU read: GET {}'.format(resp.status_code))
        data = BeautifulSoup(resp.text, "html.parser")
        rows = [
            [c.text for c in row.find_all("td")]
            for row in data.table.find_all("tr")
        ]
        # Position 0 does not contain data.
        names = [r[1] for r in rows]
        values = [float(r[2]) for r in rows]
        t_strings = {r[3] for r in rows}
        ts = pd.Timestamp.now(tz='CET')
        for timestr in t_strings:
            # TIV = time invalid, NSY = not synchronized
            if "TIV" not in timestr:
                ts = pd.to_datetime(
                    timestr[1:21],
                    format=self.datefmt
                ).tz_localize('CET')
                # Stop at first valid time.
                break

        names.append('ts')
        values.append(ts)
        return pd.Series(values, index=names)
