import base64
import requests
import time

from bs4 import BeautifulSoup
from datetime import datetime
from requests.auth import HTTPDigestAuth

# For debugging:
#from http.client import HTTPConnection
#HTTPConnection.debuglevel = 1


def encrypt_pwd(s):
    """
    JS code:
    --------
    rtu.util.encryptPassword=function(t){
        return rtu.util.encryptBase64AndReplacePlusSlashEqual(t)
    },
    rtu.util.encryptBase64AndReplacePlusSlashEqual=function(t){
        var e="",
        r=window.btoa(t),
        n=r.replace(/\+/g,"-"),
        u=n.replace(/\//g,"_");
        return e=u.replace(/=/g,".")
    }
    """
    enc = base64.urlsafe_b64encode(s.encode())
    enc = enc.replace(b'=', b'.')
    return enc


class RTUConnector():
    """
    HTTP driver for ABB RTU 560 series.
    """
    def __init__(self, addr, user, pwd):
        """
        Initialize the RTU HTTP driver.

        Args:
            addr:   (str) IP address of the RTU
            user:   (str) login
            pwd:    (str) password

        Note:
            UI index: http://192.168.0.1/rtui/index.html
            To get the hardware tree: http://192.168.0.1/ABBRTU560/PrioI_HwTree
            To get the measures: http://192.168.0.1/ABBRTU560/hwTree_pdInfoMon?IDNR=0&REF=549&MODE=1
        """
        self.addr = str(addr)
        self.auth = HTTPDigestAuth(user, pwd)
        self.sess = requests.Session()
        self.url_index = "http://{}/rtui/index.html".format(self.addr)
        self.url_hwtree = "http://{}/ABBRTU560/PrioI_HwTree".format(self.addr)
        self.connect()
        self.url_hwinfo = self.get_hw_addr()

    def connect(self, retries=10):
        resp = requests.Response()
        for _ in range(retries):
            resp = self.sess.get(self.url_index, auth=self.auth)
            if resp.ok:
                break

            time.sleep(1)

        print(resp.status_code, self.sess.cookies.get_dict())
        print()
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
        raise ValueError("RTUConnector.get_hw_addr: no matching URL found for '{}' among {}".format(prefix, ", ".join(links)))

    def iter_values(self, delay_s=5):
        datefmt = "%Y-%m-%d, %H:%M:%S"
        while True:
            resp = self.sess.get(self.url_hwinfo, auth=self.auth)
            print(resp.status_code, end='\r')
            data = BeautifulSoup(resp.text, "html.parser")
            for row in data.table.find_all("tr"):
                row = [ c.text for c in row.find_all("td") ]
                row = row[1:] # name, value, timestamp
                timestr = row[2] # TIV = time invalid, NSY = not synchronized
                if "TIV" in timestr:
                    row[2] = datetime.now()
                else:
                    row[2] = datetime.strptime(timestr[1:20], datefmt)

                yield row

            time.sleep(delay_s)


if __name__ == "__main__":
    """ 
    - pour avoir l'ip du pc : ip a
    - Guillaume : sudo ip addr add 192.168.0.2/24 dev eth0
    - Alexandre : sudo ip addr add 192.168.0.2/24 dev enx9cebe8454ad1
    
    - connexion basse tension physiquement :
        - ouvrir avec la clé grise la boite
        - brancher le cable d'alimentation
        - allumer le fusible (ON)
        - brancher le cable ethernet
        
    - http://192.168.0.1 pour se connecter a l'interface web de la cabine :
        - identifiant : ULB
        - mdp : Guillaume
    """

    addr = "192.168.0.1"
    user = "ULB"
    pwd = "Guillaume"
    rtu = RTUConnector(addr, user, pwd)
    for t in rtu.iter_values():
        print(t)

