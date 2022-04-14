#!/bin/bash

# chown -R :group_name /opt/vde/
# chmod g+rwx /opt/vde/
cd /opt/vde/
git clone https://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@github.com/AlexandreHnf/Voisin-d-energie-ULB.git/

# move repository files to correct locations
mv /opt/vde/Voisin-d-energie-ULB/*.py /opt/vde/
mv /opt/vde/Voisin-d-energie-ULB/.tmpo /opt/vde/

mv /opt/vde/Voisin-d-energie-ULB/sensors/* /home/

# chmod g+rwx /etc/systemd/system/
# mv /opt/vde/Voisin-d-energie-ULB/systemd_scripts/* /etc/systemd/system/


# create python virtual environment
virtualenv venv
source /opt/vde/venv/bin/activate
pip install -r /opt/vde/Voisin-d-energie-ULB/requirements.txt

# create log file
# chown -R :group_name /var/log/vde/
# chmod g+rw /var/log/vde/
touch /var/log/vde/logs.log

