# Voisin-d-energie-ULB
 

<div id="top"></div>

<!-- PROJECT LOGO -->
<br />
<div align="center">

<h3 align="center">"Voisin d'énergie"</h3>

  <p align="center">
    Project made in collaboration with ULB (Université Libre de Bruxelles) - IRIDIA laboratory, Innoviris, IGEAT, BinHôme, Energy4Commons and BEAMS.
    <br />
    <a href="https://wiki.voisinsenergie.agorakit.org/"><strong>Wifi de Voisin d'énergie</strong></a>
    <br />
    <br />
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About the Project</a>
      <ul>
        <li><a href="#built-with">Built with</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

The project consists in creating a backend architecture and a frontend system to visualize Fluksometer electrical data : consumption, production and total. 

<p align="right">(<a href="#top">back to top</a>)</p>



### Built With

* [python3](https://www.python.org/)
* [Cassandra](https://cassandra.apache.org/_/index.html)
* [Javascript](https://javascript.info/)
* [Bootstrap](https://getbootstrap.com/)
* [Nodejs](https://nodejs.dev/)
* [socket.io](https://socket.io/fr/docs/v4/)
* [express](https://expressjs.com/fr/)
* [chartjs](https://www.chartjs.org/)

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

This section describes how to setup the project locally.

### Prerequisites

List of packages to install (Ubuntu) :

#### 1) Frontend
* nodejs
    ```sh
    sudo apt install nodejs
    ```
* nodejs npm
    ```sh
    sudo apt install nodejs npm
    ```
* express
    ```sh
    npm install express
    ```
* socket.io
    ```sh
    npm install -g socket.io --save
    ```
* cassandra-driver 
    ```sh
    npm install -g cassandra-driver
    ```
* body-parser
    ```sh
    npm install --save body-parser
    ```
* fs
    ```sh
    npm install fs
    ```

#### Backend 

* python3 (> v3.8.10)
    ```sh
    sudo apt install python3
    ```
* numpy (> v1.17.4)
    ```sh
    sudo apt install python3-numpy
    ```
* pandas (> v1.1.0)
    ```sh
    sudo apt install python3-pandas
    ```
* tmpo (<a href="https://github.com/flukso/tmpo-py"><strong>tmpo repository</strong></a>) (v0.2.10)
    ```sh
    sudo pip3 install tmpo
    ```
* cassandra-driver (> v3.25.0)
    ```sh
    sudo -H pip3 install cassandra-driver
    ```
* mail 
	```sh
	sudo apt install mailutils
	```

#### Cassandra
    
    echo "deb http://www.apache.org/dist/cassandra/debian 40x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
    sudo apt install openjdk-8-jre-headless
    cd /usr/lib/jvm/java-8-openjdk-amd64/
    pwd
    sudo nano ~/.bashrc   => set JAVA_HOME = <pwd>
    sudo apt install curl
    curl https://downloads.apache.org/cassandra/KEYS | sudo apt-key add -
    sudo apt-get install cassandra
    sudo pip install cqlsh 


<p align="right">(<a href="#top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## Usage

In this section, we describe how to use the different scripts.

### Prerequisite
1. First, start Cassandra service
   ```sh
   sudo service cassandra start
   ``` 
2. Start cqlsh and enter the 'flukso' keyspace
   ```sh
   cqlsh
   use flukso;
   ```
3. Execute any script using the instructions below : 
4. Always make sure to stop cassandra service when we are done using it. It is very important.
   ```sh
   sudo service cassandra stop
   ```

### Scripts

Here are the list of all the executable scripts aswell as their arguments : 


* sync raw flukso data : The script automatically get the new Flukso data using the tmpo API and store it in Cassandra. No need to specify any arguments.
  ```sh
  sync_flukso.py
  ```

* preprocess Flukso sensors : The script contains a lot of different functions that are meant to be used before the raw data syncing. The script allows, among others, to create the neccessary Cassandra tables, as well as inserting the new data in them. However, those functions are automatically triggered using one command : 
  
  ```sh
  preprocess_sensors_config.py [config]
  ```
  * The _config_ argument allows to specify a config file path. The configuration file must be an Excel file containing those 3 tabs : 
	1. **Export_InstallationSensors** : All flukso info : 
		* Installation ID, 
		* Start date, 
		* Flukso ID, 
		* Phase name, 
		* Network factor, 
		* Production factor, 
		* Consumption factor, 
		* Sensor ID, 
		* Sensor Token
	2. **Export_Access** : All login IDS along with their corresponding installation IDs (group ids they belong to).
	3. **InstallationCaptions** : All installation IDs along with their captions (One sentence describing the nature of the group).


<br />

* Alerts : The script sends alert per email if it detects some unusual behaviour in the system
  ```sh
  alerts.py --mode MODE
  ```
  * The --mode argument is the behaviour we want to monitor for potential alerts. MODE : 
    1. _missing_ (default): Check if the number of missing data in the past is ok.
    2. _sign_ : Check if the signs are correct in power data. It can be incorrect/incoherent if : 
       * we see negative consumption values
       * we see positive production values
       * we see photovoltaic values during the night

<br />

* dump_csv : The script allows to get power data from Cassandra database into csv files. 1 file = 1 home and 1 specific day.
  ```sh
  dump_csv.py [--home HOME_ID] [--day DAY] [--date_range DATE1 DATE2] output_filename
  ```
  * The --day argument is the specific day we can query. It has to be in the form : YYYY_MM_DD
    *  If no specific day is specified, then the script automatically retrieve data from the last saved date based on the previous local files for each home. If no data has been saved for a home yet, the script will take all the history available in the database for that home.
  * The --home argument allows to save data of 1 home specifically
  * The --date_range argument allows to select a start date and an end date and retrieve the data for all dates between these two dates (can be combined with --home).  
  * The output_filename argument is the directory path where the csv data files will be saved.

<br />

<br />


<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<!-- CONTACT -->
## Contact

* Alexandre Heneffe - alexandre.heneffe@ulb.be
* Guillaume Levasseur - guillaume.levasseur@ulb.be

<p align="right">(<a href="#top">back to top</a>)</p>