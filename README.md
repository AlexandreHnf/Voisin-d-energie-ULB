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

* python3
    ```sh
    sudo apt install python3
    ```
* numpy
    ```sh
    sudo apt install python3-numpy
    ```
* pandas
    ```sh
    sudo apt install python3-pandas
    ```
* tmpo (<a href="https://github.com/flukso/tmpo-py"><strong>tmpo repository</strong></a>)
    ```sh
    sudo pip3 install tmpo
    ```
* cassandra-driver 
    ```sh
    sudo -H pip3 install cassandra-driver
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




<!-- USAGE EXAMPLES -->
## Usage

In this section, we describe how to use the different scripts and with the right arguments.

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
3. execute any script using the instructions below : 

### Scripts

Here are the list of all the executable scripts aswell as their arguments : 


* sync raw flukso data : The script automatically get the new Flukso data using the tmpo API and store it in Cassandra. 
  ```sh
  syncRawFluksoData.py --mode automatic
  ```

* preprocess Flukso sensors : The script contains a lot of different functions that are meant to be used before the raw data syncing. 
  
  ```sh
  preprocessFluksoSensors.py --task TASK --table_name TABLE_NAME
  ```
  * The --task argument allows us to choose between different tasks. TASK : 
  
    1. _create_table_ : Allows to create new table in cassandra. It requires the --table_name argument TABLE_NAME:
       1. **access** : table that contains the users login information (login id, group ids)
       2. **sensors_config** : table that contains the sensors configurations (home_id, sensor id, sensor token, flukso id, signs, ..) 
       3. **raw** : table that contains the raw flukso data 
       4. **power** : table that contains power data based on the raw data : consumption, production and total
       5. **raw_missing** : table that contains timestamps for missing data for each sensor from the previous synchronization query
       6. **group** : table that contains groups ids along with their captions
    2. _new_config_ : write new configuration to the **sensors_config** table 
    3. _login_config_ : write new login info to the **access** table
    4. _group_captions_ : write new group captions info to the **group** table
  
  * Remark : the tasks _2_, _3_ and _4_ depend on 1 specific Excel file that has to be defined in the **flukso_config** directory and its name must be : **Configuration.xlsx**. This file contains 3 tabs : 
    * Export_InstallationSensors : all homes and sensors info
    * Export_Access : All login ids and associated group ids
    * InstallationCaptions : All group ids along with their captions (descriptions)

<br />

* Alerts : The script sends alert per email if it detects some unusual behaviour in the system
  ```sh
  alerts.py --mode MODE
  ```
  * The --mode argument is the behaviour we want to monitor for potential alerts. MODE : 
    1. _missing_ : Check if the number of missing data in the past is ok.
    2. _sign_ : Check if the signs are correct in power data. It can be incorrect/incoherent if : 
       * we see negative consumption values
       * we see positive production values
       * we see photovoltaic values during the night




<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Alexandre Heneffe - alexandre.heneffe@ulb.be

<p align="right">(<a href="#top">back to top</a>)</p>


<p align="right">(<a href="#top">back to top</a>)</p>
