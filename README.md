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
3. execute any script using the instructions below : 

### Scripts

Here are the list of all the executable scripts aswell as their arguments : 


* sync raw flukso data : The script automatically get the new Flukso data using the tmpo API and store it in Cassandra. No need to specify any arguments.
  ```sh
  syncRawFluksoData.py
  ```

* preprocess Flukso sensors : The script contains a lot of different functions that are meant to be used before the raw data syncing. The script allows, among others, to create the neccessary Cassandra tables, as well as inserting the new data in them. However, those functions are automatically triggered using one command : 
  
  ```sh
  preprocessFluksoSensors.py [config]
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

* **Remark** : All the scripts are using constants that are defined in the _constants.py_ file. However, this file is a symbolic link to another file, which can either be _constants_dev.py_ (development) or _constants_prod.py_ (production) depending on the environment. The repository only contains the production version of the constants file. To obtain the development version, we have to proceed like this : 
	1. Copy the _constants_prod.py_ file in the same directory
	2. Change the constants according to the system (paths, ...)
	3. Create a symbolic link to _constants.py_
		```sh
		sudo ln -s constants_prod.py constants.py
		```

<br />


<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Alexandre Heneffe - alexandre.heneffe@ulb.be

<p align="right">(<a href="#top">back to top</a>)</p>


<p align="right">(<a href="#top">back to top</a>)</p>
