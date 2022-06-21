# Voisin-d-energie-ULB
 

<div id="top"></div>

<!-- PROJECT LOGO -->
<br />
<div align="center">

<h3 align="center">Projet "Voisin d'énergie"</h3>

  <p align="center">
    Projet en collaboration avec l'Université Libre de Bruxelles - Laboratoire "IRIDIA"
    <br />
    <a href="https://wiki.voisinsenergie.agorakit.org/"><strong>Wifi de Voisin d'énergie</strong></a>
    <br />
    <br />
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table des matières</summary>
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


### 

syncRawFluksoData.py --mode automatic

preprocessFluksoSensors.py 
    create_table + table_name (access, sensors_config, raw, power, raw_missing, group)
    new_config 
    login_config
    group_captions

alerts.py --m 
    missing
    sign



_For more examples, please refer to the [Documentation](https://example.com)_

<p align="right">(<a href="#top">back to top</a>)</p>




<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Alexandre Heneffe - alexandre.heneffe@ulb.be

<p align="right">(<a href="#top">back to top</a>)</p>


<p align="right">(<a href="#top">back to top</a>)</p>