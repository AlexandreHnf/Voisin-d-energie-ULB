/* 
Flukso interface - Server
Author : Alexandre Heneffe.
*/

var fs = require('fs') // get fs module for creating write streams
var assert = require('assert');
var cassandra = require('cassandra-driver');
const { Console } = require("console"); // get the Console class

const http = require("http")
const express = require('express'); //Import the express dependency
const bodyParser = require("body-parser");
const { resolve } = require('path');
const { execPath } = require('process');

const router = express.Router();
const app = express();              //Instantiate an express app, the main work horse of the server
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use("/", router);
const server = require('http').createServer(app);  //  express = request handler functions passed to http Server instances

const io = require("socket.io")(server)


// ===================== GLOBAL VARIABLES =========================== 

// constants for server

const CASSANDRA = {
	DATACENTER : 									    "datacenter1",
	CREDENTIALS_FILE : 								"cassandra_serv_credentials.json",
	DOMAIN_NAME : 								    "iridia-vde-frontend.hpda.ulb.ac.be",
  IP :                              "164.15.254.92",

	KEYSPACE : 										    "flukso"
}

const LOG_FILE = 									  "logs.txt"
const ERROR_LOG_FILE = 							"error_logs.txt"

const PORT = 										    "5000"

const VERBOSE =                     true;     // display logs in terminal or not

// ========================= LOGGER =====================================

// make a new logger
const logger = new Console({
  stdout: fs.createWriteStream(LOG_FILE, {flags: 'a'}),
  stderr: fs.createWriteStream(ERROR_LOG_FILE, {flags: 'a'}),
});

// ==================================================================

function showError(error, error_msg) {
  if (VERBOSE) {console.log(error_msg);}
  if (VERBOSE) {console.log(error);}
  logger.error(`${new Date().toISOString()}: ${error_msg}`);
  logger.error(error);
}

var client = null;
try {
  if (process.env.NODE_ENV === 'development') {
    if (VERBOSE) {console.log("Development mode")}
    // Connection to localhost cassandra database1
    client = new cassandra.Client({
      contactPoints: ['ce97f9db-6e4a-4a2c-bd7a-2c97e31e3150', '127.0.0.1'],
      localDataCenter: CASSANDRA.DATACENTER
    });
  }
  else if (process.env.NODE_ENV === 'production') {
    if (VERBOSE) {console.log("Production mode")}
    // Connection to a remote cassandra cluster
    const cassandra_credentials = JSON.parse(fs.readFileSync(CASSANDRA.CREDENTIALS_FILE, 'utf8'));
    var authProvider = new cassandra.auth.PlainTextAuthProvider(
      cassandra_credentials.username, 
      cassandra_credentials.password
    );
    var contactPoints = [CASSANDRA.IP, CASSANDRA.DOMAIN_NAME];
    client = new cassandra.Client({
      contactPoints: contactPoints, 
      authProvider: authProvider, 
      localDataCenter: CASSANDRA.DATACENTER,
      keyspace: CASSANDRA.KEYSPACE
    });
  }
} catch(error) {
  showError(error, "! Error when setting up cassandra client");
}


const TABLES = {'access': 'access', 'raw': 'raw', 'power': 'power', 'groups': 'power'};


// ========================= FUNCTIONS ==================================

async function connectCassandra() {
  client.connect().then(() => {
  if (VERBOSE) {console.log("> Connected to Cassandra !")}
	logger.log(`${new Date().toISOString()}: > Connected to Cassandra !`);
  });
}

connectCassandra().catch((error) => {
  showError(error, "There was an error connecting to the Cassandra database.");
});


//========== Launch main ==============
main().catch((error) => {
  showError(error, "An error occured when launching the server:");
});


//Ensure all queries are executed before exit
function execute(query, params, callback) {
  /* 
  may be obsolete
  */
  return new Promise((resolve, reject) => {
    client.execute(query, params, (err, result) => {
      if(err) {
        reject()
      } else {
        callback(err, result);
        resolve(result);
      }
    });
  });
}


async function queryFluksoData(data_type, date, home_id, response) {
  /*
  Query power data from cassandra based on the provided date and home ID
  */
  try {
    where_homeid = `home_id = '${home_id}' and`;
    var query = `SELECT * FROM ${CASSANDRA.KEYSPACE}.${TABLES[data_type]} WHERE ${where_homeid} day = ? ALLOW FILTERING;`

    const result = await client.execute(query, [date], { prepare: true });

    let j = 0;
    let res = [];
    // paging to get the large fetch results set in an array to send to client
    for await (const row of result) {
      res.push(row);
      j++;
    }

    response.json({msg: "date well received!", data: res});
  } catch(error) {
    showError(error, "! Error when querying cassandra data.");
  }
}

async function doesClientExist(username, response) {
  /* 
  Check if the username used to log in is indeed in db
  */
  try {
    var query = `SELECT * FROM ${CASSANDRA.KEYSPACE}.${TABLES["access"]} WHERE login = ? ALLOW FILTERING;`
    const result = await client.execute(query, [username], { prepare: true });

    response.json({status: result.rows.length > 0, grp_ids: result});
  } catch (error) {
    showError(error, "! Error when querying cassandra access table.");
  }
}


// ========================================== GET ==========================================

// ======= INDEX =======
//Idiomatic expression in express to route and respond to a client request
app.get('/', (req, res) => {        //get requests to the root ("/") will route here
    res.sendFile('index.html', {root: __dirname});      //server responds by sending the client.html file to the client's browser
                                                        //the .sendFile method needs the absolute path to the file, see: https://expressjs.com/en/4x/api.html#res.sendFile 
});

// send the index javascript file
app.get('/index.js', function(req, res) {
  res.sendFile('/index.js', {root: __dirname});
});

// ======= CLIENT =======
app.get('/client.html', (req, res) => {        //get requests to the root ("/") will route here
  res.sendFile('client.html', {root: __dirname});      //server responds by sending the client.html file to the client's browser
                                                      //the .sendFile method needs the absolute path to the file, see: https://expressjs.com/en/4x/api.html#res.sendFile 
});

app.get('/client.js', function(req, res) {
  res.sendFile('/client.js', {root: __dirname});
});

// ===========================
// send the css file
app.get('/styles.css', function(req, res) {
  res.sendFile('/styles.css', {root: __dirname});
});

// send the chartjs utils file
app.get('/chart.utils.js', function(req, res) {
  res.sendFile('/chart.utils.js', {root: __dirname});
});

// ========================================== POST =========================================

router.post('/doesClientExist', (request, response) => {
  /* 
  request from client => client wants to login
  */ 
  const username = request.body.username.toUpperCase();

  doesClientExist(username, response);
});

router.post('/date', (request, response) => {
  /* 
  client request flukso data of a specific day
  */
  const date = request.body.date;
  const data_type = request.body.data_type;  // raw or power
  const home_id = request.body.home_id;
  if (VERBOSE) {console.log(`> date request from ${home_id} | date : ${date}, type : ${data_type}`);}
  logger.log(`${new Date().toISOString()}: > date request from ${home_id} | date : ${date}, type : ${data_type}`);
  queryFluksoData(data_type, date, home_id, response);
});


// =============================================================================================

function onUserConnected(socket) {
  /* 
  Function that handles a new connection from a user and start listening for its queries
  */
  if (VERBOSE) {console.log('-> New user connected');}
  logger.log(`${new Date().toISOString()}: -> New user connected`);

  // disconnection
  socket.on('disconnect', () => {
    if (VERBOSE) {console.log('-> User disconnected');}
	  logger.log(`${new Date().toISOString()}: -> User disconnected`);
  });
}


async function main() {

  //Authorize clients only after updating the server's data
  server.listen(PORT, () => {
    if (VERBOSE) {console.log(`> Server listening on port ${PORT}`);}
	  logger.log(`${new Date().toISOString()}: > Server listening on port ${PORT}`);
  });

  // socket connection with the client
  // io.on("connection", onUserConnected);
}