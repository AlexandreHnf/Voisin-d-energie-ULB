var fs = require('fs') // get fs module for creating write streams
var assert = require('assert');
// ”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var cassandra = require('cassandra-driver');
const { Console } = require("console"); // get the Console class

const http = require("http")
const io = require("socket.io")
const express = require('express'); //Import the express dependency
const bodyParser = require("body-parser");
const { resolve } = require('path');
const router = express.Router();
const app = express();              //Instantiate an express app, the main work horse of this server
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use("/", router);
const port = 5000;                  //Save the port number where your server will be listening
const server = require('http').createServer(app);  //  express = request handler functions passed to http Server instances

// ===================== GLOBAL VARIABLES =========================== 


var client = null;
if (process.env.NODE_ENV === 'development') {
	console.log("Development mode")
	// Connection to localhost cassandra database1
	client = new cassandra.Client({
		contactPoints: ['ce97f9db-6e4a-4a2c-bd7a-2c97e31e3150', '127.0.0.1'],
		localDataCenter: 'datacenter1'
	});
}
else if (process.env.NODE_ENV === 'production') {
	console.log("Production mode")
	// Connection to a remote cassandra cluster
	const cassandra_credentials = JSON.parse(fs.readFileSync('cassandra_serv_credentials.json', 'utf8'));
	var authProvider = new cassandra.auth.PlainTextAuthProvider(
		cassandra_credentials.username, 
		cassandra_credentials.password
	);
	var contactPoints = ['iridia-vde-frontend.hpda.ulb.ac.be'];
	client = new cassandra.Client({
		contactPoints: contactPoints, 
		authProvider: authProvider, 
		localDataCenter: 'datacenter1',
		keyspace:'flukso'
	});
}


const TABLES = {'raw': 'raw', 'power': 'power', 'groups': 'groups_power'};

// ========================= LOGGER =====================================

// make a new logger
const logger = new Console({
  stdout: fs.createWriteStream("logs.txt", {flags: 'a'}),
  stderr: fs.createWriteStream("error_logs.txt", {flags: 'a'}),
});


// ========================= FUNCTIONS ==================================

async function connectCassandra() {
  client.connect().then(() => {
	console.log("> Connected to Cassandra !")
	logger.log(`${new Date().toISOString()}: > Connected to Cassandra !`);
  });
}

connectCassandra().catch((e) => {
  console.error("There was an error connecting to the Cassandra database.");
  console.error(e);
  logger.error(`${new Date().toISOString()}: There was an error connecting to the Cassandra database`);
});


//========== Launch main ==============
main().catch((e) => {
  console.error("An error occured when launching the server:");
  console.error(e);
  logger.log(`${new Date().toISOString()}: An error occured when launching the server`);
});


//Ensure all queries are executed before exit
function execute(query, params, callback) {
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


function queryFluksoData(data_type, date, home_id, response) {
  where_homeid = "";
  if (home_id != "flukso_admin") {  // if not admin
    where_homeid = `home_id = '${home_id}' and`;
  }
  var query = `SELECT * FROM flukso.${TABLES[data_type]} WHERE ${where_homeid} day = ? ALLOW FILTERING;`

  var data = execute(query, [date], (err, result) => {
	  assert.ifError(err);
    // console.log(result.rows[0]);
    // console.log(result.rows.length);
    
	}).then((result) => {
    // console.log(result.rows[1]);
    response.json({msg: "date well received!", data: result});
  });
}


async function queryFluksoData2(data_type, date, home_id, response) {
  where_homeid = "";
  if (home_id != "flukso_admin") {  // if not admin
    where_homeid = `home_id = '${home_id}' and`;
  }
  var query = `SELECT * FROM flukso.${TABLES[data_type]} WHERE ${where_homeid} day = ? ALLOW FILTERING;`

  const result = await client.execute(query, [date], { prepare: true });

  let j = 0;
  let res = [];
  for await (const row of result) {
    // console.log(j);
    // console.log(row)
    res.push(row);
    j++;
  }

  response.json({msg: "date well received!", data: res});
}
// ==============


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

router.post('/date', (request, response) => {
  // client request flukso data of a specific day
  const date = request.body.date;
  const data_type = request.body.data_type;  // raw or power
  const home_id = request.body.home_id;
  console.log(`> date request from ${home_id} | date : ${date}, type : ${data_type}`);
  logger.log(`${new Date().toISOString()}: > date request from ${home_id} | date : ${date}, type : ${data_type}`);
  // queryFluksoData(data_type, date, home_id, response);
  queryFluksoData2(data_type, date, home_id, response);
});


async function main() {
  let today = new Date().toISOString().slice(0, 10);  // format (YYYY MM DD)
  //console.log(today);
  //Authorize clients only after updating the server's data
  server.listen(port, () => {
    console.log(`> Server listening on port ${port}`);
	logger.log(`${new Date().toISOString()}: > Server listening on port ${port}`);
  });
//   console.log(new Date().toLocaleTimeString());
  io(server).on("connection", onUserConnected);
}


//Function that handles a new connection from a user and start listening for its queries
function onUserConnected(socket) {
  console.log('-> New user connected');
  logger.log(`${new Date().toISOString()}: -> New user connected`);
  socket.on('disconnect', () => {
    console.log('-> User disconnected');
	logger.log(`${new Date().toISOString()}: -> User disconnected`);
  });
}