var fs = require('fs')
var assert = require('assert');
// ”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var cassandra = require('cassandra-driver');

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
let ids = JSON.parse(fs.readFileSync('../sensors/ids.json', 'utf8'));
let grp_ids = JSON.parse(fs.readFileSync('../sensors/grp_ids.json', 'utf8'));
// Connection to localhost cassandra database1
const client = new cassandra.Client({
  contactPoints: ['ce97f9db-6e4a-4a2c-bd7a-2c97e31e3150', '127.0.0.1'],
  localDataCenter: 'datacenter1'
});

const TABLES = {'raw': 'raw_data', 'stats': 'stats', 'groups': 'groups_stats'};

/*
// Connection to a remote cassandra cluster
//Replace Username and Password with your cluster settings
var authProvider = new cassandra.auth.PlainTextAuthProvider('Username', 'Password');
//Replace PublicIP with the IP addresses of your clusters
var contactPoints = ['PublicIP','PublicIP','PublicIP’'];
var client = new cassandra.Client({contactPoints: contactPoints, authProvider: authProvider, keyspace:'grocery'});
*/


// ========================= FUNCTIONS ==================================

async function connectCassandra() {
  client.connect().then(() => console.log("> Connected to Cassandra !"));
}

connectCassandra().catch((e) => {
  console.error("There was an error connecting to the Cassandra database.");
  // console.error(e);
});


//========== Launch main ==============
main().catch((e) => {
  console.error("An error occured when launching the server:");
  console.error(e);
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
  console.log(query);
  var data = execute(query, [date], (err, result) => {
	  assert.ifError(err);
    // console.log(result.rows[0]);
    // console.log(result.rows.length);
    
	}).then((result) => {
    // console.log(result.rows[1]);
    response.json({msg: "date well received!", data: result});
  });
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
	console.log("> date request");
	const date = request.body.date;
	console.log("date: " + date);
  const data_type = request.body.data_type;  // raw or stats
  console.log("data type : " + data_type);
  const home_id = request.body.home_id;
  queryFluksoData(data_type, date, home_id, response);
  
});


function sendIdsToClient(socket) {
  console.log("sending init data to client (ids)")
  socket.emit("init", {
    "ids": ids,
    "grp_ids": grp_ids
  });
}


async function main() {
  let today = new Date().toISOString().slice(0, 10);  // format (YYYY MM DD)
  console.log(today);
  //Authorize clients only after updating the server's data
  server.listen(port, () => {
    console.log(`> Server listening on http://localhost:${port}`);
  });
  // console.log(ids);
  console.log(new Date().toLocaleTimeString());
  io(server).on("connection", onUserConnected);
}


//Function that handles a new connection from a user and start listening for its queries
function onUserConnected(socket) {
  console.log('-> New user connected');
  sendIdsToClient(socket);
  socket.on('disconnect', () => {
    console.log('-> User disconnected');
  });
}