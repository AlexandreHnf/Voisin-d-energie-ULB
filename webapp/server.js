
var assert = require('assert');
// ”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var cassandra = require('cassandra-driver');

const http = require("http")
const io = require("socket.io")
const express = require('express'); //Import the express dependency
const app = express();              //Instantiate an express app, the main work horse of this server
const port = 5000;                  //Save the port number where your server will be listening
const server = require('http').createServer(app);  //  express = request handler functions passed to http Server instances

// ===================== GLOBAL VARIABLES =========================== 
// Connection to localhost cassandra database1
const client = new cassandra.Client({
  contactPoints: ['ce97f9db-6e4a-4a2c-bd7a-2c97e31e3150', '127.0.0.1'],
  localDataCenter: 'datacenter1'
});


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
        resolve()
      }
    });
  });
}

function queryGrocery() {
  //Execute the queries 
  var query = 'SELECT name, price_p_item FROM grocery.fruit_stock WHERE name=? ALLOW FILTERING';
  var q1 = execute(query, ['oranges'], (err, result) => 
  { assert.ifError(err); console.log('The cost per orange is ' + result.rows[0].price_p_item)});
  var q2 = execute(query, ['pineapples'], (err,result) =>
  { assert.ifError(err); console.log('The cost per pineapple is ' + result.rows[0].price_p_item)});
  var q3 = execute(query, ['apples'], (err,result) => 
  { assert.ifError(err); console.log('The cost per apple is ' + result.rows[0].price_p_item)});

  Promise.all([q1,q2,q3]).then(() => {
    console.log('querys done');
    // process.exit();
  });
}



// ==============
//Idiomatic expression in express to route and respond to a client request
app.get('/', (req, res) => {        //get requests to the root ("/") will route here
    res.sendFile('client.html', {root: __dirname});      //server responds by sending the client.html file to the client's browser
                                                        //the .sendFile method needs the absolute path to the file, see: https://expressjs.com/en/4x/api.html#res.sendFile 
});

// send the client javascript file
app.get('/client.js', function(req, res) {
  res.sendFile('/client.js', {root: __dirname});
});

// send the css file
app.get('/styles.css', function(req, res) {
  res.sendFile('/styles.css', {root: __dirname});
});

// app.listen(port, () => {            //server starts listening for any attempts from a client to connect at port: {port}
//     console.log(`Now listening on port ${port}`); 
// });


async function main() {
  //Authorize clients only after updating the server's data
  server.listen(port, () => {
    console.log(`> Server listening on http://localhost:${port}`);
 });
 queryGrocery();
 io(server).on("connection", onUserConnected);
}


//Function that handles a new connection from a user and start listening for its queries
function onUserConnected(socket) {
  console.log('-> New user connected');
  socket.on('disconnect', () => {
     console.log('-> User disconnected');
  });
}