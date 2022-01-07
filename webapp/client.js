const socket = io('http://localhost:5000');

var timing_button = document.getElementById("buttonShow");

let charts = [null]; // list of ChartJS object

function validateTimingInput() {
    document.getElementById("login_err_msg").innerHTML = "OK c'est bon";
}

function processDateQuery() {
	var date = document.getElementById("day").value;
	console.log("date: "+ date);
	document.getElementById("date_msg").innerHTML = "=> " + date;
	sendDateQuery(date.toString());
}

async function sendDateQuery(date) {
	// send pseudo to server
	var testdata = "post_test";
    const data = { date };
    const options = {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(data)
    };

	const response = await fetch('/date', options);
	const resdata = await response.json();

	var msg = resdata.msg;
	console.log("msg : " + msg);
}

//Create the Charts
function createChart(i) {
    // if (charts[i] !== null) {
    //     console.log("ah")
    //    charts[i].destroy();
    // }
    charts[i] = new Chart(document.getElementById(`chartCanvas${i}`).getContext('2d'), {
       type: 'line',
       data: {
          labels: ['January', 'February', 'March', 'April', 'May', 'June'],
          datasets: [{
            label: 'My First dataset',
            borderColor: 'rgb(255, 99, 132)',
            data: [0, 10, 5, 2, 20, 30, 45],
          }]
       },
       options: {responsive: true}
    });
 }

function main() {
    
    // for (let i = 0; i < charts.length; i++) {
    //     createChart(i);
    // }
	console.log("creating charts...")
    createChart(1);

    socket.on('connect_error', function () {
        console.log('Connection Failed. Server down :-(');
    });
}

main();