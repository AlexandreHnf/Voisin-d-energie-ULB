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

	var raw_data = resdata.raw_data;
	console.log("date sent to server...");
	console.log("msg : " + raw_data.msg);
    console.log(raw_data.rows[0]);
}


// create html chart canvas : 
function createChartCanvas(row_id, chart_id, row_name) {
    let div = document.createElement("div");
    div.innerHTML += `<canvas id="chartCanvas${row_id}_${chart_id}"></canvas>`;
    console.log(`<canvas id="chartCanvas${row_id}_${chart_id}"></canvas>`);
    document.getElementById(`${row_name}`).appendChild(div);
    // console.log(document.getElementById(`${row_name}`).innerHTML);
}

//Create the Charts
function createChart(row_id, chart_id) {
    // if (charts[i] !== null) {
    //     console.log("ah")
    //    charts[i].destroy();
    // }
    console.log(`chartCanvas${row_id}_${chart_id}`);
    charts[chart_id] = new Chart(document.getElementById(`chartCanvas${row_id}_${chart_id}`).getContext('2d'), {
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

    // row 0
    createChartCanvas(0, 1, "raw_data_charts");
    createChart(0, 1);

    // row 1 
    createChartCanvas(1, 1, "stats_charts");
    createChart(1, 1);

    socket.on('connect_error', function () {
        console.log('Connection Failed. Server down :-(');
    });
}

main();