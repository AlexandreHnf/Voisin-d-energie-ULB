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
    if (row_id == 0) {
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
    } else {
        
        const inputs = {
            min: 20,
            max: 80,
            count: 8,
            decimals: 2,
            continuity: 1
          };
          
          const generateLabels = () => {
            return Samples.utils.months({count: inputs.count});
          };
          
          const generateData = () => (Samples.utils.numbers(inputs));
          
          Samples.utils.srand(42);

        const data = {
            labels: generateLabels(),
            datasets: [
              {
                label: 'D0',
                data: generateData(),
                borderColor: window.chartColors.red,
                backgroundColor: Samples.utils.transparentize(255, 99, 132, 0.5),
                hidden: true
              },
              {
                label: 'D1',
                data: generateData(),
                borderColor: window.chartColors.orange,
                backgroundColor: Samples.utils.transparentize(255, 159, 64, 0.5),
                fill: '-1'
              },
              {
                label: 'D2',
                data: generateData(),
                borderColor: window.chartColors.yellow,
                backgroundColor: Samples.utils.transparentize(255, 205, 86, 0.5),
                hidden: true,
                fill: 1
              },
              {
                label: 'D3',
                data: generateData(),
                borderColor: window.chartColors.green,
                backgroundColor: Samples.utils.transparentize(75, 192, 192, 0.5),
                fill: '-1'
              },
              {
                label: 'D4',
                data: generateData(),
                borderColor: window.chartColors.blue,
                backgroundColor: Samples.utils.transparentize(54, 162, 235, 0.5),
                fill: '-1'
              }
            ]
          };

          const config = {
            type: 'line',
            data: data,
            options: {
              scales: {
                y: {
                  stacked: true
                }
              },
              plugins: {
                filler: {
                  propagate: false
                },
                'samples-filler-analyser': {
                  target: 'chart-analyser'
                }
              },
              interaction: {
                intersect: false,
              },
            },
          };
        console.log(`chartCanvas${row_id}_${chart_id}`);
        charts[chart_id] = new Chart(document.getElementById(`chartCanvas${row_id}_${chart_id}`).getContext('2d'), {
        type: 'line',
        data: data,
        options: {
            scales: {
              y: {
                stacked: true
              }
            },
            plugins: {
              filler: {
                propagate: false
              },
              'samples-filler-analyser': {
                target: 'chart-analyser'
              }
            },
            interaction: {
              intersect: false,
            },
          }
        });
    }
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