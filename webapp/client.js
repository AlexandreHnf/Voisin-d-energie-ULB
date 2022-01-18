const socket = io('http://localhost:5000');

var timing_button = document.getElementById("buttonShow");

let charts_raw_today = [null];  // list of ChartJS object for today's data (updated throughout the day)
let charts_stats_today = [null];
let charts_raw_day = [null]; // list of ChartJS object for data of a specific day
let charts_stats_day = [null];

let ids = {}

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
function createChartCanvas(col_name) {
  let chart_id = 0;

  for (const hid in ids) {
    console.log(hid);

    let div = document.createElement("div");
    div.innerHTML += `<p> ${hid}></p>
                      <canvas id="chartCanvas${col_name}_${chart_id}"></canvas>`;
    console.log(`<canvas id="chartCanvas${col_name}_${chart_id}"></canvas>`);
    document.getElementById(`${col_name}`).appendChild(div);
    // console.log(document.getElementById(`${col_name}`).innerHTML);

    chart_id++;
  }
}



// Create Charts stats (prototype)
function createChartStats(charts, row_id, chart_id) {
  console.log("chart id : " + chart_id);
    console.log(charts)
    if (charts[chart_id] !== null) {
        console.log("destroying previous charts...");
        charts[chart_id].destroy();
    }
        
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

//Create the Charts raw data (test)
function createChartRaw(charts, row_id, chart_id) {
  console.log("chart id : " + chart_id);
    console.log(charts)
    if (charts[chart_id] !== null) {
        console.log("destroying previous charts...");
        charts[chart_id].destroy();
    }
    
    console.log(`chartCanvas${row_id}_${chart_id}`);
    charts[chart_id] = new Chart(document.getElementById(`chartCanvas${row_id}_${chart_id}`).getContext('2d'), {
    type: 'line',
    data: {
        labels: ['January', 'February', 'March', 'April', 'May', 'June'],
        datasets: [{
            label: 'My First dataset',
            borderColor: 'rgb(255, 99, 132)',  // red
            data: [0, 10, 5, 2, 20, 30, 45],
        }]
    },
    options: {responsive: true}
  });
}

//Create the Charts with raw data of a specific day
function createChartRaw2(row_id, chart_id, raw_data) {
  console.log("chart id : " + chart_id);
    console.log(charts)
    if (charts[chart_id] !== null) {
        console.log("destroying previous charts...");
        charts[chart_id].destroy();
    }
    let labels = [];
    let data = [];
    raw_data.foreach(row => {
      labels.push(row.ts);
      data.push(row.phase1);
    })
    console.log(`chartCanvas${row_id}_${chart_id}`);
    charts[chart_id] = new Chart(document.getElementById(`chartCanvas${row_id}_${chart_id}`).getContext('2d'), {
    type: 'line',
    data: {
        labels: ['January', 'February', 'March', 'April', 'May', 'June'],
        datasets: [{
            label: 'My First dataset',
            borderColor: 'rgb(255, 99, 132)',  // red
            data: [0, 10, 5, 2, 20, 30, 45],
        }]
    },
    options: {responsive: true}
  });
}

async function getIds() {
  // get all products from server replica set
  const response = await fetch('/ids');
  const data = await response.json();

  ids = data.ids;

  console.log(ids);

  // for (const hid in ids) {
  //   console.log(hid);
  //   console.log(ids[hid]);
  // }
}

function createPage() {
  // row 0
  createChartCanvas("raw_data_charts");
  createChartRaw(charts_raw_day, 0, 0);

  // row 1 
  createChartCanvas("stats_charts");
  createChartStats(charts_stats_day, 1, 0);
}

function main() {
    
    // for (let i = 0; i < charts.length; i++) {
    //     createChart(i);
    // }
	console.log("creating charts...")

  socket.once("init", (data) => {
    ids = data.ids;

    console.log(ids);
    // create html charts (2 columns)
    createPage();
  });

  socket.on('connect_error', function () {
      console.log('Connection Failed. Server down !');
  });

  // getIds();

  
}

main();