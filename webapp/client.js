const socket = io('http://localhost:5000');

var timing_button = document.getElementById("buttonShow");

let charts_raw_today = {};  // list of ChartJS object for today's data (updated throughout the day)
let charts_stats_today = {};
let charts_raw_day = {}; // list of ChartJS object for data of a specific day
let charts_stats_day = {};

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
  console.log("phase 1 value : " + raw_data.rows[0]["phase1"]);
  console.log("nb of rows received : " + raw_data.rows.length);

  createChartRaw2(raw_data);
}


// create html chart canvas : 
function createChartCanvas(col_id, col_name) {

  for (const hid in ids) { // for each home, create a chart canvas
    // console.log(hid);

    let div = document.createElement("div");
    div.innerHTML += `<p>${hid}</p>
                      <canvas id="chartCanvas${col_id}_${hid}"></canvas>`;
    // console.log(`<canvas id="chartCanvas${col_id}_${hid}"></canvas>`);
    document.getElementById(`${col_name}`).appendChild(div);
    // console.log(document.getElementById(`${col_name}`).innerHTML);

  }
}

// init each chart with empty dataset, but proper labels from the home ids
function initRawCharts() {

  for (const hid in ids) {
    // first destroy previous charts if any
    if (charts_raw_day[hid] !== null) {
      // console.log("destroying previous charts...");
      charts_raw_day[hid].destroy();
    }

    // create empty charts with labels
    datasets = []
    for (let i = 0; i < ids[hid].length; i++){
      datasets.push({
        label: ids[hid][i],  // phase name
        //borderColor: COLORS[i],
        data: []
      });
    }

    // console.log(`chartCanvas${0}_${hid}`);
    charts_raw_day[hid] = new Chart(document.getElementById(`chartCanvas${0}_${hid}`).getContext('2d'), {
      type: 'line',
      data: {
        labels: [],
        datasets: datasets
      },
      options: {responsive: true}
    });
  }
}


// Create Charts stats (prototype)
function createChartStats(charts, col_id, home_id) {
  // console.log("chart id : " + home_id);
    //console.log(charts)
    if (charts[home_id] !== null) {
        console.log("destroying previous charts...");
        charts[home_id].destroy();
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

    COLORS = [];

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
      console.log(`chartCanvas${col_id}_${home_id}`);
      charts[home_id] = new Chart(document.getElementById(`chartCanvas${col_id}_${home_id}`).getContext('2d'), {
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
function createChartRaw(charts, col_id, home_id) {
  console.log("chart id : " + home_id);
    console.log(charts)
    if (charts[home_id] !== null) {
        console.log("destroying previous charts...");
        charts[home_id].destroy();
    }
    
    console.log(`chartCanvas${col_id}_${home_id}`);
    charts[home_id] = new Chart(document.getElementById(`chartCanvas${col_id}_${home_id}`).getContext('2d'), {
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

// Create the Charts with raw data of a specific day
function createChartRaw2(raw_data) {
    initRawCharts();

    for (let i = 0; i < raw_data.rows.length; i++) {
      // row = {home_id; day, ts, phase1... phaseN}
      let row = raw_data.rows[i];
      charts_raw_day[row.home_id].data.labels.push(row.ts);
      let j = 1;
      for (let j = 0; j < charts_raw_day[row.home_id].data.datasets.length; j++) {
        dataset = charts_raw_day[row.home_id].data.datasets[j];
        phase = "phase" + j;
        dataset.data.push(row[phase]);
      }
      charts_raw_day[row.home_id].update();
    }
}

async function getIds() {
  // get all products from server replica set
  const response = await fetch('/ids');
  const data = await response.json();

  ids = data.ids;

  console.log("ids : " + ids);

  // for (const hid in ids) {
  //   console.log(hid);
  //   console.log(ids[hid]);
  // }
}

function createPage() {
  for (const hid in ids) {
    charts_raw_day[hid] = null;
    charts_raw_today[hid] = null;
    charts_stats_day[hid] = null;
    charts_stats_today[hid] = null;
  }
  // row 0
  createChartCanvas(0, "raw_data_charts");
  initRawCharts();
  // createChartRaw(charts_raw_day, 0, 'CDB011');
  // createChartRaw2()

  // row 1 
  createChartCanvas(1, "stats_charts");
  createChartStats(charts_stats_day, 1, 'CDB011');
  
}

function main() {
    
    // for (let i = 0; i < charts.length; i++) {
    //     createChart(i);
    // }
	console.log("creating charts...")

  socket.once("init", (data) => {
    ids = data.ids;

    console.log("ids : " + ids);
    // create html charts (2 columns)
    createPage();
  });

  socket.on('connect_error', function () {
      console.log('Connection Failed. Server down !');
  });

  // getIds();

  
}

main();