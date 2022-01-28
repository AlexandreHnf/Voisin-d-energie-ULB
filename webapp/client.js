const socket = io('http://localhost:5000');
const HOME_ID = sessionStorage.getItem("username")

var timing_button = document.getElementById("buttonShow");

let charts_raw_today = {};  // list of ChartJS object for today's data (updated throughout the day)
let charts_stats_today = {};
let charts_grp_stats_today = {};
let charts_raw_day = {}; // list of ChartJS object for data of a specific day
let charts_stats_day = {};
let charts_grp_stats_day = {};

let ALL_IDS = {}
let ALL_GRP_IDS = {}  // group of homes ids
let IDS = {}
let GRP_IDS = {}

function validateTimingInput() {
    document.getElementById("login_err_msg").innerHTML = "OK c'est bon";
}

function processDateQuery() {
	var date = document.getElementById("day").value;
	console.log("date: "+ date);
	document.getElementById("date_msg").innerHTML = "=> Flukso data - " + date;
	sendDateQuery("raw", date.toString());
  sendDateQuery("stats", date.toString());
  sendDateQuery("groups", date.toString());
}

async function sendDateQuery(data_type, date) {
	// send pseudo to server
  const data = { date: date, data_type: data_type, home_id: HOME_ID };
  const options = {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
  };

	const response = await fetch('/date', options);
	const resdata = await response.json();

	var alldata = resdata.data;
	console.log("> --------- date sent to server...");
  if (alldata === undefined) {
    document.getElementById("day_msg").innerHTML = "<strong>" + date + "</strong> : No data."
  } else {
    console.log("msg : " + resdata.msg);
    console.log(alldata.rows[0]);
    console.log("nb of rows received : " + alldata.rows.length);

    if (data_type === "raw") {
      createChartRaw(alldata);
    } else if (data_type === "stats") {
      createChartStats(alldata);
    } else if (data_type === "groups") {
      createChartGrpStats(alldata);
    }
  }	
  
}


// create html chart canvas : 
function createChartCanvas(col_id, col_name, ids) {

  for (const id in ids) { // for each home, create a chart canvas
    // console.log(hid);

    let div = document.createElement("div");
    div.innerHTML += `<p>${id}</p>
                      <canvas id="chartCanvas${col_id}_${id}"></canvas>`;
    // console.log(`<canvas id="chartCanvas${col_id}_${id}"></canvas>`);
    document.getElementById(`${col_name}`).appendChild(div);
    // console.log(document.getElementById(`${col_name}`).innerHTML);
  }
}

function createNotationsGroups() {
	let txt_box = document.getElementById("groups_legend");
	for (const gid in ALL_GRP_IDS) {
		
		txt_box.innerHTML += `<dt>${gid}</dt>`;
		// console.log(ALL_GRP_IDS[gid]);
		for (let i = 0; i < ALL_GRP_IDS[gid].length; i++) {
			// console.log(ALL_GRP_IDS[gid][i]);
			txt_box.innerHTML += `<dd>- ${ALL_GRP_IDS[gid][i]}</dd>`;
		}
	}
}

function createChartDatasetRaw(hid) {
  datasets = []
  for (let i = 0; i < IDS[hid].length; i++){
    datasets.push({
      label: IDS[hid][i],  // phase name
      //borderColor: COLORS[i],
      data: []
    });
  }
  return datasets;
}

function createChartDatasetStats() {
  datasets = [
    {
      label: 'P_cons',
      data: [],
      borderColor: window.chartColors.red,
      backgroundColor: Samples.utils.transparentize(255, 99, 132, 0.5),
      hidden: false,
	    fill: false
    },
    {
      label: 'P_prod',
      data: [],
      borderColor: window.chartColors.green,
      backgroundColor: Samples.utils.transparentize(75, 192, 192, 0.5),
      hidden: false,
	    fill: false
    },
    {
      label: 'P_tot',
      data: [],
      borderColor: window.chartColors.yellow,
      backgroundColor: Samples.utils.transparentize(255, 205, 86, 0.5),
      fill: {above: 'red', below: 'green', target: "origin"}
    }
  ]
  return datasets;
}

// init each chart with empty dataset, but proper labels from the home ids
function initCharts(charts, col_id, data_type, ids) {

  for (const id in ids) {
    // first destroy previous charts if any
    if (charts[id] !== null) {
      // console.log("destroying previous charts...");
      charts[id].destroy();
    }

    // create empty charts with labels
    if (data_type === "raw") {
      datasets = createChartDatasetRaw(id);
    } else if (data_type === "stats" || data_type === "groups") {
      datasets = createChartDatasetStats();
    } 

    // console.log(`chartCanvas${col_id}_${id}`);
    charts[id] = new Chart(document.getElementById(`chartCanvas${col_id}_${id}`).getContext('2d'), {
      type: 'line',
      data: {
        labels: [],
        datasets: datasets
      },
      options: {responsive: true, 
		scales: {
			yAxes: [
			  {
				scaleLabel: {
				  display: true,
				  labelString: "Power (Watts) - W",
				},
			  },
			],
			xAxes: [
			  {
				scaleLabel: {
				  display: true,
				  labelString: "Time",
				},
			  },
			],
		  },
		}
    });
  }
}


/*
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
          fill: "origin",
          hidden: true
        },
        {
          label: 'D2',
          data: [20, -40, 50, 10, -10, 30, -10, 60],
          borderColor: window.chartColors.yellow,
          backgroundColor: Samples.utils.transparentize(255, 205, 86, 0.5),
          fill: {above: 'rgb(255, 0, 0)', below: 'rgb(0, 0, 255)', target: {value: 20}}
        },
        {
          label: 'D3',
          data: generateData(),
          borderColor: window.chartColors.green,
          backgroundColor: Samples.utils.transparentize(75, 192, 192, 0.5),
          fill: '-1',
          hidden: true
        },
        {
          label: 'D4',
          data: generateData(),
          borderColor: window.chartColors.blue,
          backgroundColor: Samples.utils.transparentize(54, 162, 235, 0.5),
          fill: '-1',
          hidden: true
        },
        {
          label: 'D9',
          data: generateData(),
          borderColor: window.chartColors.purple,
          backgroundColor: Samples.utils.transparentize(153, 102, 255, 0.5),
          fill: {above: 'blue', below: 'red', target: {value: 40}}
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
function createChartRawTest(charts, col_id, home_id) {
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
*/
// Create the Charts with raw data of a specific day
function createChartRaw(raw_data) {
  initCharts(charts_raw_day, 0, "raw", IDS);

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


function createChartStats(stats_data, data_type) {
  initCharts(charts_stats_day, 1, "stats", IDS);

  for (let i = 0; i < stats_data.rows.length; i++) {
    let row = stats_data.rows[i];
    charts_stats_day[row.home_id].data.labels.push(row.ts);
    charts_stats_day[row.home_id].data.datasets[0].data.push(row["p_cons"]);
    charts_stats_day[row.home_id].data.datasets[1].data.push(row["p_prod"]);
    charts_stats_day[row.home_id].data.datasets[2].data.push(row["p_tot"]);

    charts_stats_day[row.home_id].update();
  }
}

function createChartGrpStats(grp_stats_data) {
  initCharts(charts_grp_stats_day, 2, "groups", ALL_GRP_IDS);

  for (let i = 0; i < grp_stats_data.rows.length; i++) {
    let row = grp_stats_data.rows[i];
    charts_grp_stats_day[row.home_id].data.labels.push(row.ts);
    charts_grp_stats_day[row.home_id].data.datasets[0].data.push(row["p_cons"]);
    charts_grp_stats_day[row.home_id].data.datasets[1].data.push(row["p_prod"]);
    charts_grp_stats_day[row.home_id].data.datasets[2].data.push(row["p_tot"]);

    charts_grp_stats_day[row.home_id].update();
  }
}

function createPage() {
  for (const hid in IDS) {
    charts_raw_day[hid] = null;
    charts_raw_today[hid] = null;

    charts_stats_day[hid] = null;
    charts_stats_today[hid] = null;
  }

  for (const gid in ALL_GRP_IDS) {
    charts_grp_stats_day[gid] = null;
    charts_grp_stats_today[gid] = null;
  }

  console.log("creating charts...")
  // stats data
  createChartCanvas(1, "stats_data_charts", IDS);
  initCharts(charts_stats_day, 1, "stats", IDS);

  // raw data
  createChartCanvas(0, "raw_data_charts", IDS);
  initCharts(charts_raw_day, 0, "raw", IDS);

  // groups data
  createChartCanvas(2, "groups_data_charts", ALL_GRP_IDS);
  createNotationsGroups();
  initCharts(charts_grp_stats_day, 2, "groups", ALL_GRP_IDS);
  
}

function initIds(ids, grp_ids) {
  ALL_IDS = ids;
  ALL_GRP_IDS = grp_ids;

  if (HOME_ID === "flukso_admin") {
    IDS = ids;
    GRP_IDS = grp_ids;
  } else {
    IDS[HOME_ID] = ids[HOME_ID];
  }
}

function main() {

  document.getElementById("profil_badge").innerText = HOME_ID;
  socket.once("init", (data) => {
    console.log(data.ids);
    console.log(data.grp_ids);
    initIds(data.ids, data.grp_ids);

    console.log("IDS -> " + HOME_ID);
    console.log(IDS);
    console.log(GRP_IDS);
    // create html charts canvas
    createPage();
  });

  socket.on('connect_error', function () {
      console.log('Connection Failed. Server down !');
  });

  
}

main();