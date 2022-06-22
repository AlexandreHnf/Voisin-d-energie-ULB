/**
 * Copyright (c) 2022 Alexandre Heneffe
 * License : MIT
 *
 */

const socket = io('http://localhost:5000');
const HOME_ID = sessionStorage.getItem("username")

var timing_button = document.getElementById("buttonShow");

let charts_raw = {today: {}, day: {}};
let charts_powers = {today : {}, day: {}};
let charts_groups = {today: {}, day: {}};

let charts_raw_today = {};  // list of ChartJS object for today's data (updated throughout the day)
let charts_powers_today = {};
let charts_grp_powers_today = {};
let charts_raw_day = {}; // list of ChartJS object for data of a specific day
let charts_powers_day = {};
let charts_grp_powers_day = {};

let COLORS = ["#34ace0", "#e55039", "#474787", "#fed330", "#32ff7e", "#0fb9b1", "#fa8231", "#a5b1c2",
              "#4b7bec", "#a55eea", "#fad390", "#b71540", "#e58e26", "#38ada9", "#0a3d62"];

let ALL_IDS = {}
let ALL_GRP_IDS = {}  // group of homes ids
let IDS = {}
let GRP_IDS = {}


function validateTimingInput() {
    document.getElementById("login_err_msg").innerHTML = "OK c'est bon";
}

function changeTimeUnit() {
  var radio = document.getElementsByName('time_unit_radio');
  let unit = "";
  for(i = 0; i < radio.length; i++) {
    if(radio[i].checked) {
      unit = radio[i].value;
    }
  }
  console.log(unit);
  for (const id in IDS) {
    charts_raw_day[id].options.scales.x.time.unit = unit; 
    charts_powers_day[id].options.scales.x.time.unit = unit;
    charts_raw_day[id].update();
    charts_powers_day.update();
  }
  for (const gid in ALL_GRP_IDS) {
    charts_grp_powers_day[id].options.scales.x.time.unit = unit;
    charts_grp_powers_day[id].update();
  }
  
}

function processDateQuery() {
	var date = document.getElementById("day").value;
	console.log("date: "+ date);
	let date_badge = document.querySelectorAll('[id=date_badge]');
	date_badge.forEach(badge => {
		badge.innerHTML = date;
	})
	// document.getElementById("date_msg").innerHTML = "=> Flukso data - " + date;
	sendDateQuery("raw", date.toString());
  sendDateQuery("powers", date.toString());
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
    } else if (data_type === "powers") {
      createChartpowers(alldata);
    } else if (data_type === "groups") {
      createChartGrppowers(alldata);
    }
  }	
  
}


// create html chart canvas : 
function createChartCanvas(col_id, col_name, ids) {

  for (const id in ids) { // for each home, create a chart canvas

    let div = document.createElement("div");
    div.innerHTML += `<p>${id}</p>
                      <canvas id="chartCanvas${col_id}_${id}"></canvas>`;
    document.getElementById(`${col_name}`).appendChild(div);
  }
}

function createNotationsGroups() {
	let txt_box = document.getElementById("groups_legend");
	for (const gid in ALL_GRP_IDS) {
		
		txt_box.innerHTML += `<dt>${gid}</dt>`;
		for (let i = 0; i < ALL_GRP_IDS[gid].length; i++) {
			txt_box.innerHTML += `<dd>- ${ALL_GRP_IDS[gid][i]}</dd>`;
		}
	}
}

function getRandomColor() {
  var letters = '0123456789ABCDEF'.split('');
  var color = '#';
  for (var i = 0; i < 6; i++ ) {
      color += letters[Math.floor(Math.random() * 16)];
  }
  return color;
}

function createChartDatasetRaw(hid) {
  datasets = []
  for (let i = 0; i < IDS[hid].length; i++){
    // let color = getRandomColor();
    let color = COLORS[i];
    datasets.push({
      label: IDS[hid][i],  // phase name
      borderColor: color,  // random color
      borderWidth: 2,
			pointRadius: 2,
      data: []
    });
  }
  return datasets;
}

function createChartDatasetpowers() {
  datasets = [
    {
      label: 'P_cons',
      data: [],
      parsing: {
        yAxisKey: 'p_cons'
      },
      borderColor: 'rgb(255, 99, 132)',
      backgroundColor: Samples.utils.transparentize(255, 99, 132, 0.4),
      borderWidth: 2,
			pointRadius: 2,
      hidden: false,
	    fill: false
    },
    {
      label: 'P_prod',
      data: [],
      parsing: {
        yAxisKey: 'p_prod'
      },
      borderColor: 'rgb(54, 162, 235)',
      backgroundColor: Samples.utils.transparentize(54, 162, 235, 0.4),
      borderWidth: 2,
			pointRadius: 2,
      hidden: false,
	    fill: false
    },
    {
      label: 'P_tot',
      data: [],
      parsing: {
        yAxisKey: 'p_tot'
      },
      borderColor: 'rgb(170, 166, 157)',
      backgroundColor: Samples.utils.transparentize(201, 203, 207, 0.4),
      borderWidth: 2,
			pointRadius: 2,
      // fill: {above: 'red', below: 'green', target: "origin"}
      fill : false
    },
		{
			label: 'Prélèvement',
			data: [],
      parsing: {
        yAxisKey: 'pre'
      },
			borderColor: window.chartColors.yellow,
			borderWidth: 1, 
			pointRadius: 0,
			backgroundColor: Samples.utils.transparentize(255, 242, 0, 0.3),
			fill: true
		}, 
		{
			label: 'Injection',
			data: [],
      parsing: {
        yAxisKey: 'inj'
      },
			borderColor: window.chartColors.green,
			borderWidth: 1,
			pointRadius: 0,
			backgroundColor: Samples.utils.transparentize(38, 222, 129, 0.3),
			fill: true
		}
		
  ]
  return datasets;
}

// init each chart with empty dataset, but proper labels from the home ids
function initChart(charts, col_id, data_type, ids) {

  for (const id in ids) {
    // first destroy previous charts if any
    if (charts[id] !== null) {
      // console.log("destroying previous charts...");
      charts[id].destroy();
    }

    // create empty charts with labels
    if (data_type === "raw") {
      datasets = createChartDatasetRaw(id);
    } else if (data_type === "powers" || data_type === "groups") {
      datasets = createChartDatasetpowers();
    }

    // console.log(`chartCanvas${col_id}_${id}`);
    charts[id] = new Chart(document.getElementById(`chartCanvas${col_id}_${id}`).getContext('2d'), {
      type: 'line',
      data: {
        datasets: datasets
      },
      options: {
		    responsive: true, 
        plugins: {
          legend: {
            position: 'right'
          }
        },
		    scales: {
          x: {
            type: 'time',
            time: {
              unit: 'minute'
            },
            title: {
              color: 'red',
              display: true,
              text: 'Time'
            }
          },
          y: {
            title: {
              color: 'red',
              display: true,
              text: 'Power (Watts) - W'
            }
          }
		    },
		  }
    });
  }
}


// Create the Charts with raw data of a specific day
function createChartRaw(raw_data) {
  initChart(charts_raw_day, 0, "raw", IDS);

  for (let i = 0; i < raw_data.rows.length; i++) {
    // row = {home_id; day, ts, phase1... phaseN}
    let row = raw_data.rows[i];
    let ts = row.ts.slice(0, -5);
    let j = 1;
    for (let j = 0; j < charts_raw_day[row.home_id].data.datasets.length; j++) {
      dataset = charts_raw_day[row.home_id].data.datasets[j];
      phase = "phase" + j;
      dataset.data.push({x: ts, y: row[phase]});
    }
    charts_raw_day[row.home_id].update();
  }
}


function createChartpowers(powers_data) {
  initChart(charts_powers_day, 1, "powers", IDS);

  data = []
  for (let i = 0; i < powers_data.rows.length; i++) {
    let row = powers_data.rows[i];
    let ts = row.ts.slice(0, -5);
    charts_powers_day[row.home_id].data.datasets[0].data.push({x: ts, p_cons: row["p_cons"]});
    charts_powers_day[row.home_id].data.datasets[1].data.push({x: ts, p_prod: row["p_prod"]});
    charts_powers_day[row.home_id].data.datasets[2].data.push({x: ts, p_tot: row["p_tot"]});

		let prelevement = 0;
		let injection = 0;
		if (row["p_tot"] > 0) {
			prelevement = row["p_tot"];
		} else if (row["p_tot"] < 0) {
			injection = row["p_tot"];
		}
		charts_powers_day[row.home_id].data.datasets[3].data.push({x: ts, pre: prelevement});
		charts_powers_day[row.home_id].data.datasets[4].data.push({x: ts, inj: injection});

    charts_powers_day[row.home_id].update();
  }

  console.log(charts_powers_day['CDB003'].data.datasets);
}

function createChartGrppowers(grp_powers_data) {
  initChart(charts_grp_powers_day, 2, "groups", ALL_GRP_IDS);

  for (let i = 0; i < grp_powers_data.rows.length; i++) {
    let row = grp_powers_data.rows[i];
    let ts = row.ts.slice(0, -5);
    // charts_grp_powers_day[row.home_id].data.labels.push(row.ts);
    charts_grp_powers_day[row.home_id].data.datasets[0].data.push({x: ts, p_cons: row["p_cons"]});
    charts_grp_powers_day[row.home_id].data.datasets[1].data.push({x: ts, p_prod: row["p_prod"]});
    charts_grp_powers_day[row.home_id].data.datasets[2].data.push({x: ts, p_tot: row["p_tot"]});

		let prelevement = 0;
		let injection = 0;
		if (row["p_tot"] > 0) {
			prelevement = row["p_tot"];
		} else if (row["p_tot"] < 0) {
			injection = row["p_tot"];
		}
		charts_grp_powers_day[row.home_id].data.datasets[3].data.push({x: ts, pre: prelevement});
		charts_grp_powers_day[row.home_id].data.datasets[4].data.push({x: ts, inj: injection});

    charts_grp_powers_day[row.home_id].update();
  }
}

function createPage() {
  for (const hid in IDS) {
    charts_raw_day[hid] = null;
    charts_raw_today[hid] = null;

    charts_powers_day[hid] = null;
    charts_powers_today[hid] = null;
  }

  for (const gid in ALL_GRP_IDS) {
    charts_grp_powers_day[gid] = null;
    charts_grp_powers_today[gid] = null;
  }

  console.log("creating charts...")
  // powers data
  createChartCanvas(1, "powers_data_charts", IDS);
  initChart(charts_powers_day, 1, "powers", IDS);

  // raw data
  createChartCanvas(0, "raw_data_charts", IDS);
  // initCharts(charts_raw_day, 0, "raw", IDS);

  // groups data
  createChartCanvas(2, "groups_data_charts", ALL_GRP_IDS);
  // createNotationsGroups();
  // initCharts(charts_grp_powers_day, 2, "groups", ALL_GRP_IDS);
  
}

// function initIds(ids, grp_ids) {
//   ALL_IDS = ids;
//   ALL_GRP_IDS = grp_ids;

//   if (HOME_ID === "flukso_admin") {
//     IDS = ids;
//     GRP_IDS = grp_ids;
//   } else {
//     IDS[HOME_ID] = ids[HOME_ID];
//   }
// }

function main() {

  document.getElementById("profil_badge").innerText = HOME_ID;
  // socket.once("init", (data) => {
  //   console.log(data.ids);
  //   console.log(data.grp_ids);
  //   initIds(data.ids, data.grp_ids);

  //   console.log("IDS -> " + HOME_ID);
  //   console.log(IDS);
  //   console.log(GRP_IDS);
  //   // create html charts canvas
  //   createPage();
  // });

  createPage();

  socket.on('connect_error', function () {
      console.log('Connection Failed. Server down !');
  });

  
}

main();