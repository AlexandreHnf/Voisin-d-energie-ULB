/**
 * Copyright (c) 2022 Alexandre Heneffe
 * License : MIT
 * Client.js -> main page
 */


// ============================== VARIABLES =================================

const domain_name = 'http://localhost:5000';

const HOME_ID = sessionStorage.getItem("username");
let GRP_IDS = JSON.parse(sessionStorage.getItem('grp_ids'));
let GRP_captions = {};

// button that allows to confirm a timestamp choice
var timing_button = document.getElementById("buttonShow"); 

// ChartJS objects for today's data and specific day data.
let charts_powers = {today : null, day: {HOME_ID: null}};
// let charts_groups = {today : {}, day: {}};
let powers_data = {};
let current_date = new Date().toISOString().slice(0, 10).toString();

// predefined colors for charts lines
let COLORS = ["#34ace0", "#e55039", "#474787", "#fed330", "#32ff7e", "#0fb9b1", "#fa8231", "#a5b1c2",
              "#4b7bec", "#a55eea", "#fad390", "#b71540", "#e58e26", "#38ada9", "#0a3d62"];

const VERBOSE = true;

// ============================= FUNCTIONS ==================================

function activateLoader() {
	/*
	make the loader appear and spin
	*/
	document.getElementById("loader").style.display = "block";
}

function deactivateLoader() {
	/* 
	make the loader animation disappear after a certain amount of time
  -> it simulates the actual loading time of the data arriving in the graphs.
	*/
	setTimeout(() =>{
		document.getElementById("loader").style.display = "none";
	}, 3000);  // 3 seconds
}

function resetChartsPower(charts_powers) {
  /* 
  set all charts to null
  */
  charts_powers.day[HOME_ID] = null;
  for (let i = 0; i < GRP_IDS.length; i++) {
    charts_powers.day[GRP_IDS[i]] = null;
  }
}

function initFirstQuery() {
  /* 
  When arriving in the main page, init a first query to get today's data
  */
  // set active tab : power
	document.getElementById("groups_tab").click();  // simulates a click
  document.getElementById("power_tab").click();  // simulates a click

  sendCaptionQuery();

  // query data of today
  let today = new Date().toISOString().slice(0, 10);  // format (YYYY MM DD)
  let date_badge = document.querySelectorAll('[id=date_badge]');
	date_badge.forEach(badge => {
		badge.innerHTML = today;
	})
	activateLoader();
  sendDateQuery("power", today.toString(), HOME_ID);
  for (let i = 0; i < GRP_IDS.length; i++) { // groups
    sendDateQuery("groups", today.toString(), GRP_IDS[i]);
  }
}


function changeTimeUnit() {
	/*
	Change the graph time unit - x axis
	*/
  var radio = document.getElementsByName('time_unit_radio');
  let unit = "";
  for(i = 0; i < radio.length; i++) {
    if(radio[i].checked) {
      unit = radio[i].value;
    }
  }
  charts_powers.day[HOME_ID].options.scales.x.time.unit = unit;
	charts_powers.day[HOME_ID].update();
  // update groups charts
  for (let i = 0; i < GRP_IDS.length; i++) {
    charts_powers.day[GRP_IDS[i]].options.scales.x.time.unit = unit;
	  charts_powers.day[GRP_IDS[i]].update();
  }
 
}


function download(){
	/* 
	// Download flukso data of the home and of the groups
	*/
	for (const home_id in powers_data) {
		// convert to csv 
		let csv = $.csv.fromObjects(powers_data[home_id]);

		// trigger a download
		var downloadLink = document.createElement("a");
		var blob = new Blob([csv], { type: 'text/csv' });
		var url = URL.createObjectURL(blob);
		downloadLink.href = url;
		downloadLink.download = `data_${home_id}_${current_date}.csv`;
		document.body.appendChild(downloadLink);
		downloadLink.click();
		document.body.removeChild(downloadLink);
	}
}


async function sendCaptionQuery() {
  /*
  Send caption query to get captions for each group
  */
  const data = {group_ids : GRP_IDS};
  const options = {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(data)
  };

  const response = await fetch('/caption', options);
  const resdata = await response.json();

  if (resdata != undefined) {
    for (let i = 0; i < resdata.data.rows.length; i++) {
      cap = resdata.data.rows[i];
      GRP_captions[cap.installation_id] = cap.caption;
      document.getElementById(`caption${cap.installation_id}`).innerHTML = `${cap.caption}`;
    }
  } 

} 


function processDateQuery() {
	/* 
	Get a date input, send query to server
	*/
	var date = document.getElementById("day").value;
	let date_badge = document.querySelectorAll('[id=date_badge]');
	date_badge.forEach(badge => {
		badge.innerHTML = date;
	})
	activateLoader();
  sendDateQuery("power", date.toString(), HOME_ID);
  for (let i = 0; i < GRP_IDS.length; i++) {
    sendDateQuery("groups", date.toString(), GRP_IDS[i]);
  }
}


async function sendDateQuery(data_type, date, home_id) {
	/* 
	send pseudo to server
	*/
  const data = { date: date, data_type: data_type, home_id: home_id };
  const options = {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
  };

	const response = await fetch('/date', options);
	const resdata = await response.json();

	var alldata = resdata.data;
  if (alldata != undefined) {
    if (VERBOSE) {console.log("nb of rows received : " + alldata.length);}

    if (alldata.length == 0) {
      document.getElementById(`query_msg_${data_type}`).innerHTML = "<strong>" + date + "</strong> : Aucune donnée."
    } else {
      document.getElementById(`query_msg_${data_type}`).innerHTML = "";
    }
    // if (VERBOSE) {console.log(alldata[0]);}

	powers_data[home_id] = alldata;
	current_date = date;
    
    if (data_type === "raw") {
      createChartRaw(alldata);
    } else if (data_type === "power") {
      createChartPowers(alldata, 1, home_id, date);
    } else if (data_type === "groups") {
      createChartPowers(alldata, 2, home_id, date);
    }
  }	
  
}

function createChartCanvas(col_id, col_name, ids) {
	/* 
	create html chart canvas :
  may be cleaner if the html were not defined here. To improve
	*/
  for (var i = 0; i < ids.length; i++) {
    let div = document.createElement("div");
    div.innerHTML += `<b>${ids[i]}</b><br>
                      <p id="caption${ids[i]}"> </p><br>
                      <canvas id="chartCanvas${col_id}_${ids[i]}"></canvas>
											
											<div class="alert alert-secondary" role="alert">

											<!-- Stats table with total injection and total taking (Prelevement) -->
											<h3>Energies</h3>
											<p>Energies totales de la journée</p>         
											<table id=stat_table${col_id}_${ids[i]} class="table table-striped">
												<thead>
													<tr>
														<th>Installation ID</th>
														<th>Consommation (kWh)</th>
														<th>Production (kWh)</th>
														<th>Injection - P_tot < 0 (kWh)</th>
														<th>Prelevement - P_tot > 0 (kWh)</th>
													</tr>
												</thead>
												<tbody>
													<tr>
														<td></td>
														<td></td>
														<td></td>
														<td></td>
														<td></td>
													</tr>
												</tbody>
											</table>
										</div>
										`;
              
    document.getElementById(`${col_name}`).appendChild(div);
  }
	
}

function getRandomColor() {
	/* 
	get a random color with format #XXXXXX
	*/
  var letters = '0123456789ABCDEF'.split('');
  var color = '#';
  for (var i = 0; i < 6; i++ ) {
      color += letters[Math.floor(Math.random() * 16)];
  }
  return color;
}

function createChartDatasetpowers() {
	/* 
	create Chartjs dataset for power data
	labels : P_cons, P_prod, P_net 
	*/
  datasets = [
    {
      label: 'P_cons',
      data: [],
      parsing: {
        yAxisKey: 'p_cons'
      },
      borderColor: 'rgb(255, 99, 132)',
      backgroundColor: Samples.utils.transparentize(255, 99, 132, 0.4),
      borderWidth: 1,
			pointRadius: 1.2,
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
      borderWidth: 1,
			pointRadius: 1.2,
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
      borderWidth: 1,
			pointRadius: 1.2,
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


function initChart(chart, col_id, home_id) {
  /* 
  init each chart with empty dataset, but proper labels from the home
  */
  if (chart.day[home_id] !== null) { // first destroy previous charts if any
    chart.day[home_id].destroy();
  }

  // create empty charts with labels
  createChartDatasetpowers();

  chart.day[home_id] = new Chart(document.getElementById(`chartCanvas${col_id}_${home_id}`).getContext('2d'), {
    type: 'line',
    data: {
      datasets: datasets
    },
    options: {
      responsive: true, 
      plugins: {
        legend: {
          position: 'top'
        },
        tooltip: {
          enabled: false
        }
      },
      scales: {
        x: {
          type: 'time',
          time: {
            unit: 'minute',
			displayFormats: {
				second: 'HH:mm:ss',
				minute: 'HH:mm',
				hour: 'HH',
				day: 'DD',
				month: 'MM yyyy',
				year: 'yyyy'
			}
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


function createChartPowers(powers_data, col_id, home_id, date) {
  /* 
  fill chart with received data 
  */
  initChart(charts_powers, col_id, home_id);

  data = []
  totals = {p_cons_tot: 0, p_prod_tot: 0, p_inj_tot: 0, p_pre_tot: 0}
  for (let i = 0; i < powers_data.length; i++) {
    let row = powers_data[i];

    let tss = new Date(row.ts);
    let ts = date + "T" + tss.toTimeString().slice(0,8) // local timezone (CET)

    charts_powers.day[home_id].data.datasets[0].data.push({x: ts, p_cons: row["p_cons"]});
    charts_powers.day[home_id].data.datasets[1].data.push({x: ts, p_prod: row["p_prod"]});
    charts_powers.day[home_id].data.datasets[2].data.push({x: ts, p_tot: row["p_tot"]});

	let prelevement = 0;
	let injection = 0;
	if (row["p_tot"] > 0) {
		prelevement = row["p_tot"];
	} else if (row["p_tot"] < 0) {
		injection = row["p_tot"];
	}
	charts_powers.day[home_id].data.datasets[3].data.push({x: ts, pre: prelevement});
	charts_powers.day[home_id].data.datasets[4].data.push({x: ts, inj: injection});

	totals.p_cons_tot += row["p_cons"];
	totals.p_prod_tot += row["p_prod"];
	totals.p_inj_tot += injection;
	totals.p_pre_tot += prelevement;

  }
  charts_powers.day[home_id].update();
	deactivateLoader();
	updateStatsTable(powers_data, totals, `stat_table${col_id}_${home_id}`, home_id);
}

function getEnergy(powers_data, tot_power) {
	/*
	from the total power value, compute the total energy
	given a certain number of data points and timestamps
	Energy : volume under the curve
	*/

	let N = powers_data.length;  // number of data points
	let T = 0										 // number of hours 
	if (N > 0) {
		first_ts = new Date(powers_data[0].ts.slice(0, -5))
		last_ts = new Date(powers_data[N-1].ts.slice(0, -5))
		T = Math.abs(last_ts - first_ts) / 36e5;
	}
	// first compute the mean :
	let P_mean = Math.abs(tot_power) / N;
	Q_tot = (P_mean / 1000) * T;  // divided by 1000 to convert to KWh

  if (isNaN(Q_tot)) {
    Q_tot = 0;
  }
  Q_tot = Math.round(Q_tot);
  console.log(Q_tot);

	return Q_tot;
}


function updateStatsTable(powers_data, totals, table_name, home_id) {

	document.getElementById(table_name).rows[1].cells[0].innerHTML = home_id;
	document.getElementById(table_name).rows[1].cells[1].innerHTML = getEnergy(powers_data, totals.p_cons_tot);
	document.getElementById(table_name).rows[1].cells[2].innerHTML = getEnergy(powers_data, totals.p_prod_tot);
	document.getElementById(table_name).rows[1].cells[3].innerHTML = getEnergy(powers_data, totals.p_inj_tot);
	document.getElementById(table_name).rows[1].cells[4].innerHTML = getEnergy(powers_data, totals.p_pre_tot);
}


function createPage() {
  /* 
  when arriving in the main page, create the chart canvas and init charts.
  */
  resetChartsPower(charts_powers);

  // powers data
  createChartCanvas(1, "power_data_charts", [HOME_ID]);
  initChart(charts_powers, 1, HOME_ID);

  // groups power data
  createChartCanvas(2, "groups_data_charts", GRP_IDS);
  for (let i = 0; i < GRP_IDS.length; i++) {
    initChart(charts_powers, 2, GRP_IDS[i]);
  }
  
}


function main() {
  document.getElementById("profil_badge").innerText = HOME_ID;

  createPage();

  initFirstQuery();

}

main();