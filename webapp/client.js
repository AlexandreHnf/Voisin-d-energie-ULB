const domain_name = 'http://localhost:5000';
// const domain_name = 'https://iridia-vde.ulb.ac.be:5000';
// const socket = io(domain_name);
const HOME_ID = sessionStorage.getItem("username")

var timing_button = document.getElementById("buttonShow");

// ChartJS objects for today's data and specific day data.
let charts_powers = {today : null, day: null};

let COLORS = ["#34ace0", "#e55039", "#474787", "#fed330", "#32ff7e", "#0fb9b1", "#fa8231", "#a5b1c2",
              "#4b7bec", "#a55eea", "#fad390", "#b71540", "#e58e26", "#38ada9", "#0a3d62"];

// ==========================================================================

function initFirstQuery() {
  // set active tab : power
  document.getElementById("default").click();

  // query data of today
  let today = new Date().toISOString().slice(0, 10);  // format (YYYY MM DD)
  let date_badge = document.querySelectorAll('[id=date_badge]');
	date_badge.forEach(badge => {
		badge.innerHTML = today;
	})
  sendDateQuery("power", today.toString());
}

function validateTimingInput() {
	document.getElementById("login_err_msg").innerHTML = "OK c'est bon";
}

function changeTimeUnit() {
	/*
	
	*/
  var radio = document.getElementsByName('time_unit_radio');
  let unit = "";
  for(i = 0; i < radio.length; i++) {
    if(radio[i].checked) {
      unit = radio[i].value;
    }
  }
	charts_powers[day].options.scales.x.time.unit = unit;
	charts_powers[day].update();
 
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
	// document.getElementById("date_msg").innerHTML = "=> Flukso data - " + date;
	// sendDateQuery("raw", date.toString());
  sendDateQuery("power", date.toString());
  // sendDateQuery("groups", date.toString());
}


async function sendDateQuery(data_type, date) {
	/* 
	send pseudo to server
	*/
  const data = { date: date, data_type: data_type, home_id: HOME_ID };
  const options = {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
  };

	const response = await fetch('/date', options);
	const resdata = await response.json();

	var alldata = resdata.data;
  if (alldata != undefined) {
    console.log("nb of rows received : " + alldata.length);

    if (alldata.length == 0) {
      document.getElementById("day_msg").innerHTML = "<strong>" + date + "</strong> : No data."
    }
    console.log(alldata[0]);
    
    if (data_type === "raw") {
      createChartRaw(alldata);
    } else if (data_type === "power") {
      createChartpowers(alldata);
    } else if (data_type === "groups") {
      createChartGrppowers(alldata);
    }
  }	
  
}

function createChartCanvas(col_id, col_name) {
	/* 
	create html chart canvas :
	*/
	let div = document.createElement("div");
	div.innerHTML += `<p>${HOME_ID}</p>
										<canvas id="chartCanvas${col_id}_${HOME_ID}"></canvas>`;
	document.getElementById(`${col_name}`).appendChild(div);
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


function initCharts(chart, col_id) {
  /* 
  init each chart with empty dataset, but proper labels from the home
  */
  // console.log(charts_powers[day]);
  if (charts_powers[day] !== null) { // first destroy previous charts if any
    // console.log("destroying previous charts...");
    charts_powers[day].destroy();
  }

  // create empty charts with labels
  createChartDatasetpowers();

//   console.log(`chartCanvas${col_id}_${HOME_ID}`);
  charts_powers[day] = new Chart(document.getElementById(`chartCanvas${col_id}_${HOME_ID}`).getContext('2d'), {
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


function createChartpowers(powers_data) {
  /* 
  fill chart with received data 
  */
  initCharts(charts_powers[day], 1);

  data = []
  for (let i = 0; i < powers_data.length; i++) {
    let row = powers_data[i];
    let ts = row.ts.slice(0, -5);
	  //let ts = row.ts;
    charts_powers[day].data.datasets[0].data.push({x: ts, p_cons: row["p_cons"]});
    charts_powers[day].data.datasets[1].data.push({x: ts, p_prod: row["p_prod"]});
    charts_powers[day].data.datasets[2].data.push({x: ts, p_tot: row["p_tot"]});

		let prelevement = 0;
		let injection = 0;
		if (row["p_tot"] > 0) {
			prelevement = row["p_tot"];
		} else if (row["p_tot"] < 0) {
			injection = row["p_tot"];
		}
		charts_powers[day].data.datasets[3].data.push({x: ts, pre: prelevement});
		charts_powers[day].data.datasets[4].data.push({x: ts, inj: injection});

  }
  charts_powers[day].update();

  // console.log(charts_powers[day].data.datasets);
}


function createPage() {
  charts_powers[day] = null;

//   console.log("creating charts...")
  // powers data
  createChartCanvas(1, "power_data_charts");
  initCharts(charts_powers[day], 1);
  
}


function main() {
  document.getElementById("profil_badge").innerText = HOME_ID;

  createPage();

  // socket.on('connect_error', function () {
  //   console.log('Connection Failed. Server down !');
  // });

  initFirstQuery();

}

main();